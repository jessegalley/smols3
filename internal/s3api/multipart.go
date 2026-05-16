package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/jessegalley/smols3/internal/etag"
	"github.com/jessegalley/smols3/internal/index"
	"github.com/jessegalley/smols3/internal/storage"
)

// ---- POST /<bucket>/<key>?uploads : CreateMultipartUpload ----

func (s *Server) handleCreateMultipart(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)

	if ok, err := s.DB.BucketExists(b); err != nil || !ok {
		writeS3Error(w, r, ErrNoSuchBucket)
		return
	}

	uploadID := uuid.NewString()
	now := time.Now().UnixNano()

	rec := index.MultipartRecord{
		Schema:    index.RecordSchema,
		UploadID:  uploadID,
		Bucket:    b,
		Key:       k,
		Initiated: now,
		Meta: index.ObjectRecord{
			ContentType: r.Header.Get("Content-Type"),
			ContentEnc:  r.Header.Get("Content-Encoding"),
			ContentDisp: r.Header.Get("Content-Disposition"),
			CacheCtrl:   r.Header.Get("Cache-Control"),
			Expires:     r.Header.Get("Expires"),
			UserMeta:    extractUserMeta(r.Header),
		},
	}
	if err := s.DB.PutMultipart(rec); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	// Pre-create staging directory.
	if err := os.MkdirAll(storage.StagingDir(s.Cfg.Storage.DataDir, b, uploadID), 0o755); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	writeXML(w, http.StatusOK, &InitiateMultipartUploadResult{
		Xmlns: s3NS, Bucket: b, Key: k, UploadID: uploadID,
	})
}

// ---- PUT /<bucket>/<key>?partNumber=N&uploadId=X : UploadPart ----

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNumStr := q.Get("partNumber")

	partNum, err := strconv.Atoi(partNumStr)
	if err != nil || partNum < 1 || partNum > s.Cfg.Limits.MaxMultipartParts {
		writeS3Error(w, r, ErrInvalidPart)
		return
	}

	rec, err := s.DB.GetMultipart(b, uploadID)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if rec.Key != k {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}

	relPath := storage.StagingPartRelPath(b, uploadID, partNum)
	absPath := filepath.Join(s.Cfg.Storage.DataDir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	f, err := os.Create(absPath)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	teed, hasher := etag.TeeReader(r.Body)
	written, err := io.Copy(f, teed)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(absPath)
		writeS3Error(w, r, translateErr(err))
		return
	}
	if s.Cfg.Storage.FsyncData {
		_ = f.Sync()
	}
	_ = f.Close()

	part := index.PartRecord{
		Number:     partNum,
		ETag:       hasher.SumHex(),
		Size:       written,
		Path:       relPath,
		UploadedAt: time.Now().UnixNano(),
	}
	if err := s.DB.UpsertPart(b, uploadID, part); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	w.Header().Set("ETag", quoteETag(part.ETag))
	w.WriteHeader(http.StatusOK)
}

// ---- POST /<bucket>/<key>?uploadId=X : CompleteMultipartUpload ----

func (s *Server) handleCompleteMultipart(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := bucketName(r)
	k := objectKey(r)

	rec, err := s.DB.GetMultipart(b, uploadID)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if rec.Key != k {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}

	var req CompleteMultipartUpload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		writeS3Error(w, r, ErrMalformedXML)
		return
	}
	if len(req.Parts) == 0 {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	// Verify part order ascending and match the server's recorded parts.
	for i := 1; i < len(req.Parts); i++ {
		if req.Parts[i].PartNumber <= req.Parts[i-1].PartNumber {
			writeS3Error(w, r, ErrInvalidPartOrder)
			return
		}
	}
	byNum := make(map[int]index.PartRecord, len(rec.Parts))
	for _, p := range rec.Parts {
		byNum[p.Number] = p
	}
	completeMD5s := make([]string, len(req.Parts))
	chosen := make([]index.PartRecord, len(req.Parts))
	for i, rp := range req.Parts {
		got, ok := byNum[rp.PartNumber]
		if !ok {
			s.Logger.Debug("complete: missing part", "n", rp.PartNumber, "have", partNumbers(rec.Parts))
			writeS3Error(w, r, ErrInvalidPart)
			return
		}
		// Compare ETag — clients quote it, strip both.
		want := stripQuotes(rp.ETag)
		if want != got.ETag {
			s.Logger.Debug("complete: etag mismatch", "n", rp.PartNumber, "want_client", rp.ETag, "have_server", got.ETag)
			writeS3Error(w, r, ErrInvalidPart)
			return
		}
		completeMD5s[i] = got.ETag
		chosen[i] = got
	}

	// Concatenate parts into the final object.
	// For simplicity, we always stream the assembled object through the regular
	// storage Put path (which picks file vs pack mode based on size).
	stage := filepath.Join(s.Cfg.Storage.DataDir,
		filepath.FromSlash(storage.StagingPartRelPath(b, uploadID, 0))+".joined")
	_ = os.MkdirAll(filepath.Dir(stage), 0o755)

	joined, err := os.Create(stage)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	var total int64
	for _, p := range chosen {
		abs := filepath.Join(s.Cfg.Storage.DataDir, filepath.FromSlash(p.Path))
		pf, err := os.Open(abs)
		if err != nil {
			_ = joined.Close()
			_ = os.Remove(stage)
			writeS3Error(w, r, translateErr(err))
			return
		}
		n, err := io.Copy(joined, pf)
		_ = pf.Close()
		if err != nil {
			_ = joined.Close()
			_ = os.Remove(stage)
			writeS3Error(w, r, translateErr(err))
			return
		}
		total += n
	}
	if _, err := joined.Seek(0, io.SeekStart); err != nil {
		_ = joined.Close()
		_ = os.Remove(stage)
		writeS3Error(w, r, translateErr(err))
		return
	}

	res, err := s.Storage.Put(b, k, total, joined)
	_ = joined.Close()
	_ = os.Remove(stage)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	finalETag, err := etag.MultipartETag(completeMD5s)
	if err != nil {
		writeS3Error(w, r, ErrInternal)
		return
	}

	now := time.Now().UnixNano()
	obj := index.ObjectRecord{
		Schema:       index.RecordSchema,
		Key:          k,
		Size:         res.Size,
		ETag:         finalETag,
		ContentType:  rec.Meta.ContentType,
		ContentEnc:   rec.Meta.ContentEnc,
		ContentDisp:  rec.Meta.ContentDisp,
		CacheCtrl:    rec.Meta.CacheCtrl,
		Expires:      rec.Meta.Expires,
		CreatedAt:    now,
		ModifiedAt:   now,
		UserMeta:     rec.Meta.UserMeta,
		StorageClass: "STANDARD",
		Storage:      res.Ref,
	}

	if obj.Storage.Mode == "pack" {
		err = s.DB.Bolt().Update(func(tx *bolt.Tx) error {
			pack, err := index.GetPackTx(tx, b, obj.Storage.PackID)
			if err != nil {
				return err
			}
			end := obj.Storage.Offset + obj.Storage.Length
			if end > pack.Size {
				pack.Size = end
			}
			pack.LiveBytes += obj.Storage.Length
			if err := index.PutPackTx(tx, b, pack); err != nil {
				return err
			}
			return index.PutObjectTx(tx, b, obj)
		})
	} else {
		err = s.DB.PutObject(b, obj)
	}
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	// Remove staging dir and multipart record.
	_ = os.RemoveAll(storage.StagingDir(s.Cfg.Storage.DataDir, b, uploadID))
	_ = s.DB.DeleteMultipart(b, uploadID)

	writeXML(w, http.StatusOK, &CompleteMultipartUploadResult{
		Xmlns:    s3NS,
		Location: fmt.Sprintf("/%s/%s", b, k),
		Bucket:   b,
		Key:      k,
		ETag:     quoteETag(finalETag),
	})
}

// ---- DELETE /<bucket>/<key>?uploadId=X : AbortMultipartUpload ----

func (s *Server) handleAbortMultipart(w http.ResponseWriter, r *http.Request, uploadID string) {
	b := bucketName(r)
	if err := s.DB.DeleteMultipart(b, uploadID); err != nil {
		if errors.Is(err, index.ErrUploadNotFound) {
			writeS3Error(w, r, ErrNoSuchUpload)
			return
		}
		writeS3Error(w, r, translateErr(err))
		return
	}
	_ = os.RemoveAll(storage.StagingDir(s.Cfg.Storage.DataDir, b, uploadID))
	w.WriteHeader(http.StatusNoContent)
}

// ---- GET /<bucket>/<key>?uploadId=X : ListParts ----

func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	uploadID := r.URL.Query().Get("uploadId")
	rec, err := s.DB.GetMultipart(b, uploadID)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if rec.Key != k {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	owner, _ := s.DB.ServerID()
	out := ListPartsResult{
		Xmlns:        s3NS,
		Bucket:       b,
		Key:          k,
		UploadID:     uploadID,
		Initiator:    Owner{ID: owner, DisplayName: "smols3"},
		Owner:        Owner{ID: owner, DisplayName: "smols3"},
		StorageClass: "STANDARD",
		MaxParts:     1000,
	}
	for _, p := range rec.Parts {
		out.Parts = append(out.Parts, PartXML{
			PartNumber:   p.Number,
			LastModified: fmtIso(time.Unix(0, p.UploadedAt)),
			ETag:         quoteETag(p.ETag),
			Size:         p.Size,
		})
	}
	if len(rec.Parts) > 0 {
		out.NextPartNumberMarker = rec.Parts[len(rec.Parts)-1].Number
	}
	writeXML(w, http.StatusOK, &out)
}

// ---- GET /<bucket>?uploads : ListMultipartUploads ----

func (s *Server) handleListMultipart(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	uploads, err := s.DB.ListMultipart(b)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	owner, _ := s.DB.ServerID()
	out := ListMultipartUploadsResult{
		Xmlns:      s3NS,
		Bucket:     b,
		MaxUploads: 1000,
	}
	for _, u := range uploads {
		out.Uploads = append(out.Uploads, UploadEntry{
			Key:          u.Key,
			UploadID:     u.UploadID,
			Initiator:    Owner{ID: owner, DisplayName: "smols3"},
			Owner:        Owner{ID: owner, DisplayName: "smols3"},
			StorageClass: "STANDARD",
			Initiated:    fmtIso(time.Unix(0, u.Initiated)),
		})
	}
	writeXML(w, http.StatusOK, &out)
}

func partNumbers(ps []index.PartRecord) []int {
	out := make([]int, len(ps))
	for i, p := range ps {
		out[i] = p.Number
	}
	return out
}

func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
