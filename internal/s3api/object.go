package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/jessegalley/smols3/internal/etag"
	"github.com/jessegalley/smols3/internal/index"
)

// ---- HEAD /<bucket>/<key> ----

func (s *Server) handleObjectHead(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	rec, err := s.DB.GetObject(b, k)
	if err != nil {
		if errors.Is(err, index.ErrObjectNotFound) || errors.Is(err, index.ErrBucketNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeS3Error(w, r, translateErr(err))
		return
	}
	writeObjectHeaders(w, rec)
	w.WriteHeader(http.StatusOK)
}

// ---- GET /<bucket>/<key> ----

func (s *Server) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if q.Has("tagging") {
		s.handleGetObjectTagging(w, r)
		return
	}
	if q.Has("acl") {
		owner, _ := s.DB.ServerID()
		writeXML(w, http.StatusOK, &AccessControlPolicy{
			Owner: Owner{ID: owner, DisplayName: "smols3"},
			AccessControlList: AccessControlList{
				Grant: []Grant{{
					Grantee: Grantee{
						Type:        "CanonicalUser",
						XmlnsXSI:    "http://www.w3.org/2001/XMLSchema-instance",
						ID:          owner,
						DisplayName: "smols3",
					},
					Permission: "FULL_CONTROL",
				}},
			},
		})
		return
	}
	if uid := q.Get("uploadId"); uid != "" {
		s.handleListParts(w, r)
		return
	}

	b := bucketName(r)
	k := objectKey(r)
	rec, err := s.DB.GetObject(b, k)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	// Range support
	rng := r.Header.Get("Range")
	if rng != "" {
		off, length, ok := parseRange(rng, rec.Size)
		if !ok {
			writeS3Error(w, r, ErrInvalidRange)
			return
		}
		rc, err := s.Storage.OpenRange(rec.Storage, off, length)
		if err != nil {
			writeS3Error(w, r, translateErr(err))
			return
		}
		defer rc.Close()
		writeObjectHeaders(w, rec)
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+length-1, rec.Size))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(w, rc)
		return
	}

	rc, err := s.Storage.Open(rec.Storage)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	defer rc.Close()
	writeObjectHeaders(w, rec)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// parseRange parses an HTTP Range header like "bytes=0-499" against an object
// of size n and returns (offset, length, ok).
func parseRange(h string, n int64) (int64, int64, bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, prefix)
	// multi-range not supported
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false
	}
	startStr, endStr := spec[:dash], spec[dash+1:]
	if startStr == "" {
		// suffix range: last N bytes
		nn, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || nn <= 0 {
			return 0, 0, false
		}
		if nn > n {
			nn = n
		}
		return n - nn, nn, true
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 || start >= n {
		return 0, 0, false
	}
	if endStr == "" {
		return start, n - start, true
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= n {
		end = n - 1
	}
	return start, end - start + 1, true
}

// ---- PUT /<bucket>/<key> ----

func (s *Server) handleObjectPut(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Multipart UploadPart
	if uid := q.Get("uploadId"); uid != "" {
		s.handleUploadPart(w, r)
		return
	}
	if q.Has("tagging") {
		s.handlePutObjectTagging(w, r)
		return
	}
	if q.Has("acl") {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Server-side copy: PUT with x-amz-copy-source header.
	if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
		s.handleCopyObject(w, r, src)
		return
	}

	s.putObjectBody(w, r)
}

func (s *Server) putObjectBody(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	if k == "" {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}

	size := r.ContentLength

	// Tee through MD5 hasher so we can compute the ETag.
	teedReader, hasher := etag.TeeReader(r.Body)

	// In concat mode we strictly need size; if absent we fall through to file
	// fallback by passing size=-1 — the storage layer's pack.Put already routes
	// unknown-size to file mode.
	res, err := s.Storage.Put(b, k, size, teedReader)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	// Build & commit the record.
	now := time.Now().UnixNano()
	rec := index.ObjectRecord{
		Schema:       index.RecordSchema,
		Key:          k,
		Size:         res.Size,
		ETag:         hasher.SumHex(),
		ContentType:  r.Header.Get("Content-Type"),
		ContentEnc:   r.Header.Get("Content-Encoding"),
		ContentDisp:  r.Header.Get("Content-Disposition"),
		CacheCtrl:    r.Header.Get("Cache-Control"),
		Expires:      r.Header.Get("Expires"),
		CreatedAt:    now,
		ModifiedAt:   now,
		UserMeta:     extractUserMeta(r.Header),
		StorageClass: "STANDARD",
		Storage:      res.Ref,
	}

	if rec.Storage.Mode == "pack" {
		// Atomically update PackFileRecord (Size/LiveBytes) and insert ObjectRecord.
		err = s.DB.Bolt().Update(func(tx *bolt.Tx) error {
			pack, err := index.GetPackTx(tx, b, rec.Storage.PackID)
			if err != nil {
				return err
			}
			end := rec.Storage.Offset + rec.Storage.Length
			if end > pack.Size {
				pack.Size = end
			}
			pack.LiveBytes += rec.Storage.Length
			if err := index.PutPackTx(tx, b, pack); err != nil {
				return err
			}
			return index.PutObjectTx(tx, b, rec)
		})
	} else {
		err = s.DB.PutObject(b, rec)
	}
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	w.Header().Set("ETag", quoteETag(rec.ETag))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, src string) {
	src = strings.TrimPrefix(src, "/")
	// May be URL-encoded.
	if decoded, err := url.QueryUnescape(src); err == nil {
		src = decoded
	}
	slash := strings.IndexByte(src, '/')
	if slash <= 0 {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	srcBucket := src[:slash]
	srcKey := src[slash+1:]
	srcRec, err := s.DB.GetObject(srcBucket, srcKey)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	rc, err := s.Storage.Open(srcRec.Storage)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	defer rc.Close()

	b := bucketName(r)
	k := objectKey(r)
	teed, hasher := etag.TeeReader(rc)
	res, err := s.Storage.Put(b, k, srcRec.Size, teed)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	now := time.Now().UnixNano()
	directive := strings.ToUpper(r.Header.Get("X-Amz-Metadata-Directive"))
	rec := index.ObjectRecord{
		Schema:       index.RecordSchema,
		Key:          k,
		Size:         res.Size,
		ETag:         hasher.SumHex(),
		CreatedAt:    now,
		ModifiedAt:   now,
		StorageClass: "STANDARD",
		Storage:      res.Ref,
	}
	if directive == "REPLACE" {
		rec.ContentType = r.Header.Get("Content-Type")
		rec.ContentEnc = r.Header.Get("Content-Encoding")
		rec.ContentDisp = r.Header.Get("Content-Disposition")
		rec.CacheCtrl = r.Header.Get("Cache-Control")
		rec.Expires = r.Header.Get("Expires")
		rec.UserMeta = extractUserMeta(r.Header)
	} else {
		rec.ContentType = srcRec.ContentType
		rec.ContentEnc = srcRec.ContentEnc
		rec.ContentDisp = srcRec.ContentDisp
		rec.CacheCtrl = srcRec.CacheCtrl
		rec.Expires = srcRec.Expires
		rec.UserMeta = srcRec.UserMeta
		rec.Tags = srcRec.Tags
	}

	if rec.Storage.Mode == "pack" {
		err = s.DB.Bolt().Update(func(tx *bolt.Tx) error {
			pack, err := index.GetPackTx(tx, b, rec.Storage.PackID)
			if err != nil {
				return err
			}
			end := rec.Storage.Offset + rec.Storage.Length
			if end > pack.Size {
				pack.Size = end
			}
			pack.LiveBytes += rec.Storage.Length
			if err := index.PutPackTx(tx, b, pack); err != nil {
				return err
			}
			return index.PutObjectTx(tx, b, rec)
		})
	} else {
		err = s.DB.PutObject(b, rec)
	}
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	body, _ := xml.MarshalIndent(&CopyObjectResult{
		Xmlns:        s3NS,
		ETag:         quoteETag(rec.ETag),
		LastModified: fmtIso(time.Unix(0, rec.ModifiedAt)),
	}, "", "  ")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// ---- DELETE /<bucket>/<key> ----

func (s *Server) handleObjectDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if uid := q.Get("uploadId"); uid != "" {
		s.handleAbortMultipart(w, r, uid)
		return
	}
	if q.Has("tagging") {
		s.handleDeleteObjectTagging(w, r)
		return
	}
	s.deleteSingleObject(w, r)
}

func (s *Server) deleteSingleObject(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)

	rec, err := s.DB.DeleteObject(b, k)
	if err != nil {
		if errors.Is(err, index.ErrObjectNotFound) {
			// S3 returns 204 on delete of nonexistent key (idempotent).
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if errors.Is(err, index.ErrBucketNotFound) {
			writeS3Error(w, r, ErrNoSuchBucket)
			return
		}
		writeS3Error(w, r, translateErr(err))
		return
	}

	// Adjust pack accounting for pack-mode references; remove file for file mode.
	if rec.Storage.Mode == "pack" {
		err = s.DB.Bolt().Update(func(tx *bolt.Tx) error {
			pack, err := index.GetPackTx(tx, b, rec.Storage.PackID)
			if err != nil {
				return nil // ignore — pack may have been compacted away
			}
			pack.LiveBytes -= rec.Storage.Length
			if pack.LiveBytes < 0 {
				pack.LiveBytes = 0
			}
			return index.PutPackTx(tx, b, pack)
		})
		if err != nil {
			s.Logger.Warn("pack accounting on delete", "err", err)
		}
	} else {
		if err := s.Storage.Delete(rec.Storage); err != nil {
			s.Logger.Warn("storage delete", "err", err)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- POST /<bucket>/<key> ----

func (s *Server) handleObjectPost(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("uploads") {
		s.handleCreateMultipart(w, r)
		return
	}
	if uid := q.Get("uploadId"); uid != "" {
		s.handleCompleteMultipart(w, r, uid)
		return
	}
	writeS3Error(w, r, ErrNotImplemented)
}

// ---- helpers ----

func extractUserMeta(h http.Header) map[string]string {
	var out map[string]string
	const pfx = "X-Amz-Meta-"
	for k, v := range h {
		if len(v) > 0 && strings.HasPrefix(k, pfx) {
			if out == nil {
				out = make(map[string]string)
			}
			out[strings.ToLower(k[len(pfx):])] = v[0]
		}
	}
	return out
}

func writeObjectHeaders(w http.ResponseWriter, rec index.ObjectRecord) {
	w.Header().Set("ETag", quoteETag(rec.ETag))
	w.Header().Set("Content-Length", strconv.FormatInt(rec.Size, 10))
	if rec.ContentType != "" {
		w.Header().Set("Content-Type", rec.ContentType)
	}
	if rec.ContentEnc != "" {
		w.Header().Set("Content-Encoding", rec.ContentEnc)
	}
	if rec.ContentDisp != "" {
		w.Header().Set("Content-Disposition", rec.ContentDisp)
	}
	if rec.CacheCtrl != "" {
		w.Header().Set("Cache-Control", rec.CacheCtrl)
	}
	if rec.Expires != "" {
		w.Header().Set("Expires", rec.Expires)
	}
	w.Header().Set("Last-Modified", time.Unix(0, rec.ModifiedAt).UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	for k, v := range rec.UserMeta {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}
}

// avoid unused-import warning when filepath is not referenced elsewhere
var _ = filepath.Join
