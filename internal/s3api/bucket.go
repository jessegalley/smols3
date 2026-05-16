package s3api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jessegalley/smols3/internal/index"
)

// validBucketName is a relaxed S3 bucket name validator (test server is lenient).
func validBucketName(s string) bool {
	if len(s) < 1 || len(s) > 255 {
		return false
	}
	for _, r := range s {
		if r == '/' || r == '\\' || r == 0 {
			return false
		}
	}
	return true
}

// ---- HEAD /<bucket> : HeadBucket ----

func (s *Server) handleBucketHead(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	ok, err := s.DB.BucketExists(b)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("X-Amz-Bucket-Region", s.Cfg.Server.Region)
	w.WriteHeader(http.StatusOK)
}

// ---- PUT /<bucket> : CreateBucket (or subresource PUTs) ----

func (s *Server) handleBucketPut(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case q.Has("acl"), q.Has("cors"), q.Has("policy"), q.Has("logging"),
		q.Has("lifecycle"), q.Has("notification"), q.Has("replication"),
		q.Has("tagging"), q.Has("requestPayment"), q.Has("website"),
		q.Has("versioning"), q.Has("encryption"), q.Has("accelerate"),
		q.Has("inventory"), q.Has("metrics"), q.Has("analytics"),
		q.Has("intelligent-tiering"), q.Has("ownershipControls"),
		q.Has("object-lock"), q.Has("publicAccessBlock"):
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		return
	}

	b := bucketName(r)
	if !validBucketName(b) {
		writeS3Error(w, r, ErrInvalidBucketName)
		return
	}
	if err := s.DB.CreateBucket(b); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	w.Header().Set("Location", "/"+b)
	w.WriteHeader(http.StatusOK)
}

// ---- DELETE /<bucket> : DeleteBucket (or subresource DELETEs) ----

func (s *Server) handleBucketDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("acl") || q.Has("cors") || q.Has("policy") || q.Has("lifecycle") ||
		q.Has("tagging") || q.Has("website") || q.Has("encryption") ||
		q.Has("replication") || q.Has("inventory") || q.Has("metrics") ||
		q.Has("analytics") || q.Has("intelligent-tiering") ||
		q.Has("ownershipControls") || q.Has("publicAccessBlock") {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	b := bucketName(r)
	if err := s.DB.DeleteBucket(b); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if err := s.Storage.RemoveBucketTree(b); err != nil {
		s.Logger.Warn("remove bucket tree", "bucket", b, "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- GET /<bucket> : ListObjectsV2/V1 or subresource GET ----

func (s *Server) handleBucketGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Subresource GETs
	switch {
	case q.Has("location"):
		writeXML(w, http.StatusOK, &LocationConstraintResp{Xmlns: s3NS, Value: s.Cfg.Server.Region})
		return
	case q.Has("acl"):
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
	case q.Has("versioning"):
		writeXML(w, http.StatusOK, &VersioningConfig{Xmlns: s3NS})
		return
	case q.Has("uploads"):
		s.handleListMultipart(w, r)
		return
	case q.Has("cors"), q.Has("policy"), q.Has("logging"),
		q.Has("lifecycle"), q.Has("notification"), q.Has("replication"),
		q.Has("tagging"), q.Has("requestPayment"), q.Has("website"),
		q.Has("encryption"), q.Has("accelerate"),
		q.Has("inventory"), q.Has("metrics"), q.Has("analytics"),
		q.Has("intelligent-tiering"), q.Has("ownershipControls"),
		q.Has("object-lock"), q.Has("publicAccessBlock"):
		writeS3Error(w, r, ErrNotImplemented)
		return
	}

	b := bucketName(r)
	ok, err := s.DB.BucketExists(b)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	if !ok {
		writeS3Error(w, r, ErrNoSuchBucket)
		return
	}

	// Decide V1 vs V2
	if q.Get("list-type") == "2" {
		s.listObjectsV2(w, r, b, q)
		return
	}
	s.listObjectsV1(w, r, b, q)
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	prefix := first(q["prefix"])
	delim := first(q["delimiter"])
	startAfter := first(q["start-after"])
	token := first(q["continuation-token"])
	maxKeys := parseInt(first(q["max-keys"]), 1000)

	res, err := s.DB.ListObjects(bucket, index.ListOptions{
		Prefix:     prefix,
		Delimiter:  delim,
		MaxKeys:    maxKeys,
		StartAfter: startAfter,
		Token:      token,
	})
	if err != nil {
		if errors.Is(err, index.ErrBucketNotFound) {
			writeS3Error(w, r, ErrNoSuchBucket)
			return
		}
		writeS3Error(w, r, translateErr(err))
		return
	}

	out := ListBucketResultV2{
		Xmlns:             s3NS,
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delim,
		MaxKeys:           maxKeys,
		KeyCount:          len(res.Objects) + len(res.CommonPrefixes),
		IsTruncated:       res.IsTruncated,
		ContinuationToken: token,
		StartAfter:        startAfter,
	}
	if res.IsTruncated {
		out.NextContinuationToken = res.NextToken
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, ObjectXML{
			Key:          o.Key,
			LastModified: fmtIso(time.Unix(0, o.ModifiedAt)),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, CommonPrefix{Prefix: cp})
	}
	writeXML(w, http.StatusOK, &out)
}

func (s *Server) listObjectsV1(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	prefix := first(q["prefix"])
	delim := first(q["delimiter"])
	marker := first(q["marker"])
	maxKeys := parseInt(first(q["max-keys"]), 1000)

	res, err := s.DB.ListObjects(bucket, index.ListOptions{
		Prefix:     prefix,
		Delimiter:  delim,
		MaxKeys:    maxKeys,
		StartAfter: marker,
	})
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	out := ListBucketResultV1{
		Xmlns:       s3NS,
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		Delimiter:   delim,
		IsTruncated: res.IsTruncated,
	}
	if res.IsTruncated {
		out.NextMarker = res.NextToken
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, ObjectXML{
			Key:          o.Key,
			LastModified: fmtIso(time.Unix(0, o.ModifiedAt)),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, CommonPrefix{Prefix: cp})
	}
	writeXML(w, http.StatusOK, &out)
}

// ---- POST /<bucket> : DeleteObjects ----

func (s *Server) handleBucketPost(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("delete") {
		s.handleDeleteObjects(w, r)
		return
	}
	writeS3Error(w, r, ErrNotImplemented)
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func quoteETag(etag string) string {
	if etag == "" {
		return ""
	}
	return "\"" + etag + "\""
}
