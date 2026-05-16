// Package s3api implements the S3-compatible HTTP layer.
package s3api

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/jessegalley/smols3/internal/config"
	"github.com/jessegalley/smols3/internal/index"
	"github.com/jessegalley/smols3/internal/sigv4"
	"github.com/jessegalley/smols3/internal/storage"
)

// Server is the HTTP handler that implements the S3 wire protocol against
// the index DB and storage backend.
type Server struct {
	Cfg     config.Config
	DB      *index.DB
	Storage *storage.Storage
	Logger  *slog.Logger
}

// Router builds the chi router with all routes wired up.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if s.Cfg.Log.AccessLog {
		r.Use(s.accessLog)
	}
	r.Use(s.auth)

	// Service-level: GET / -> ListBuckets
	r.Get("/", s.handleListBuckets)

	// Bucket-level (paths with no key portion).
	r.Route("/{bucket}", func(r chi.Router) {
		r.Get("/", s.handleBucketGet)        // ListObjectsV2/V1 + subresource GETs (acl, location, versioning, ...)
		r.Head("/", s.handleBucketHead)      // HeadBucket
		r.Put("/", s.handleBucketPut)        // CreateBucket + subresource PUTs (stubs)
		r.Delete("/", s.handleBucketDelete)  // DeleteBucket + subresource DELETEs (stubs)
		r.Post("/", s.handleBucketPost)      // DeleteObjects (?delete)

		// Object-level. We can't use chi's path matching naively because
		// keys contain slashes; we capture the rest via {key:.*}.
		r.Get("/*", s.handleObjectGet)
		r.Head("/*", s.handleObjectHead)
		r.Put("/*", s.handleObjectPut)
		r.Delete("/*", s.handleObjectDelete)
		r.Post("/*", s.handleObjectPost)
	})

	return r
}

// objectKey extracts the URL key portion from a chi-matched request. Chi
// strips the leading "/{bucket}/" so the URL.Path tail is what we need.
func objectKey(r *http.Request) string {
	// chi.URLParam doesn't capture wildcard well for keys with slashes; use the path.
	path := r.URL.Path
	// path is like /bucket/foo/bar/baz; strip the leading "/<bucket>/"
	// chi.RouteContext gives us the bucket.
	rctx := chi.RouteContext(r.Context())
	bucket := rctx.URLParam("bucket")
	prefix := "/" + bucket + "/"
	if strings.HasPrefix(path, prefix) {
		return path[len(prefix):]
	}
	return ""
}

func bucketName(r *http.Request) string {
	return chi.URLParamFromCtx(r.Context(), "bucket")
}

// auth is the SigV4 verification middleware. Bypassed when auth_mode == "none".
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Cfg.Auth.Mode == "none" {
			next.ServeHTTP(w, r)
			return
		}
		creds := sigv4.Credentials{AccessKey: s.Cfg.Auth.AccessKey, SecretKey: s.Cfg.Auth.SecretKey}
		if err := sigv4.Verify(r, creds, s.Cfg.Server.Region); err != nil {
			s.Logger.Debug("sigv4 verify failed", "err", err)
			writeS3Error(w, r, ErrSignatureMismatch)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		s.Logger.Info("req",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
		)
	})
}

// ---- response helpers ----

func writeXML(w http.ResponseWriter, status int, v interface{}) {
	body, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// translateErr maps internal package errors into appropriate S3Errors.
func translateErr(err error) *S3Error {
	switch {
	case errors.Is(err, index.ErrBucketNotFound):
		return ErrNoSuchBucket
	case errors.Is(err, index.ErrObjectNotFound):
		return ErrNoSuchKey
	case errors.Is(err, index.ErrUploadNotFound):
		return ErrNoSuchUpload
	case errors.Is(err, index.ErrBucketExists):
		return ErrBucketAlreadyOwnedByYou
	case errors.Is(err, index.ErrBucketNotEmpty):
		return ErrBucketNotEmpty
	case errors.Is(err, storage.ErrTooLarge):
		return ErrEntityTooLarge
	}
	e := *ErrInternal
	e.Message = err.Error()
	return &e
}
