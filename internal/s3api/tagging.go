package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
)

// ---- GET /<bucket>/<key>?tagging ----

func (s *Server) handleGetObjectTagging(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	rec, err := s.DB.GetObject(b, k)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	out := Tagging{Xmlns: s3NS}
	for tk, tv := range rec.Tags {
		out.TagSet.Tag = append(out.TagSet.Tag, Tag{Key: tk, Value: tv})
	}
	writeXML(w, http.StatusOK, &out)
}

// ---- PUT /<bucket>/<key>?tagging ----

func (s *Server) handlePutObjectTagging(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	rec, err := s.DB.GetObject(b, k)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	var req Tagging
	if err := xml.Unmarshal(body, &req); err != nil {
		writeS3Error(w, r, ErrMalformedXML)
		return
	}

	if len(req.TagSet.Tag) > 10 {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}

	rec.Tags = make(map[string]string, len(req.TagSet.Tag))
	for _, t := range req.TagSet.Tag {
		rec.Tags[t.Key] = t.Value
	}
	if err := s.DB.PutObject(b, rec); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---- DELETE /<bucket>/<key>?tagging ----

func (s *Server) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)
	k := objectKey(r)
	rec, err := s.DB.GetObject(b, k)
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	rec.Tags = nil
	if err := s.DB.PutObject(b, rec); err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
