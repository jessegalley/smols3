package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	bolt "go.etcd.io/bbolt"

	"github.com/jessegalley/smols3/internal/index"
)

// POST /<bucket>?delete : DeleteObjects (batch)
func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	b := bucketName(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, r, ErrInvalidRequest)
		return
	}
	var req DeleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeS3Error(w, r, ErrMalformedXML)
		return
	}

	out := DeleteResult{Xmlns: s3NS}
	for _, o := range req.Objects {
		rec, derr := s.DB.DeleteObject(b, o.Key)
		switch {
		case derr == nil:
			if rec.Storage.Mode == "pack" {
				_ = s.DB.Bolt().Update(func(tx *bolt.Tx) error {
					pack, err := index.GetPackTx(tx, b, rec.Storage.PackID)
					if err != nil {
						return nil
					}
					pack.LiveBytes -= rec.Storage.Length
					if pack.LiveBytes < 0 {
						pack.LiveBytes = 0
					}
					return index.PutPackTx(tx, b, pack)
				})
			} else {
				_ = s.Storage.Delete(rec.Storage)
			}
			if !req.Quiet {
				out.Deleted = append(out.Deleted, DeletedEntry{Key: o.Key})
			}
		case errors.Is(derr, index.ErrObjectNotFound):
			if !req.Quiet {
				out.Deleted = append(out.Deleted, DeletedEntry{Key: o.Key})
			}
		default:
			out.Errors = append(out.Errors, DeleteErrorE{
				Key: o.Key, Code: "InternalError", Message: derr.Error(),
			})
		}
	}
	writeXML(w, http.StatusOK, &out)
}
