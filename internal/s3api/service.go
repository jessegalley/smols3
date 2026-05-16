package s3api

import (
	"net/http"
	"time"
)

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	bkts, err := s.DB.ListBuckets()
	if err != nil {
		writeS3Error(w, r, translateErr(err))
		return
	}
	serverID, _ := s.DB.ServerID()
	resp := ListAllMyBucketsResult{
		Xmlns: s3NS,
		Owner: Owner{ID: serverID, DisplayName: "smols3"},
	}
	for _, b := range bkts {
		resp.Buckets.Bucket = append(resp.Buckets.Bucket, BucketEntry{
			Name:         b.Name,
			CreationDate: time.Unix(0, b.CreatedAt).UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, &resp)
}
