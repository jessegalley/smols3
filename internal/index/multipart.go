package index

import (
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

var ErrUploadNotFound = errors.New("multipart upload not found")

func (d *DB) PutMultipart(rec MultipartRecord) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bktBuckets).Get([]byte(rec.Bucket)) == nil {
			return ErrBucketNotFound
		}
		mb := tx.Bucket(bktMultipart).Bucket([]byte(rec.Bucket))
		if mb == nil {
			return ErrBucketNotFound
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return mb.Put([]byte(rec.UploadID), data)
	})
}

func (d *DB) GetMultipart(bucket, uploadID string) (MultipartRecord, error) {
	var rec MultipartRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bktMultipart).Bucket([]byte(bucket))
		if mb == nil {
			return ErrBucketNotFound
		}
		raw := mb.Get([]byte(uploadID))
		if raw == nil {
			return ErrUploadNotFound
		}
		return json.Unmarshal(raw, &rec)
	})
	return rec, err
}

func (d *DB) DeleteMultipart(bucket, uploadID string) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bktMultipart).Bucket([]byte(bucket))
		if mb == nil {
			return ErrBucketNotFound
		}
		if mb.Get([]byte(uploadID)) == nil {
			return ErrUploadNotFound
		}
		return mb.Delete([]byte(uploadID))
	})
}

// UpsertPart atomically reads the multipart record, upserts the given part by
// number (preserving sorted order), and writes the record back. This is the
// correct path for concurrent UploadPart calls that would otherwise race.
func (d *DB) UpsertPart(bucket, uploadID string, part PartRecord) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bktMultipart).Bucket([]byte(bucket))
		if mb == nil {
			return ErrBucketNotFound
		}
		raw := mb.Get([]byte(uploadID))
		if raw == nil {
			return ErrUploadNotFound
		}
		var rec MultipartRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return err
		}
		replaced := false
		for i := range rec.Parts {
			if rec.Parts[i].Number == part.Number {
				rec.Parts[i] = part
				replaced = true
				break
			}
		}
		if !replaced {
			rec.Parts = append(rec.Parts, part)
		}
		// keep sorted by part number
		for i := 1; i < len(rec.Parts); i++ {
			for j := i; j > 0 && rec.Parts[j-1].Number > rec.Parts[j].Number; j-- {
				rec.Parts[j-1], rec.Parts[j] = rec.Parts[j], rec.Parts[j-1]
			}
		}
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return mb.Put([]byte(uploadID), out)
	})
}

func (d *DB) ListMultipart(bucket string) ([]MultipartRecord, error) {
	var out []MultipartRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bktMultipart).Bucket([]byte(bucket))
		if mb == nil {
			return ErrBucketNotFound
		}
		return mb.ForEach(func(_, v []byte) error {
			var rec MultipartRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			out = append(out, rec)
			return nil
		})
	})
	return out, err
}
