package index

import (
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotFound = errors.New("bucket not found")
	ErrBucketNotEmpty = errors.New("bucket not empty")
)

func (d *DB) CreateBucket(name string) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		bb := tx.Bucket(bktBuckets)
		if bb.Get([]byte(name)) != nil {
			return ErrBucketExists
		}
		rec := BucketRecord{
			Schema:    RecordSchema,
			Name:      name,
			CreatedAt: time.Now().UnixNano(),
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := bb.Put([]byte(name), data); err != nil {
			return err
		}
		// Create per-bucket sub-buckets for objects, multipart, and packs.
		if _, err := tx.Bucket(bktObjects).CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
		if _, err := tx.Bucket(bktMultipart).CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
		if _, err := tx.Bucket(bktPacks).CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
		return nil
	})
}

func (d *DB) GetBucket(name string) (BucketRecord, error) {
	var rec BucketRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bktBuckets).Get([]byte(name))
		if raw == nil {
			return ErrBucketNotFound
		}
		return json.Unmarshal(raw, &rec)
	})
	return rec, err
}

func (d *DB) BucketExists(name string) (bool, error) {
	var ok bool
	err := d.bolt.View(func(tx *bolt.Tx) error {
		ok = tx.Bucket(bktBuckets).Get([]byte(name)) != nil
		return nil
	})
	return ok, err
}

// DeleteBucket removes a bucket. The bucket must contain no objects and no in-progress multipart uploads.
func (d *DB) DeleteBucket(name string) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		bb := tx.Bucket(bktBuckets)
		if bb.Get([]byte(name)) == nil {
			return ErrBucketNotFound
		}
		if ob := tx.Bucket(bktObjects).Bucket([]byte(name)); ob != nil {
			if k, _ := ob.Cursor().First(); k != nil {
				return ErrBucketNotEmpty
			}
		}
		if mb := tx.Bucket(bktMultipart).Bucket([]byte(name)); mb != nil {
			if k, _ := mb.Cursor().First(); k != nil {
				return ErrBucketNotEmpty
			}
		}
		if err := tx.Bucket(bktObjects).DeleteBucket([]byte(name)); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		if err := tx.Bucket(bktMultipart).DeleteBucket([]byte(name)); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		if err := tx.Bucket(bktPacks).DeleteBucket([]byte(name)); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		if err := tx.Bucket(bktPacksActive).Delete([]byte(name)); err != nil {
			return err
		}
		return bb.Delete([]byte(name))
	})
}

func (d *DB) ListBuckets() ([]BucketRecord, error) {
	var out []BucketRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bktBuckets).ForEach(func(_, v []byte) error {
			var rec BucketRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			out = append(out, rec)
			return nil
		})
	})
	return out, err
}
