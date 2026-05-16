package index

import (
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrPackNotFound = errors.New("pack not found")
)

// PackIDKey formats a pack ID as the 16-char lowercase hex bbolt key.
func PackIDKey(id uint64) []byte {
	return []byte(fmt.Sprintf("%016x", id))
}

// PutPackTx writes/updates a pack record inside a transaction.
func PutPackTx(tx *bolt.Tx, bucket string, rec PackFileRecord) error {
	pb := tx.Bucket(bktPacks).Bucket([]byte(bucket))
	if pb == nil {
		return ErrBucketNotFound
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return pb.Put(PackIDKey(rec.PackID), data)
}

// GetPackTx returns a pack record inside a transaction.
func GetPackTx(tx *bolt.Tx, bucket string, id uint64) (PackFileRecord, error) {
	var rec PackFileRecord
	pb := tx.Bucket(bktPacks).Bucket([]byte(bucket))
	if pb == nil {
		return rec, ErrBucketNotFound
	}
	raw := pb.Get(PackIDKey(id))
	if raw == nil {
		return rec, ErrPackNotFound
	}
	err := json.Unmarshal(raw, &rec)
	return rec, err
}

func (d *DB) GetPack(bucket string, id uint64) (PackFileRecord, error) {
	var rec PackFileRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		var e error
		rec, e = GetPackTx(tx, bucket, id)
		return e
	})
	return rec, err
}

// ActivePackTx returns the currently-active pack id for a bucket, or 0 if none.
func ActivePackTx(tx *bolt.Tx, bucket string) uint64 {
	b := tx.Bucket(bktPacksActive)
	v := b.Get([]byte(bucket))
	return beu64(v)
}

// SetActivePackTx records the active pack id for a bucket.
func SetActivePackTx(tx *bolt.Tx, bucket string, id uint64) error {
	return tx.Bucket(bktPacksActive).Put([]byte(bucket), u64be(id))
}

// NextPackIDTx allocates a new monotonic pack id for the bucket.
func NextPackIDTx(tx *bolt.Tx, bucket string) (uint64, error) {
	pb := tx.Bucket(bktPacks).Bucket([]byte(bucket))
	if pb == nil {
		return 0, ErrBucketNotFound
	}
	return pb.NextSequence()
}

// DeletePackTx removes a pack record from the index. Does NOT touch the file on disk.
func DeletePackTx(tx *bolt.Tx, bucket string, id uint64) error {
	pb := tx.Bucket(bktPacks).Bucket([]byte(bucket))
	if pb == nil {
		return ErrBucketNotFound
	}
	return pb.Delete(PackIDKey(id))
}

// IterPackObjectsTx calls fn for every ObjectRecord that references the given pack.
func IterPackObjectsTx(tx *bolt.Tx, bucket string, packID uint64, fn func(ObjectRecord) error) error {
	ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
	if ob == nil {
		return ErrBucketNotFound
	}
	return ob.ForEach(func(_, v []byte) error {
		var rec ObjectRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		if rec.Storage.Mode == "pack" && rec.Storage.PackID == packID {
			return fn(rec)
		}
		return nil
	})
}

// IterPacks calls fn for every pack in a bucket.
func (d *DB) IterPacks(bucket string, fn func(PackFileRecord) error) error {
	return d.bolt.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket(bktPacks).Bucket([]byte(bucket))
		if pb == nil {
			return ErrBucketNotFound
		}
		return pb.ForEach(func(_, v []byte) error {
			var rec PackFileRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			return fn(rec)
		})
	})
}
