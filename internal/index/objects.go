package index

import (
	"bytes"
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrObjectNotFound = errors.New("object not found")
)

// PutObject inserts or replaces an object record. The bucket must exist.
func (d *DB) PutObject(bucket string, rec ObjectRecord) error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		return PutObjectTx(tx, bucket, rec)
	})
}

// PutObjectTx allows insertion inside an externally managed bolt transaction
// (used by concat-mode flow that needs to update pack + object atomically).
func PutObjectTx(tx *bolt.Tx, bucket string, rec ObjectRecord) error {
	if tx.Bucket(bktBuckets).Get([]byte(bucket)) == nil {
		return ErrBucketNotFound
	}
	ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
	if ob == nil {
		return ErrBucketNotFound
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return ob.Put([]byte(rec.Key), data)
}

func (d *DB) GetObject(bucket, key string) (ObjectRecord, error) {
	var rec ObjectRecord
	err := d.bolt.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if ob == nil {
			return ErrBucketNotFound
		}
		raw := ob.Get([]byte(key))
		if raw == nil {
			return ErrObjectNotFound
		}
		return json.Unmarshal(raw, &rec)
	})
	return rec, err
}

// DeleteObject removes the index entry. It does NOT touch on-disk data (caller's responsibility).
// Returns the deleted record so caller can clean up storage.
func (d *DB) DeleteObject(bucket, key string) (ObjectRecord, error) {
	var rec ObjectRecord
	err := d.bolt.Update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if ob == nil {
			return ErrBucketNotFound
		}
		raw := ob.Get([]byte(key))
		if raw == nil {
			return ErrObjectNotFound
		}
		if err := json.Unmarshal(raw, &rec); err != nil {
			return err
		}
		return ob.Delete([]byte(key))
	})
	return rec, err
}

// ListResult is the outcome of a prefix-delimiter listing.
type ListResult struct {
	Objects        []ObjectRecord
	CommonPrefixes []string
	IsTruncated    bool
	NextToken      string // empty when not truncated
}

// ListOptions controls a ListObjects call.
type ListOptions struct {
	Prefix     string
	Delimiter  string
	MaxKeys    int    // 0 -> 1000 (S3 default)
	StartAfter string // exclusive
	Token      string // continuation token (== last key returned)
}

// ListObjects walks the bucket key-space lexicographically with prefix/delimiter
// semantics matching S3 ListObjectsV2.
func (d *DB) ListObjects(bucket string, opt ListOptions) (ListResult, error) {
	if opt.MaxKeys <= 0 || opt.MaxKeys > 1000 {
		opt.MaxKeys = 1000
	}
	var res ListResult
	err := d.bolt.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if ob == nil {
			return ErrBucketNotFound
		}
		c := ob.Cursor()

		// Starting position. Token is the literal Seek position for resume
		// (encoded by a previous truncated listing as nextKey or nextPrefix
		// depending on whether it ended on a key or a CommonPrefix). For an
		// initial request, StartAfter is exclusive; default is prefix.
		var start []byte
		switch {
		case opt.Token != "":
			start = []byte(opt.Token)
		case opt.StartAfter != "" && opt.StartAfter > opt.Prefix:
			start = nextKey([]byte(opt.StartAfter))
		default:
			start = []byte(opt.Prefix)
		}

		prefixBytes := []byte(opt.Prefix)
		seenPrefixes := make(map[string]struct{})
		// nextResume holds the Seek position to use if we truncate on the
		// *next* iteration. Updated after each emit.
		var nextResume []byte

		k, v := c.Seek(start)
		for k != nil {
			if !bytes.HasPrefix(k, prefixBytes) {
				break
			}

			// Common-prefix detection: if a delimiter exists in the tail after the prefix,
			// emit the CommonPrefix and skip past every key sharing it.
			if opt.Delimiter != "" {
				tail := k[len(prefixBytes):]
				if idx := bytes.Index(tail, []byte(opt.Delimiter)); idx >= 0 {
					cp := string(prefixBytes) + string(tail[:idx+len(opt.Delimiter)])
					if _, ok := seenPrefixes[cp]; !ok {
						if len(res.Objects)+len(res.CommonPrefixes) >= opt.MaxKeys {
							res.IsTruncated = true
							res.NextToken = string(nextResume)
							return nil
						}
						seenPrefixes[cp] = struct{}{}
						res.CommonPrefixes = append(res.CommonPrefixes, cp)
						nextResume = nextPrefix([]byte(cp))
					}
					skip := nextPrefix([]byte(cp))
					if skip == nil {
						return nil
					}
					k, v = c.Seek(skip)
					continue
				}
			}

			// Plain object.
			if len(res.Objects)+len(res.CommonPrefixes) >= opt.MaxKeys {
				res.IsTruncated = true
				res.NextToken = string(nextResume)
				return nil
			}
			var rec ObjectRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			res.Objects = append(res.Objects, rec)
			nextResume = nextKey([]byte(rec.Key))
			k, v = c.Next()
		}
		return nil
	})
	return res, err
}

// nextKey returns the smallest key strictly greater than k (k followed by a zero byte).
func nextKey(k []byte) []byte {
	out := make([]byte, len(k)+1)
	copy(out, k)
	out[len(k)] = 0
	return out
}

// nextPrefix returns the smallest byte slice that is strictly greater than every
// slice starting with p. Used to seek past all keys sharing a common prefix.
// Returns nil if p is all 0xff (no upper bound) or empty.
func nextPrefix(p []byte) []byte {
	if len(p) == 0 {
		return nil
	}
	out := make([]byte, len(p))
	copy(out, p)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// CountObjects returns the number of objects in a bucket. O(n) — for diagnostic use.
func (d *DB) CountObjects(bucket string) (int, error) {
	var n int
	err := d.bolt.View(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if ob == nil {
			return ErrBucketNotFound
		}
		return ob.ForEach(func(_, _ []byte) error {
			n++
			return nil
		})
	})
	return n, err
}

// IterAllObjects calls fn for every object in every bucket. Used by compaction / fsck.
func (d *DB) IterAllObjects(fn func(bucket string, rec ObjectRecord) error) error {
	return d.bolt.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bktObjects).ForEach(func(bname, _ []byte) error {
			ob := tx.Bucket(bktObjects).Bucket(bname)
			if ob == nil {
				return nil
			}
			return ob.ForEach(func(_, v []byte) error {
				var rec ObjectRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					return err
				}
				return fn(string(bname), rec)
			})
		})
	})
}
