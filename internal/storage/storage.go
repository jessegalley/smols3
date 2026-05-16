// Package storage handles object byte persistence on disk. It exposes two modes:
// "file" (1:1, one file per object) and "concat" (small objects packed into
// shared append-only pack files up to max_concat_size).
package storage

import (
	"errors"
	"io"

	"github.com/jessegalley/smols3/internal/index"
)

// PutResult is the outcome of a successful Put. The index record's StorageRef
// is populated based on which mode was used; the caller is responsible for
// committing the index entry.
type PutResult struct {
	Ref  index.StorageRef
	Size int64
	ETag string // hex MD5 of the body, populated by the hasher in Put
}

// Storage is the abstraction over the on-disk layout. Implementations:
//   - fileStorage (1:1)
//   - packStorage (concat, with per-bucket append writers)
type Storage interface {
	// Put streams body bytes to disk and returns where they live. size is the
	// caller-asserted byte count (for headers like Content-Length); -1 means
	// unknown, stream until EOF.
	Put(bucket, key string, size int64, body io.Reader) (PutResult, error)

	// Open returns a reader for an existing object record. Caller must Close.
	Open(ref index.StorageRef) (io.ReadCloser, error)

	// OpenRange returns a reader limited to [offset, offset+length).
	// offset/length are within the logical object, not the underlying file.
	OpenRange(ref index.StorageRef, offset, length int64) (io.ReadCloser, error)

	// Delete removes the on-disk bytes associated with ref. For pack-mode
	// records this is a no-op on the data file (bytes leak until compaction)
	// but adjusts pack accounting.
	Delete(ref index.StorageRef) error

	// RemoveBucketTree wipes the bucket's on-disk data (called after DeleteBucket).
	RemoveBucketTree(bucket string) error
}

var (
	ErrTooLarge       = errors.New("object exceeds max_object_size")
	ErrShortWrite     = errors.New("short write")
	ErrInvalidRef     = errors.New("invalid storage ref")
	ErrNoBody         = errors.New("nil body")
)
