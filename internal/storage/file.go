package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jessegalley/smols3/internal/index"
)

// fileStorage is the 1:1 mode: each object lives in its own file under the
// bucket directory, sharded by a hash prefix.
type fileStorage struct {
	dataDir    string
	shardDepth int
	maxObjSize int64
	fsync      bool
}

func NewFileStorage(dataDir string, shardDepth int, maxObjSize int64, fsync bool) Storage {
	return &fileStorage{
		dataDir:    dataDir,
		shardDepth: shardDepth,
		maxObjSize: maxObjSize,
		fsync:      fsync,
	}
}

func (s *fileStorage) Put(bucket, key string, size int64, body io.Reader) (PutResult, error) {
	if body == nil {
		return PutResult{}, ErrNoBody
	}
	if size >= 0 && s.maxObjSize > 0 && size > s.maxObjSize {
		return PutResult{}, ErrTooLarge
	}
	rel := ObjectFileRelPath(bucket, key, s.shardDepth)
	abs := filepath.Join(s.dataDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return PutResult{}, fmt.Errorf("mkdir for object: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), ".tmp-*")
	if err != nil {
		return PutResult{}, fmt.Errorf("create tempfile: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}

	var written int64
	if s.maxObjSize > 0 {
		written, err = io.CopyN(tmp, body, s.maxObjSize+1)
		if err != nil && err != io.EOF {
			cleanup()
			return PutResult{}, fmt.Errorf("write body: %w", err)
		}
		if written > s.maxObjSize {
			cleanup()
			return PutResult{}, ErrTooLarge
		}
	} else {
		written, err = io.Copy(tmp, body)
		if err != nil {
			cleanup()
			return PutResult{}, fmt.Errorf("write body: %w", err)
		}
	}
	if size >= 0 && written != size {
		cleanup()
		return PutResult{}, fmt.Errorf("%w: declared %d got %d", ErrShortWrite, size, written)
	}
	if s.fsync {
		if err := tmp.Sync(); err != nil {
			cleanup()
			return PutResult{}, fmt.Errorf("fsync: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return PutResult{}, err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		_ = os.Remove(tmpName)
		return PutResult{}, fmt.Errorf("rename: %w", err)
	}
	return PutResult{
		Ref: index.StorageRef{
			Mode:   "file",
			Path:   rel,
			Offset: 0,
			Length: written,
		},
		Size: written,
	}, nil
}

func (s *fileStorage) Open(ref index.StorageRef) (io.ReadCloser, error) {
	if ref.Mode != "file" {
		return nil, ErrInvalidRef
	}
	abs := filepath.Join(s.dataDir, filepath.FromSlash(ref.Path))
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *fileStorage) OpenRange(ref index.StorageRef, offset, length int64) (io.ReadCloser, error) {
	if ref.Mode != "file" {
		return nil, ErrInvalidRef
	}
	abs := filepath.Join(s.dataDir, filepath.FromSlash(ref.Path))
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &limitedReadCloser{Reader: io.LimitReader(f, length), close: f.Close}, nil
}

func (s *fileStorage) Delete(ref index.StorageRef) error {
	if ref.Mode != "file" {
		return ErrInvalidRef
	}
	abs := filepath.Join(s.dataDir, filepath.FromSlash(ref.Path))
	err := os.Remove(abs)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *fileStorage) RemoveBucketTree(bucket string) error {
	dir := filepath.Join(s.dataDir, bucket)
	return os.RemoveAll(dir)
}

type limitedReadCloser struct {
	io.Reader
	close func() error
}

func (l *limitedReadCloser) Close() error { return l.close() }
