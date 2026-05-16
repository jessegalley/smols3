// Package storage handles object byte persistence on disk. Two on-disk
// layouts coexist:
//
//   - file mode (1:1): each object is its own file under
//     <data_dir>/<bucket>/<sharded path>.
//   - pack mode (concat): small objects are appended into shared pack files
//     up to max_concat_size; larger objects fall through to 1:1 files.
//
// The configured mode only affects how *new* PUTs are placed. Reads, range
// reads, and deletes dispatch on the StorageRef's recorded Mode and work
// regardless of the currently-configured mode. This means an object written
// in concat mode is fully readable after the server is restarted in file
// mode and vice versa.
package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/jessegalley/smols3/internal/index"
)

// Deps groups the parameters NewStorage needs.
type Deps struct {
	DataDir               string
	Mode                  string // "file" | "concat"
	ShardDepth            int
	MaxObjSize            int64
	MaxConcatSize         int64
	MaxPackableObjectSize int64
	Fsync                 bool
	DB                    *index.DB // required when Mode == "concat"
}

// PutResult is the outcome of a successful Put. The caller is responsible
// for committing the index entry that points at Ref.
type PutResult struct {
	Ref  index.StorageRef
	Size int64
}

// Storage is the unified read/write surface over the on-disk byte layout.
// Behavior at a glance:
//
//   - Put places bytes according to the configured mode (file or concat).
//   - Open / OpenRange dispatch on ref.Mode and work for any ref, regardless
//     of which mode the server is currently configured for.
//   - Delete handles file refs (os.Remove); pack refs are a no-op (the
//     caller adjusts PackFileRecord.LiveBytes in its own bolt tx).
type Storage struct {
	deps Deps

	// per-bucket pack-writer state (lazily allocated; unused in file mode)
	packsMu sync.Mutex
	packs   map[string]*bucketPackState
}

type bucketPackState struct {
	mu         sync.Mutex
	activeID   uint64
	activeFile *os.File
	activeSize int64 // logical EOF == next reservable offset; in-memory only
	inflight   sync.WaitGroup
}

var (
	ErrTooLarge     = errors.New("object exceeds max_object_size")
	ErrShortWrite   = errors.New("short write")
	ErrInvalidRef   = errors.New("invalid storage ref")
	ErrNoBody       = errors.New("nil body")
	ErrSizeRequired = errors.New("concat mode requires known Content-Length")
)

// New returns a Storage configured with the given deps.
func New(d Deps) *Storage {
	return &Storage{
		deps:  d,
		packs: make(map[string]*bucketPackState),
	}
}

// ---- writes ----

// Put streams body bytes to disk and returns where they live. size is the
// caller-asserted byte count (-1 means unknown, stream until EOF). The
// configured mode plus size determines the placement: concat-eligible
// objects go into the active pack; everything else gets a 1:1 file.
func (s *Storage) Put(bucket, key string, size int64, body io.Reader) (PutResult, error) {
	if body == nil {
		return PutResult{}, ErrNoBody
	}
	if size >= 0 && s.deps.MaxObjSize > 0 && size > s.deps.MaxObjSize {
		return PutResult{}, ErrTooLarge
	}
	// concat-eligibility test
	if s.deps.Mode == "concat" && size >= 0 && size <= s.deps.MaxPackableObjectSize {
		return s.putPack(bucket, key, size, body)
	}
	return s.putFile(bucket, key, size, body)
}

// putFile writes a single 1:1 file via tempfile + atomic rename.
func (s *Storage) putFile(bucket, key string, size int64, body io.Reader) (PutResult, error) {
	rel := ObjectFileRelPath(bucket, key, s.deps.ShardDepth)
	abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(rel))
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
	if s.deps.MaxObjSize > 0 {
		written, err = io.CopyN(tmp, body, s.deps.MaxObjSize+1)
		if err != nil && err != io.EOF {
			cleanup()
			return PutResult{}, fmt.Errorf("write body: %w", err)
		}
		if written > s.deps.MaxObjSize {
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
	if s.deps.Fsync {
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

// putPack reserves a byte range in the active pack file, streams the body
// into it via positional writes, and returns a pack-mode ref. The caller
// commits PackFileRecord.Size/LiveBytes alongside the new ObjectRecord in
// its own bolt tx (see internal/s3api/object.go).
func (s *Storage) putPack(bucket, key string, size int64, body io.Reader) (PutResult, error) {
	if s.deps.DB == nil {
		return PutResult{}, errors.New("pack mode requires index.DB")
	}
	st := s.stateFor(bucket)

	st.mu.Lock()
	if st.activeFile == nil {
		if err := s.openActiveLocked(bucket, st); err != nil {
			st.mu.Unlock()
			return PutResult{}, err
		}
	}
	if size > s.deps.MaxConcatSize-st.activeSize {
		st.mu.Unlock()
		st.inflight.Wait()
		st.mu.Lock()
		if err := s.rotateLocked(bucket, st); err != nil {
			st.mu.Unlock()
			return PutResult{}, err
		}
	}
	packID := st.activeID
	offset := st.activeSize
	file := st.activeFile
	st.activeSize += size
	st.inflight.Add(1)
	st.mu.Unlock()

	written, err := writeAt(file, offset, size, body)
	if err != nil {
		st.inflight.Done()
		return PutResult{}, err
	}
	if s.deps.Fsync {
		if err := file.Sync(); err != nil {
			st.inflight.Done()
			return PutResult{}, fmt.Errorf("fsync pack: %w", err)
		}
	}
	st.inflight.Done()

	return PutResult{
		Ref: index.StorageRef{
			Mode:   "pack",
			Path:   PackFileRelPath(bucket, packID),
			Offset: offset,
			Length: written,
			PackID: packID,
		},
		Size: written,
	}, nil
}

func (s *Storage) stateFor(bucket string) *bucketPackState {
	s.packsMu.Lock()
	defer s.packsMu.Unlock()
	st, ok := s.packs[bucket]
	if !ok {
		st = &bucketPackState{}
		s.packs[bucket] = st
	}
	return st
}

func (s *Storage) openActiveLocked(bucket string, st *bucketPackState) error {
	var activeID uint64
	var pack index.PackFileRecord
	err := s.deps.DB.Bolt().View(func(tx *bolt.Tx) error {
		activeID = index.ActivePackTx(tx, bucket)
		if activeID != 0 {
			var e error
			pack, e = index.GetPackTx(tx, bucket, activeID)
			return e
		}
		return nil
	})
	if err != nil && !errors.Is(err, index.ErrPackNotFound) {
		return err
	}
	if activeID == 0 || errors.Is(err, index.ErrPackNotFound) {
		return s.allocateNewActiveLocked(bucket, st)
	}
	abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(pack.Path))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open active pack: %w", err)
	}
	st.activeID = pack.PackID
	st.activeFile = f
	st.activeSize = pack.Size
	return nil
}

func (s *Storage) allocateNewActiveLocked(bucket string, st *bucketPackState) error {
	var newID uint64
	err := s.deps.DB.Bolt().Update(func(tx *bolt.Tx) error {
		id, err := index.NextPackIDTx(tx, bucket)
		if err != nil {
			return err
		}
		newID = id
		rec := index.PackFileRecord{
			Schema:    index.RecordSchema,
			PackID:    id,
			Path:      PackFileRelPath(bucket, id),
			Size:      0,
			Sealed:    false,
			LiveBytes: 0,
			CreatedAt: time.Now().UnixNano(),
		}
		if err := index.PutPackTx(tx, bucket, rec); err != nil {
			return err
		}
		return index.SetActivePackTx(tx, bucket, id)
	})
	if err != nil {
		return err
	}
	abs := PackFilePath(s.deps.DataDir, bucket, newID)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			f, err = os.OpenFile(abs, os.O_RDWR, 0o644)
		}
		if err != nil {
			return fmt.Errorf("create pack file: %w", err)
		}
	}
	st.activeID = newID
	st.activeFile = f
	st.activeSize = 0
	return nil
}

func (s *Storage) rotateLocked(bucket string, st *bucketPackState) error {
	if st.activeFile != nil {
		if s.deps.Fsync {
			_ = st.activeFile.Sync()
		}
		_ = st.activeFile.Close()
	}
	if st.activeID != 0 {
		err := s.deps.DB.Bolt().Update(func(tx *bolt.Tx) error {
			rec, err := index.GetPackTx(tx, bucket, st.activeID)
			if err != nil {
				return err
			}
			rec.Sealed = true
			return index.PutPackTx(tx, bucket, rec)
		})
		if err != nil {
			return err
		}
	}
	st.activeFile = nil
	st.activeID = 0
	st.activeSize = 0
	return s.allocateNewActiveLocked(bucket, st)
}

func writeAt(f *os.File, offset, size int64, body io.Reader) (int64, error) {
	const bufSize = 64 * 1024
	buf := make([]byte, bufSize)
	var total int64
	pos := offset
	remaining := size
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		r, err := io.ReadFull(body, buf[:n])
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return total, fmt.Errorf("read body: %w", err)
		}
		if r == 0 {
			if remaining > 0 {
				return total, fmt.Errorf("%w: %d bytes unread", ErrShortWrite, remaining)
			}
			break
		}
		w, werr := f.WriteAt(buf[:r], pos)
		if werr != nil {
			return total, fmt.Errorf("write pack: %w", werr)
		}
		if w != r {
			return total, ErrShortWrite
		}
		pos += int64(w)
		total += int64(w)
		remaining -= int64(w)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	return total, nil
}

// ---- reads (mode-agnostic; dispatch on ref) ----

// Open returns a reader for an object. Dispatches on ref.Mode — works for
// pack-mode refs even when the server is currently configured in file mode
// and vice versa.
func (s *Storage) Open(ref index.StorageRef) (io.ReadCloser, error) {
	switch ref.Mode {
	case "file":
		abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(ref.Path))
		return os.Open(abs)
	case "pack":
		abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(ref.Path))
		f, err := os.Open(abs)
		if err != nil {
			return nil, err
		}
		if _, err := f.Seek(ref.Offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &limitedReadCloser{Reader: io.LimitReader(f, ref.Length), close: f.Close}, nil
	default:
		return nil, ErrInvalidRef
	}
}

// OpenRange returns a reader bounded to [offset, offset+length) within the
// logical object (not the underlying file).
func (s *Storage) OpenRange(ref index.StorageRef, offset, length int64) (io.ReadCloser, error) {
	switch ref.Mode {
	case "file":
		abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(ref.Path))
		f, err := os.Open(abs)
		if err != nil {
			return nil, err
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &limitedReadCloser{Reader: io.LimitReader(f, length), close: f.Close}, nil
	case "pack":
		abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(ref.Path))
		f, err := os.Open(abs)
		if err != nil {
			return nil, err
		}
		if _, err := f.Seek(ref.Offset+offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &limitedReadCloser{Reader: io.LimitReader(f, length), close: f.Close}, nil
	default:
		return nil, ErrInvalidRef
	}
}

// ---- deletes ----

// Delete removes on-disk bytes for a ref. For file refs this is os.Remove;
// for pack refs this is a no-op — the caller adjusts PackFileRecord.LiveBytes
// in its own bolt tx, and compact eventually reclaims the bytes.
func (s *Storage) Delete(ref index.StorageRef) error {
	switch ref.Mode {
	case "file":
		abs := filepath.Join(s.deps.DataDir, filepath.FromSlash(ref.Path))
		err := os.Remove(abs)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case "pack":
		return nil
	default:
		return ErrInvalidRef
	}
}

// RemoveBucketTree wipes the bucket's on-disk data after a DeleteBucket.
// Also closes and forgets any active pack-writer state for the bucket.
func (s *Storage) RemoveBucketTree(bucket string) error {
	s.packsMu.Lock()
	if st, ok := s.packs[bucket]; ok {
		st.mu.Lock()
		if st.activeFile != nil {
			_ = st.activeFile.Close()
		}
		st.activeFile = nil
		st.activeID = 0
		st.activeSize = 0
		st.mu.Unlock()
		delete(s.packs, bucket)
	}
	s.packsMu.Unlock()
	return os.RemoveAll(filepath.Join(s.deps.DataDir, bucket))
}

// Close releases all active pack file handles. Call on server shutdown.
func (s *Storage) Close() error {
	s.packsMu.Lock()
	defer s.packsMu.Unlock()
	for _, st := range s.packs {
		st.mu.Lock()
		if st.activeFile != nil {
			_ = st.activeFile.Sync()
			_ = st.activeFile.Close()
			st.activeFile = nil
		}
		st.mu.Unlock()
	}
	return nil
}

type limitedReadCloser struct {
	io.Reader
	close func() error
}

func (l *limitedReadCloser) Close() error { return l.close() }

// avoid unused-import warning if a helper is dropped later
var _ = hex.EncodeToString
var _ = rand.Read
