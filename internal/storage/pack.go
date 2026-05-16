package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jessegalley/smols3/internal/index"
	bolt "go.etcd.io/bbolt"
)

// packStorage implements concat mode. Small objects are appended into shared
// pack files; large objects fall back to 1:1 files via an embedded fileStorage.
type packStorage struct {
	dataDir              string
	maxObjSize           int64
	maxConcatSize        int64
	maxPackableObjectSize int64
	fsync                bool
	db                   *index.DB
	file                 *fileStorage // fallback for large objects

	mu      sync.Mutex
	buckets map[string]*bucketPackState
}

type bucketPackState struct {
	mu         sync.Mutex
	activeID   uint64
	activeFile *os.File
	activeSize int64 // logical EOF == next reservable offset; in-memory only until index commit
	inflight   sync.WaitGroup
}

// PackStorageDeps groups the parameters NewPackStorage needs.
type PackStorageDeps struct {
	DataDir               string
	ShardDepth            int
	MaxObjSize            int64
	MaxConcatSize         int64
	MaxPackableObjectSize int64
	Fsync                 bool
	DB                    *index.DB
}

func NewPackStorage(d PackStorageDeps) Storage {
	return &packStorage{
		dataDir:               d.DataDir,
		maxObjSize:            d.MaxObjSize,
		maxConcatSize:         d.MaxConcatSize,
		maxPackableObjectSize: d.MaxPackableObjectSize,
		fsync:                 d.Fsync,
		db:                    d.DB,
		file:                  NewFileStorage(d.DataDir, d.ShardDepth, d.MaxObjSize, d.Fsync).(*fileStorage),
		buckets:               make(map[string]*bucketPackState),
	}
}

// ErrSizeRequired is returned when concat mode receives a stream of unknown length.
var ErrSizeRequired = errors.New("concat mode requires known Content-Length")

func (p *packStorage) stateFor(bucket string) *bucketPackState {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.buckets[bucket]
	if !ok {
		st = &bucketPackState{}
		p.buckets[bucket] = st
	}
	return st
}

func (p *packStorage) Put(bucket, key string, size int64, body io.Reader) (PutResult, error) {
	if body == nil {
		return PutResult{}, ErrNoBody
	}
	if size >= 0 && p.maxObjSize > 0 && size > p.maxObjSize {
		return PutResult{}, ErrTooLarge
	}

	// Large or unknown-size objects always go to 1:1 file mode.
	if size < 0 || size > p.maxPackableObjectSize {
		return p.file.Put(bucket, key, size, body)
	}

	st := p.stateFor(bucket)

	// Phase 1: under the bucket lock, ensure we have an active pack and reserve an offset.
	st.mu.Lock()
	if st.activeFile == nil {
		if err := p.openActiveLocked(bucket, st); err != nil {
			st.mu.Unlock()
			return PutResult{}, err
		}
	}
	if size > p.maxConcatSize-st.activeSize {
		// Drain in-flight writes on current active pack before sealing/rotating.
		st.mu.Unlock()
		st.inflight.Wait()
		st.mu.Lock()
		if err := p.rotateLocked(bucket, st); err != nil {
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

	// Phase 2: stream body to the reserved range using positional writes.
	written, err := writeAt(file, offset, size, body)
	if err != nil {
		st.inflight.Done()
		return PutResult{}, err
	}
	if p.fsync {
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

// openActiveLocked sets st.activeID/activeFile/activeSize from the persisted
// active-pack pointer, or allocates a fresh pack if none exists.
func (p *packStorage) openActiveLocked(bucket string, st *bucketPackState) error {
	var activeID uint64
	var pack index.PackFileRecord
	err := p.db.Bolt().View(func(tx *bolt.Tx) error {
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
		return p.allocateNewActiveLocked(bucket, st)
	}
	// Open the existing active pack.
	abs := filepath.Join(p.dataDir, filepath.FromSlash(pack.Path))
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

func (p *packStorage) allocateNewActiveLocked(bucket string, st *bucketPackState) error {
	var newID uint64
	err := p.db.Bolt().Update(func(tx *bolt.Tx) error {
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
	abs := PackFilePath(p.dataDir, bucket, newID)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		// File already exists from a previous incarnation; open RDWR.
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

// rotateLocked seals the current active pack and opens a new one. Caller must
// have drained inflight writes.
func (p *packStorage) rotateLocked(bucket string, st *bucketPackState) error {
	if st.activeFile != nil {
		if p.fsync {
			_ = st.activeFile.Sync()
		}
		_ = st.activeFile.Close()
	}
	// Mark the old pack sealed in bolt.
	if st.activeID != 0 {
		err := p.db.Bolt().Update(func(tx *bolt.Tx) error {
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
	return p.allocateNewActiveLocked(bucket, st)
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

func (p *packStorage) Open(ref index.StorageRef) (io.ReadCloser, error) {
	switch ref.Mode {
	case "file":
		return p.file.Open(ref)
	case "pack":
		abs := filepath.Join(p.dataDir, filepath.FromSlash(ref.Path))
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

func (p *packStorage) OpenRange(ref index.StorageRef, offset, length int64) (io.ReadCloser, error) {
	switch ref.Mode {
	case "file":
		return p.file.OpenRange(ref, offset, length)
	case "pack":
		abs := filepath.Join(p.dataDir, filepath.FromSlash(ref.Path))
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

// Delete handles only on-disk bytes. For file mode the file is removed; for
// pack mode this is a no-op (bytes leak until compaction). LiveBytes accounting
// is the caller's responsibility — it should be committed in the same bolt tx
// that removes the ObjectRecord.
func (p *packStorage) Delete(ref index.StorageRef) error {
	switch ref.Mode {
	case "file":
		return p.file.Delete(ref)
	case "pack":
		return nil
	default:
		return ErrInvalidRef
	}
}

func (p *packStorage) RemoveBucketTree(bucket string) error {
	// Close active pack handle if open.
	p.mu.Lock()
	st, ok := p.buckets[bucket]
	if ok {
		st.mu.Lock()
		if st.activeFile != nil {
			_ = st.activeFile.Close()
		}
		st.activeFile = nil
		st.activeID = 0
		st.activeSize = 0
		st.mu.Unlock()
		delete(p.buckets, bucket)
	}
	p.mu.Unlock()
	return os.RemoveAll(filepath.Join(p.dataDir, bucket))
}

// Close releases active pack file handles. Call on server shutdown.
func (p *packStorage) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, st := range p.buckets {
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

// Closer is satisfied by packStorage; expose via type assertion for cmd/serve shutdown.
type Closer interface {
	Close() error
}
