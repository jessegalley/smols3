// Package compact reclaims dead space in concat-mode pack files. It is intended
// to run offline (server stopped). For every pack whose live_bytes / size ratio
// is below the configured threshold, it copies all live objects into a fresh
// pack and rewrites their ObjectRecord offsets, then removes the old pack file.
package compact

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/jessegalley/smols3/internal/index"
	"github.com/jessegalley/smols3/internal/storage"
)

type Options struct {
	DataDir        string
	DB             *index.DB
	BucketFilter   string  // "" = all
	LiveBytesRatio float64 // packs below this ratio are compacted
}

type Report struct {
	Compacted      int
	Kept           int
	BytesReclaimed int64
}

func Run(opt Options) (Report, error) {
	if opt.LiveBytesRatio <= 0 {
		opt.LiveBytesRatio = 0.5
	}
	var rep Report

	buckets, err := opt.DB.ListBuckets()
	if err != nil {
		return rep, err
	}
	for _, b := range buckets {
		if opt.BucketFilter != "" && opt.BucketFilter != b.Name {
			continue
		}
		if err := compactBucket(opt, b.Name, &rep); err != nil {
			return rep, fmt.Errorf("bucket %s: %w", b.Name, err)
		}
	}
	return rep, nil
}

func compactBucket(opt Options, bucket string, rep *Report) error {
	var candidates []index.PackFileRecord
	err := opt.DB.IterPacks(bucket, func(p index.PackFileRecord) error {
		if !p.Sealed || p.Size == 0 {
			return nil
		}
		ratio := float64(p.LiveBytes) / float64(p.Size)
		if ratio >= opt.LiveBytesRatio {
			rep.Kept++
			return nil
		}
		candidates = append(candidates, p)
		return nil
	})
	if err != nil {
		return err
	}
	for _, old := range candidates {
		reclaimed, err := compactPack(opt, bucket, old)
		if err != nil {
			return err
		}
		rep.Compacted++
		rep.BytesReclaimed += reclaimed
	}
	return nil
}

func compactPack(opt Options, bucket string, old index.PackFileRecord) (int64, error) {
	// Collect live object refs for this pack.
	var refs []index.ObjectRecord
	err := opt.DB.Bolt().View(func(tx *bolt.Tx) error {
		return index.IterPackObjectsTx(tx, bucket, old.PackID, func(rec index.ObjectRecord) error {
			refs = append(refs, rec)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}

	// Allocate a new pack id and on-disk file.
	var newID uint64
	err = opt.DB.Bolt().Update(func(tx *bolt.Tx) error {
		id, err := index.NextPackIDTx(tx, bucket)
		if err != nil {
			return err
		}
		newID = id
		return index.PutPackTx(tx, bucket, index.PackFileRecord{
			Schema:    index.RecordSchema,
			PackID:    id,
			Path:      storage.PackFileRelPath(bucket, id),
			Size:      0,
			Sealed:    false,
			LiveBytes: 0,
			CreatedAt: time.Now().UnixNano(),
		})
	})
	if err != nil {
		return 0, err
	}

	newPath := storage.PackFilePath(opt.DataDir, bucket, newID)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		return 0, err
	}
	newF, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, fmt.Errorf("create new pack: %w", err)
	}

	oldPath := filepath.Join(opt.DataDir, filepath.FromSlash(old.Path))
	oldF, err := os.Open(oldPath)
	if err != nil {
		_ = newF.Close()
		return 0, fmt.Errorf("open old pack: %w", err)
	}
	defer oldF.Close()

	var offset int64
	for _, rec := range refs {
		if _, err := oldF.Seek(rec.Storage.Offset, io.SeekStart); err != nil {
			_ = newF.Close()
			return 0, err
		}
		if _, err := io.CopyN(&offsetWriter{f: newF, off: offset}, oldF, rec.Storage.Length); err != nil {
			_ = newF.Close()
			return 0, err
		}
		newRec := rec
		newRec.Storage.PackID = newID
		newRec.Storage.Path = storage.PackFileRelPath(bucket, newID)
		newRec.Storage.Offset = offset

		err := opt.DB.Bolt().Update(func(tx *bolt.Tx) error {
			pack, err := index.GetPackTx(tx, bucket, newID)
			if err != nil {
				return err
			}
			pack.Size = offset + rec.Storage.Length
			pack.LiveBytes += rec.Storage.Length
			if err := index.PutPackTx(tx, bucket, pack); err != nil {
				return err
			}
			return index.PutObjectTx(tx, bucket, newRec)
		})
		if err != nil {
			_ = newF.Close()
			return 0, err
		}
		offset += rec.Storage.Length
	}
	if err := newF.Sync(); err != nil {
		_ = newF.Close()
		return 0, err
	}
	if err := newF.Close(); err != nil {
		return 0, err
	}

	// Remove old pack record and file.
	err = opt.DB.Bolt().Update(func(tx *bolt.Tx) error {
		return index.DeletePackTx(tx, bucket, old.PackID)
	})
	if err != nil {
		return 0, err
	}
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	return old.Size - offset, nil
}

type offsetWriter struct {
	f   *os.File
	off int64
}

func (o *offsetWriter) Write(p []byte) (int, error) {
	n, err := o.f.WriteAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// avoid unused-import warning
var _ = bolt.ErrBucketNotFound
