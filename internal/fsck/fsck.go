// Package fsck verifies on-disk state against the index, and optionally repairs
// known-safe inconsistencies (pack file tails beyond recorded Size).
package fsck

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jessegalley/smols3/internal/index"
)

type Options struct {
	DataDir string
	DB      *index.DB
	Repair  bool
}

type Report struct {
	Objects         int
	MissingFiles    int
	OrphanPackBytes int64
	TruncatedPacks  int
}

func Run(opt Options) (Report, error) {
	var rep Report

	// 1. Verify every ObjectRecord references reachable bytes.
	err := opt.DB.IterAllObjects(func(_ string, rec index.ObjectRecord) error {
		rep.Objects++
		abs := filepath.Join(opt.DataDir, filepath.FromSlash(rec.Storage.Path))
		st, err := os.Stat(abs)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				rep.MissingFiles++
				return nil
			}
			return err
		}
		// For pack-mode refs verify offset+length <= file size.
		if rec.Storage.Mode == "pack" {
			if rec.Storage.Offset+rec.Storage.Length > st.Size() {
				rep.MissingFiles++
			}
		} else {
			if rec.Storage.Length != st.Size() {
				// file-mode size mismatch is also a missing-file indicator
				rep.MissingFiles++
			}
		}
		return nil
	})
	if err != nil {
		return rep, err
	}

	// 2. For every pack, compare on-disk size to recorded Size. Truncate tails if --repair.
	buckets, err := opt.DB.ListBuckets()
	if err != nil {
		return rep, err
	}
	for _, b := range buckets {
		err := opt.DB.IterPacks(b.Name, func(p index.PackFileRecord) error {
			abs := filepath.Join(opt.DataDir, filepath.FromSlash(p.Path))
			st, err := os.Stat(abs)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
			if st.Size() > p.Size {
				orphan := st.Size() - p.Size
				rep.OrphanPackBytes += orphan
				if opt.Repair {
					if err := os.Truncate(abs, p.Size); err != nil {
						return fmt.Errorf("truncate %s: %w", abs, err)
					}
					rep.TruncatedPacks++
				}
			}
			return nil
		})
		if err != nil {
			return rep, err
		}
	}
	return rep, nil
}
