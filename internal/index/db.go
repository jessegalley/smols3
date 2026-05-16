package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

type DB struct {
	bolt *bolt.DB
	path string
}

// Open opens or creates the index database at path. Parent directories must exist.
func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create index parent dir: %w", err)
	}
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:      5 * time.Second,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("open bolt %s: %w", path, err)
	}
	d := &DB{bolt: bdb, path: path}
	if err := d.init(); err != nil {
		_ = bdb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error {
	if d.bolt == nil {
		return nil
	}
	err := d.bolt.Close()
	d.bolt = nil
	return err
}

func (d *DB) Path() string { return d.path }

// Bolt exposes the underlying bbolt DB for advanced ops (compaction, fsck).
func (d *DB) Bolt() *bolt.DB { return d.bolt }

func (d *DB) init() error {
	return d.bolt.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bktMeta, bktBuckets, bktObjects, bktMultipart, bktPacks, bktPacksActive} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		mb := tx.Bucket(bktMeta)
		if mb.Get(metaSchemaVersion) == nil {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, SchemaVersion)
			if err := mb.Put(metaSchemaVersion, buf); err != nil {
				return err
			}
		} else {
			cur := binary.BigEndian.Uint32(mb.Get(metaSchemaVersion))
			if cur != SchemaVersion {
				return fmt.Errorf("schema version mismatch: db=%d binary=%d", cur, SchemaVersion)
			}
		}
		if mb.Get(metaServerID) == nil {
			id := uuid.NewString()
			if err := mb.Put(metaServerID, []byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ServerID returns the persistent server UUID.
func (d *DB) ServerID() (string, error) {
	var id string
	err := d.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktMeta).Get(metaServerID)
		if b == nil {
			return errors.New("server_id not set")
		}
		id = string(b)
		return nil
	})
	return id, err
}

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func beu64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
