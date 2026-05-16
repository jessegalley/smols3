package storage

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jessegalley/smols3/internal/index"
)

func newPackStorage(t *testing.T) (Storage, *index.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := index.Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	st := NewPackStorage(PackStorageDeps{
		DataDir:               dir,
		ShardDepth:            2,
		MaxObjSize:            10 * 1024 * 1024,
		MaxConcatSize:         1 * 1024 * 1024, // 1 MiB pack
		MaxPackableObjectSize: 64 * 1024,       // 64 KiB packable cap
		Fsync:                 false,
		DB:                    db,
	})
	return st, db, dir
}

func TestPackPutGet(t *testing.T) {
	st, _, _ := newPackStorage(t)
	body := []byte("hello concat\n")
	res, err := st.Put("b", "k1", int64(len(body)), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if res.Ref.Mode != "pack" {
		t.Fatalf("small object should be in pack, got mode=%q", res.Ref.Mode)
	}
	rc, err := st.Open(res.Ref)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, body) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestPackLargeFallback(t *testing.T) {
	st, _, _ := newPackStorage(t)
	// 128 KiB > max_packable (64 KiB) → should be 1:1 file
	body := make([]byte, 128*1024)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	res, err := st.Put("b", "big", int64(len(body)), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if res.Ref.Mode != "file" {
		t.Fatalf("large object should be 1:1 file, got mode=%q", res.Ref.Mode)
	}
}

func TestPackConcurrentNonOverlapping(t *testing.T) {
	st, db, _ := newPackStorage(t)
	const N = 50
	const sz = 1024

	bodies := make([][]byte, N)
	refs := make([]index.StorageRef, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			b := make([]byte, sz)
			if _, err := rand.Read(b); err != nil {
				t.Error(err)
				return
			}
			bodies[i] = b
			res, err := st.Put("b", "k", int64(sz), bytes.NewReader(b))
			if err != nil {
				t.Error(err)
				return
			}
			refs[i] = res.Ref
		}()
	}
	wg.Wait()

	// Verify all offsets are non-overlapping and reads return the right bytes.
	for i := 0; i < N; i++ {
		rc, err := st.Open(refs[i])
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		if md5.Sum(got) != md5.Sum(bodies[i]) {
			t.Fatalf("body mismatch at %d (offset %d, len %d)", i, refs[i].Offset, refs[i].Length)
		}
	}
	// Spot-check: verify offsets don't overlap WITHIN a single pack.
	byPack := make(map[uint64][]index.StorageRef)
	for _, r := range refs {
		byPack[r.PackID] = append(byPack[r.PackID], r)
	}
	for pid, group := range byPack {
		used := make(map[int64]bool)
		for _, r := range group {
			for off := r.Offset; off < r.Offset+r.Length; off++ {
				if used[off] {
					t.Fatalf("pack %d offset %d used twice", pid, off)
				}
				used[off] = true
			}
		}
	}
	_ = db
}

func TestPackRotation(t *testing.T) {
	st, db, _ := newPackStorage(t)
	// Pack cap is 1 MiB; write 20 * 64 KiB = 1.25 MiB → should rotate at least once.
	body := make([]byte, 64*1024)
	for i := 0; i < 20; i++ {
		rand.Read(body)
		_, err := st.Put("b", "k", int64(len(body)), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Count distinct pack records.
	count := 0
	if err := db.IterPacks("b", func(_ index.PackFileRecord) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count < 2 {
		t.Fatalf("expected at least 2 pack files after rotation, got %d", count)
	}
}

func TestPackRangeRead(t *testing.T) {
	st, _, _ := newPackStorage(t)
	body := []byte("0123456789ABCDEFGHIJ")
	res, err := st.Put("b", "k", int64(len(body)), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := st.OpenRange(res.Ref, 4, 6)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "456789" {
		t.Fatalf("range read: got %q want \"456789\"", got)
	}
}

// avoid unused import warning if hex isn't used
var _ = hex.EncodeToString
