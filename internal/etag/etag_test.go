package etag

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

func TestStreamHasher(t *testing.T) {
	body := []byte("hello world\n")
	h := NewStreamHasher()
	h.Write(body)
	want := md5.Sum(body)
	if h.SumHex() != hex.EncodeToString(want[:]) {
		t.Fatalf("got %s want %s", h.SumHex(), hex.EncodeToString(want[:]))
	}
	if h.Size() != int64(len(body)) {
		t.Fatalf("size: got %d want %d", h.Size(), len(body))
	}
}

func TestTeeReader(t *testing.T) {
	body := "the quick brown fox"
	r, h := TeeReader(strings.NewReader(body))
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != body {
		t.Fatalf("body roundtrip mismatch")
	}
	want := md5.Sum([]byte(body))
	if h.SumHex() != hex.EncodeToString(want[:]) {
		t.Fatalf("tee hash mismatch")
	}
}

func TestMultipartETag(t *testing.T) {
	// Two 5 MiB parts of 'a' and 'b'; check the form is <md5ofmd5s>-2.
	partA := bytes.Repeat([]byte{'a'}, 5*1024*1024)
	partB := bytes.Repeat([]byte{'b'}, 5*1024*1024)
	mA := md5.Sum(partA)
	mB := md5.Sum(partB)
	combined := append(append([]byte{}, mA[:]...), mB[:]...)
	want := md5.Sum(combined)
	wantStr := hex.EncodeToString(want[:]) + "-2"

	got, err := MultipartETag([]string{hex.EncodeToString(mA[:]), hex.EncodeToString(mB[:])})
	if err != nil {
		t.Fatal(err)
	}
	if got != wantStr {
		t.Fatalf("multipart etag: got %s want %s", got, wantStr)
	}
}

func TestMultipartETagBadHex(t *testing.T) {
	if _, err := MultipartETag([]string{"zzzz"}); err == nil {
		t.Fatal("expected error on bad hex")
	}
}
