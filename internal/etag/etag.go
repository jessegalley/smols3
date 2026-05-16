// Package etag computes S3-compatible ETags. For single-part uploads the ETag
// is the hex-encoded MD5 of the object body. For multipart uploads it is the
// hex-encoded MD5 of the concatenated binary MD5s of each part, suffixed with
// "-N" where N is the part count.
package etag

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

// StreamHasher is an io.Writer that streams bytes into an MD5 hasher and tracks size.
type StreamHasher struct {
	h    hash.Hash
	n    int64
}

func NewStreamHasher() *StreamHasher {
	return &StreamHasher{h: md5.New()}
}

func (s *StreamHasher) Write(p []byte) (int, error) {
	n, err := s.h.Write(p)
	s.n += int64(n)
	return n, err
}

// SumHex returns the hex MD5 of all bytes written so far.
func (s *StreamHasher) SumHex() string {
	return hex.EncodeToString(s.h.Sum(nil))
}

// SumRaw returns the 16-byte raw MD5 of all bytes written so far.
func (s *StreamHasher) SumRaw() []byte {
	return s.h.Sum(nil)
}

func (s *StreamHasher) Size() int64 { return s.n }

// TeeReader returns an io.Reader that wraps r and writes everything read through to a fresh hasher.
// Callers can retrieve the hash via the returned StreamHasher after EOF.
func TeeReader(r io.Reader) (io.Reader, *StreamHasher) {
	h := NewStreamHasher()
	return io.TeeReader(r, h), h
}

// MultipartETag combines per-part MD5s into the S3 multipart ETag form.
// partMD5s is a list of hex-encoded MD5 strings, one per part (in part order).
func MultipartETag(partMD5s []string) (string, error) {
	buf := make([]byte, 0, len(partMD5s)*16)
	for i, hexMD5 := range partMD5s {
		raw, err := hex.DecodeString(hexMD5)
		if err != nil {
			return "", fmt.Errorf("part %d: invalid hex md5 %q: %w", i+1, hexMD5, err)
		}
		if len(raw) != 16 {
			return "", fmt.Errorf("part %d: md5 must be 16 bytes, got %d", i+1, len(raw))
		}
		buf = append(buf, raw...)
	}
	sum := md5.Sum(buf)
	return fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(partMD5s)), nil
}
