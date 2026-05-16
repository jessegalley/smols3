package storage

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// BucketDir returns the absolute directory path for a bucket.
func BucketDir(dataDir, bucket string) string {
	return filepath.Join(dataDir, bucket)
}

// PackDir returns the absolute directory path holding pack files for a bucket.
func PackDir(dataDir, bucket string) string {
	return filepath.Join(dataDir, bucket, ".packs")
}

// PackFilePath returns the absolute path of a pack file given its 16-hex id.
func PackFilePath(dataDir, bucket string, packID uint64) string {
	return filepath.Join(PackDir(dataDir, bucket), fmt.Sprintf("%016x.pack", packID))
}

// PackFileRelPath returns the data-dir-relative path of a pack file.
func PackFileRelPath(bucket string, packID uint64) string {
	return path.Join(bucket, ".packs", fmt.Sprintf("%016x.pack", packID))
}

// StagingDir returns the absolute directory holding in-progress multipart parts.
func StagingDir(dataDir, bucket, uploadID string) string {
	return filepath.Join(dataDir, bucket, ".uploads", uploadID)
}

// StagingPartRelPath returns the data-dir-relative path of a multipart part.
func StagingPartRelPath(bucket, uploadID string, partNum int) string {
	return path.Join(bucket, ".uploads", uploadID, fmt.Sprintf("%05d", partNum))
}

// ObjectFilePath returns the absolute path of a 1:1-mode object file.
// shardDepth controls how many 2-hex-char directories are prepended (from a hash of the key)
// so that no single directory holds too many files. 0 disables sharding (key used verbatim
// under the bucket dir, replacing path separators).
func ObjectFilePath(dataDir, bucket, key string, shardDepth int) string {
	rel := ObjectFileRelPath(bucket, key, shardDepth)
	return filepath.Join(dataDir, filepath.FromSlash(rel))
}

// ObjectFileRelPath returns the data-dir-relative path of a 1:1-mode object file.
// The key is hashed into shard directories (e.g. ab/cd/<safe-key>) so listing
// behavior remains independent of disk layout.
func ObjectFileRelPath(bucket, key string, shardDepth int) string {
	if shardDepth < 0 {
		shardDepth = 0
	}
	if shardDepth > 8 {
		shardDepth = 8
	}
	sum := sha1.Sum([]byte(key))
	hexed := hex.EncodeToString(sum[:])
	parts := []string{bucket}
	for i := 0; i < shardDepth; i++ {
		parts = append(parts, hexed[i*2:i*2+2])
	}
	parts = append(parts, hexed+"_"+sanitizeKey(key))
	return path.Join(parts...)
}

// sanitizeKey converts an S3 object key into a filesystem-safe leaf component.
// We rely on the hash prefix for uniqueness — the suffix is only present as a human-readable hint.
func sanitizeKey(k string) string {
	var b strings.Builder
	for _, r := range k {
		switch {
		case r == '/' || r == '\\':
			b.WriteByte('_')
		case r < 0x20 || r == 0x7f:
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := b.String()
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}
