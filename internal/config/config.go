package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	toml "github.com/pelletier/go-toml/v2"
)

type Config struct {
	Server  ServerConfig  `toml:"server"`
	Storage StorageConfig `toml:"storage"`
	Auth    AuthConfig    `toml:"auth"`
	Log     LogConfig     `toml:"log"`
	Limits  LimitsConfig  `toml:"limits"`
}

type ServerConfig struct {
	Listen         string        `toml:"listen"`
	Region         string        `toml:"region"`
	ReadTimeout    Duration      `toml:"read_timeout"`
	WriteTimeout   Duration      `toml:"write_timeout"`
	IdleTimeout    Duration      `toml:"idle_timeout"`
	MaxRequestSize ByteSize      `toml:"max_request_size"`
}

type StorageConfig struct {
	DataDir               string   `toml:"data_dir"`
	IndexPath             string   `toml:"index_path"`
	Mode                  string   `toml:"mode"` // "file" | "concat"
	MaxObjectSize         ByteSize `toml:"max_object_size"`
	MaxConcatSize         ByteSize `toml:"max_concat_size"`
	MaxPackableObjectSize ByteSize `toml:"max_packable_object_size"`
	FsyncData             bool     `toml:"fsync_data"`
	ShardDirDepth         int      `toml:"shard_dir_depth"`
}

type AuthConfig struct {
	Mode      string `toml:"mode"` // "sigv4" | "none"
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
}

type LogConfig struct {
	Level     string `toml:"level"`
	Format    string `toml:"format"`
	AccessLog bool   `toml:"access_log"`
}

type LimitsConfig struct {
	MaxMultipartParts int      `toml:"max_multipart_parts"`
	MinPartSize       ByteSize `toml:"min_part_size"`
}

func Defaults() Config {
	dd := defaultDataDir()
	return Config{
		Server: ServerConfig{
			Listen:         "127.0.0.1:9000",
			Region:         "us-east-1",
			ReadTimeout:    Duration(30 * time.Second),
			WriteTimeout:   Duration(5 * time.Minute),
			IdleTimeout:    Duration(120 * time.Second),
			MaxRequestSize: 5 * 1024 * 1024 * 1024,
		},
		Storage: StorageConfig{
			DataDir:               dd,
			IndexPath:             filepath.Join(dd, "index.db"),
			Mode:                  "file",
			MaxObjectSize:         5 * 1024 * 1024 * 1024,
			MaxConcatSize:         64 * 1024 * 1024,
			MaxPackableObjectSize: 1 * 1024 * 1024,
			FsyncData:             true,
			ShardDirDepth:         2,
		},
		Auth: AuthConfig{
			Mode:      "sigv4",
			AccessKey: "smols3",
			SecretKey: "smols3secret",
		},
		Log: LogConfig{
			Level:     "info",
			Format:    "text",
			AccessLog: true,
		},
		Limits: LimitsConfig{
			MaxMultipartParts: 10000,
			MinPartSize:       5 * 1024 * 1024,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	dec := toml.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Storage.IndexPath == "" {
		cfg.Storage.IndexPath = filepath.Join(cfg.Storage.DataDir, "index.db")
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	switch c.Storage.Mode {
	case "file", "concat":
	default:
		return fmt.Errorf("storage.mode must be \"file\" or \"concat\", got %q", c.Storage.Mode)
	}
	switch c.Auth.Mode {
	case "sigv4", "none":
	default:
		return fmt.Errorf("auth.mode must be \"sigv4\" or \"none\", got %q", c.Auth.Mode)
	}
	if c.Storage.Mode == "concat" {
		if c.Storage.MaxConcatSize <= 0 {
			return fmt.Errorf("storage.max_concat_size must be > 0 in concat mode")
		}
		if c.Storage.MaxPackableObjectSize <= 0 {
			return fmt.Errorf("storage.max_packable_object_size must be > 0 in concat mode")
		}
		if c.Storage.MaxPackableObjectSize > c.Storage.MaxConcatSize {
			return fmt.Errorf("storage.max_packable_object_size (%d) cannot exceed max_concat_size (%d)",
				c.Storage.MaxPackableObjectSize, c.Storage.MaxConcatSize)
		}
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
	}
	if c.Server.Listen == "" {
		return fmt.Errorf("server.listen is required")
	}
	if c.Auth.Mode == "sigv4" && (c.Auth.AccessKey == "" || c.Auth.SecretKey == "") {
		return fmt.Errorf("auth.access_key and auth.secret_key are required in sigv4 mode")
	}
	return nil
}

// EncodeDefault returns a TOML serialization suitable for `smols3 init` output.
func EncodeDefault() ([]byte, error) {
	cfg := Defaults()
	return toml.Marshal(&cfg)
}

// defaultDataDir picks a sensible writable default location:
//  1. $XDG_DATA_HOME/smols3
//  2. ~/.local/share/smols3
//  3. ./smols3-data (cwd-relative fallback)
//
// The first two are only chosen if the path can actually be created.
func defaultDataDir() string {
	candidates := make([]string, 0, 2)
	if x := os.Getenv("XDG_DATA_HOME"); x != "" {
		candidates = append(candidates, filepath.Join(x, "smols3"))
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		candidates = append(candidates, filepath.Join(h, ".local", "share", "smols3"))
	}
	for _, p := range candidates {
		if err := os.MkdirAll(p, 0o755); err == nil {
			return p
		}
	}
	return "smols3-data"
}

// Duration is a time.Duration that decodes from either a TOML string ("30s") or integer nanoseconds.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

// ByteSize accepts an integer (bytes) or a string with optional KiB/MiB/GiB/TiB or KB/MB/GB/TB suffix.
type ByteSize int64

func (b *ByteSize) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		*b = 0
		return nil
	}
	// Integer-only fast path
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*b = ByteSize(n)
		return nil
	}
	// Suffix path
	s = strings.ToUpper(s)
	mults := []struct {
		suf string
		mul int64
	}{
		{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10},
		{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
		{"B", 1},
	}
	for _, m := range mults {
		if strings.HasSuffix(s, m.suf) {
			num := strings.TrimSpace(strings.TrimSuffix(s, m.suf))
			n, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return fmt.Errorf("invalid byte size %q: %w", string(text), err)
			}
			*b = ByteSize(int64(n * float64(m.mul)))
			return nil
		}
	}
	return fmt.Errorf("invalid byte size %q", string(text))
}

func (b ByteSize) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(b), 10)), nil
}

func (b ByteSize) I() int64 { return int64(b) }
