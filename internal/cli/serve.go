package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/jessegalley/smols3/internal/config"
	"github.com/jessegalley/smols3/internal/index"
	"github.com/jessegalley/smols3/internal/s3api"
	"github.com/jessegalley/smols3/internal/storage"
)

// serveFlags backs all the per-knob CLI flags on `serve`. Each is applied to
// the loaded config only if its flag was explicitly set (cobra Changed()).
type serveFlags struct {
	listen          string
	region          string
	readTimeout     time.Duration
	writeTimeout    time.Duration
	idleTimeout     time.Duration
	maxRequestSize  string

	dataDir               string
	indexPath             string
	mode                  string
	maxObjectSize         string
	maxConcatSize         string
	maxPackableObjectSize string
	fsyncData             bool
	shardDirDepth         int

	authMode  string
	accessKey string
	secretKey string

	logLevel  string
	logFormat string
	accessLog bool

	maxMultipartParts int
	minPartSize       string
}

func newServeCmd() *cobra.Command {
	var f serveFlags
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the S3-compatible HTTP server",
		Long:  "Run the server. If --config is omitted, built-in defaults are used. Individual settings can be overridden with the flags below.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfigOrDefaults(configPath)
			if err != nil {
				return err
			}
			if err := applyServeFlags(cmd, &f, &cfg); err != nil {
				return err
			}
			if cfg.Storage.IndexPath == "" {
				cfg.Storage.IndexPath = filepath.Join(cfg.Storage.DataDir, "index.db")
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
				return fmt.Errorf("create data_dir: %w", err)
			}

			db, err := index.Open(cfg.Storage.IndexPath)
			if err != nil {
				return err
			}
			defer db.Close()

			logger := buildLogger(cfg.Log)
			printBanner(os.Stderr, cfg)

			var st storage.Storage
			if cfg.Storage.Mode == "concat" {
				st = storage.NewPackStorage(storage.PackStorageDeps{
					DataDir:               cfg.Storage.DataDir,
					ShardDepth:            cfg.Storage.ShardDirDepth,
					MaxObjSize:            cfg.Storage.MaxObjectSize.I(),
					MaxConcatSize:         cfg.Storage.MaxConcatSize.I(),
					MaxPackableObjectSize: cfg.Storage.MaxPackableObjectSize.I(),
					Fsync:                 cfg.Storage.FsyncData,
					DB:                    db,
				})
			} else {
				st = storage.NewFileStorage(
					cfg.Storage.DataDir,
					cfg.Storage.ShardDirDepth,
					cfg.Storage.MaxObjectSize.I(),
					cfg.Storage.FsyncData,
				)
			}

			srv := &s3api.Server{Cfg: cfg, DB: db, Storage: st, Logger: logger}
			httpSrv := &http.Server{
				Addr:         cfg.Server.Listen,
				Handler:      srv.Router(),
				ReadTimeout:  cfg.Server.ReadTimeout.D(),
				WriteTimeout: cfg.Server.WriteTimeout.D(),
				IdleTimeout:  cfg.Server.IdleTimeout.D(),
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				err := httpSrv.ListenAndServe()
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					errCh <- err
				}
				close(errCh)
			}()

			select {
			case <-ctx.Done():
				logger.Info("shutting down")
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer shutCancel()
				if err := httpSrv.Shutdown(shutCtx); err != nil {
					logger.Warn("shutdown", "err", err)
				}
				if closer, ok := st.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			case err := <-errCh:
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	bindServeFlags(cmd, &f)
	return cmd
}

func bindServeFlags(cmd *cobra.Command, f *serveFlags) {
	fl := cmd.Flags()

	// server
	fl.StringVar(&f.listen, "listen", "", "HTTP listen address (host:port)")
	fl.StringVar(&f.region, "region", "", "S3 region advertised by the server")
	fl.DurationVar(&f.readTimeout, "read-timeout", 0, "HTTP read timeout (e.g. 30s)")
	fl.DurationVar(&f.writeTimeout, "write-timeout", 0, "HTTP write timeout (e.g. 5m)")
	fl.DurationVar(&f.idleTimeout, "idle-timeout", 0, "HTTP idle timeout (e.g. 120s)")
	fl.StringVar(&f.maxRequestSize, "max-request-size", "", "Maximum request body size (e.g. 5GiB)")

	// storage
	fl.StringVar(&f.dataDir, "data-dir", "", "Object storage directory")
	fl.StringVar(&f.indexPath, "index-path", "", "Path to the index database (default: <data-dir>/index.db)")
	fl.StringVar(&f.mode, "mode", "", "Storage mode: file | concat")
	fl.StringVar(&f.maxObjectSize, "max-object-size", "", "Hard per-object ceiling (e.g. 5GiB)")
	fl.StringVar(&f.maxConcatSize, "max-concat-size", "", "Pack-file size cap in concat mode (e.g. 64MiB)")
	fl.StringVar(&f.maxPackableObjectSize, "max-packable-object-size", "", "Per-object eligibility cap for packing (e.g. 1MiB)")
	fl.BoolVar(&f.fsyncData, "fsync-data", true, "Fsync data files before committing index")
	fl.IntVar(&f.shardDirDepth, "shard-dir-depth", 0, "Number of 2-hex shard directories under each bucket (0-8)")

	// auth
	fl.StringVar(&f.authMode, "auth-mode", "", "Authentication mode: sigv4 | none")
	fl.StringVar(&f.accessKey, "access-key", "", "Static SigV4 access key")
	fl.StringVar(&f.secretKey, "secret-key", "", "Static SigV4 secret key")

	// logging
	fl.StringVar(&f.logLevel, "log-level", "", "Log level: debug | info | warn | error")
	fl.StringVar(&f.logFormat, "log-format", "", "Log format: text | json")
	fl.BoolVar(&f.accessLog, "access-log", true, "Emit one log line per HTTP request")

	// limits
	fl.IntVar(&f.maxMultipartParts, "max-multipart-parts", 0, "Maximum number of multipart upload parts (S3 default 10000)")
	fl.StringVar(&f.minPartSize, "min-part-size", "", "Minimum non-final multipart part size (S3 minimum 5MiB)")
}

// applyServeFlags overrides cfg with any flag whose value was explicitly set.
func applyServeFlags(cmd *cobra.Command, f *serveFlags, cfg *config.Config) error {
	changed := func(name string) bool {
		fl := cmd.Flag(name)
		return fl != nil && fl.Changed
	}
	parseSize := func(name, raw string, into *config.ByteSize) error {
		var v config.ByteSize
		if err := v.UnmarshalText([]byte(raw)); err != nil {
			return fmt.Errorf("--%s: %w", name, err)
		}
		*into = v
		return nil
	}

	if changed("listen") {
		cfg.Server.Listen = f.listen
	}
	if changed("region") {
		cfg.Server.Region = f.region
	}
	if changed("read-timeout") {
		cfg.Server.ReadTimeout = config.Duration(f.readTimeout)
	}
	if changed("write-timeout") {
		cfg.Server.WriteTimeout = config.Duration(f.writeTimeout)
	}
	if changed("idle-timeout") {
		cfg.Server.IdleTimeout = config.Duration(f.idleTimeout)
	}
	if changed("max-request-size") {
		if err := parseSize("max-request-size", f.maxRequestSize, &cfg.Server.MaxRequestSize); err != nil {
			return err
		}
	}

	if changed("data-dir") {
		cfg.Storage.DataDir = f.dataDir
		// If the user moved the data dir but didn't set --index-path, default it under the new data dir.
		if !changed("index-path") {
			cfg.Storage.IndexPath = filepath.Join(f.dataDir, "index.db")
		}
	}
	if changed("index-path") {
		cfg.Storage.IndexPath = f.indexPath
	}
	if changed("mode") {
		cfg.Storage.Mode = f.mode
	}
	if changed("max-object-size") {
		if err := parseSize("max-object-size", f.maxObjectSize, &cfg.Storage.MaxObjectSize); err != nil {
			return err
		}
	}
	if changed("max-concat-size") {
		if err := parseSize("max-concat-size", f.maxConcatSize, &cfg.Storage.MaxConcatSize); err != nil {
			return err
		}
	}
	if changed("max-packable-object-size") {
		if err := parseSize("max-packable-object-size", f.maxPackableObjectSize, &cfg.Storage.MaxPackableObjectSize); err != nil {
			return err
		}
	}
	if changed("fsync-data") {
		cfg.Storage.FsyncData = f.fsyncData
	}
	if changed("shard-dir-depth") {
		cfg.Storage.ShardDirDepth = f.shardDirDepth
	}

	if changed("auth-mode") {
		cfg.Auth.Mode = f.authMode
	}
	if changed("access-key") {
		cfg.Auth.AccessKey = f.accessKey
	}
	if changed("secret-key") {
		cfg.Auth.SecretKey = f.secretKey
	}

	if changed("log-level") {
		cfg.Log.Level = f.logLevel
	}
	if changed("log-format") {
		cfg.Log.Format = f.logFormat
	}
	if changed("access-log") {
		cfg.Log.AccessLog = f.accessLog
	}

	if changed("max-multipart-parts") {
		cfg.Limits.MaxMultipartParts = f.maxMultipartParts
	}
	if changed("min-part-size") {
		if err := parseSize("min-part-size", f.minPartSize, &cfg.Limits.MinPartSize); err != nil {
			return err
		}
	}
	return nil
}

// loadConfigOrDefaults returns config.Defaults() when path is empty, otherwise
// loads from path. Empty path is the zero-ceremony case.
func loadConfigOrDefaults(path string) (config.Config, error) {
	if path == "" {
		return config.Defaults(), nil
	}
	return config.Load(path)
}

func printBanner(w *os.File, cfg config.Config) {
	scheme := "http"
	fmt.Fprintf(w, "smols3 %s listening on %s://%s\n", Version, scheme, cfg.Server.Listen)
	fmt.Fprintf(w, "  data_dir:   %s\n", cfg.Storage.DataDir)
	fmt.Fprintf(w, "  index_path: %s\n", cfg.Storage.IndexPath)
	fmt.Fprintf(w, "  mode:       %s\n", cfg.Storage.Mode)
	fmt.Fprintf(w, "  region:     %s\n", cfg.Server.Region)
	switch cfg.Auth.Mode {
	case "sigv4":
		fmt.Fprintf(w, "  auth:       sigv4\n")
		fmt.Fprintf(w, "  access_key: %s\n", cfg.Auth.AccessKey)
		fmt.Fprintf(w, "  secret_key: %s\n", cfg.Auth.SecretKey)
	case "none":
		fmt.Fprintf(w, "  auth:       disabled\n")
	}
}

func buildLogger(c config.LogConfig) *slog.Logger {
	var level slog.Level
	switch c.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if c.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
