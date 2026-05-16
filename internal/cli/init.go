package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/jessegalley/smols3/internal/config"
	"github.com/jessegalley/smols3/internal/index"
)

func newInitCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create config file, data directory, and an empty index",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg := config.Defaults()
			if dataDir != "" {
				cfg.Storage.DataDir = dataDir
				cfg.Storage.IndexPath = filepath.Join(dataDir, "index.db")
			}
			if cfg.Storage.IndexPath == "" {
				cfg.Storage.IndexPath = filepath.Join(cfg.Storage.DataDir, "index.db")
			}

			// Default config path for init if unset on the root flag.
			target := configPath
			if target == "" {
				target = "smols3.toml"
			}

			if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
				return fmt.Errorf("create data_dir: %w", err)
			}

			// Write the TOML config (don't overwrite if exists).
			if _, err := os.Stat(target); err == nil {
				fmt.Fprintf(os.Stderr, "config %s already exists, skipping\n", target)
			} else if errors.Is(err, os.ErrNotExist) {
				body, err := config.EncodeDefault()
				if err != nil {
					return err
				}
				if dataDir != "" {
					body, err = injectDataDir(body, dataDir)
					if err != nil {
						return err
					}
				}
				if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
					return fmt.Errorf("create config parent: %w", err)
				}
				if err := os.WriteFile(target, body, 0o644); err != nil {
					return fmt.Errorf("write config: %w", err)
				}
				fmt.Fprintf(os.Stderr, "wrote %s\n", target)
			}

			db, err := index.Open(cfg.Storage.IndexPath)
			if err != nil {
				return fmt.Errorf("open index: %w", err)
			}
			defer db.Close()
			id, _ := db.ServerID()
			fmt.Fprintf(os.Stderr, "initialized index %s (server_id=%s)\n", cfg.Storage.IndexPath, id)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Override storage.data_dir (also updates index_path)")
	return cmd
}

// injectDataDir is a quick textual override: rewrite data_dir and index_path
// lines in a generated config so the file matches the flag-provided path.
func injectDataDir(body []byte, dir string) ([]byte, error) {
	out := make([]byte, 0, len(body)+128)
	lines := splitLines(body)
	for _, ln := range lines {
		switch {
		case startsWith(ln, "data_dir"):
			out = append(out, []byte("data_dir                 = "+quote(dir))...)
		case startsWith(ln, "index_path"):
			out = append(out, []byte("index_path               = "+quote(filepath.Join(dir, "index.db")))...)
		default:
			out = append(out, ln...)
		}
		out = append(out, '\n')
	}
	return out, nil
}

func splitLines(b []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			lines = append(lines, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		lines = append(lines, b[start:])
	}
	return lines
}

func startsWith(line []byte, s string) bool {
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	if i+len(s) > len(line) {
		return false
	}
	return string(line[i:i+len(s)]) == s
}

func quote(s string) string { return `"` + s + `"` }
