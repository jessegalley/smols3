// Package cli wires up the smols3 cobra command tree.
package cli

import (
	"github.com/spf13/cobra"
)

var (
	configPath string
)

func NewRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "smols3",
		Short: "Small S3-compatible test server",
		Long:  "smols3 is a single-binary S3-compatible test server that persists objects to the local filesystem with two storage modes: 1:1 (file) and concat (small-object packing).",
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "Path to TOML configuration file (optional; built-in defaults used if unset)")

	root.AddCommand(newServeCmd())
	root.AddCommand(newInitCmd())
	root.AddCommand(newVersionCmd())
	root.AddCommand(newCompactCmd())
	root.AddCommand(newFsckCmd())
	return root
}
