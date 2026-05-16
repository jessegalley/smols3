package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

const Version = "0.1.0"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("smols3 %s (%s/%s, %s)\n", Version, runtime.GOOS, runtime.GOARCH, runtime.Version())
		},
	}
}
