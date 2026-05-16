package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/jessegalley/smols3/internal/compact"
	"github.com/jessegalley/smols3/internal/index"
)

func newCompactCmd() *cobra.Command {
	var bucket string
	var threshold float64
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Reclaim space in concat-mode pack files (offline; server must be stopped)",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadConfigOrDefaults(configPath)
			if err != nil {
				return err
			}
			db, err := index.Open(cfg.Storage.IndexPath)
			if err != nil {
				return err
			}
			defer db.Close()
			report, err := compact.Run(compact.Options{
				DataDir:        cfg.Storage.DataDir,
				DB:             db,
				BucketFilter:   bucket,
				LiveBytesRatio: threshold,
			})
			if err != nil {
				return err
			}
			fmt.Printf("packs_compacted=%d packs_kept=%d bytes_reclaimed=%d\n",
				report.Compacted, report.Kept, report.BytesReclaimed)
			return nil
		},
	}
	cmd.Flags().StringVar(&bucket, "bucket", "", "Only compact this bucket (default: all)")
	cmd.Flags().Float64Var(&threshold, "threshold", 0.5, "Compact packs with live_bytes/size below this ratio")
	return cmd
}
