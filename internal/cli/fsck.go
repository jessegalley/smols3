package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/jessegalley/smols3/internal/fsck"
	"github.com/jessegalley/smols3/internal/index"
)

func newFsckCmd() *cobra.Command {
	var repair bool
	cmd := &cobra.Command{
		Use:   "fsck",
		Short: "Verify index against on-disk state",
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
			report, err := fsck.Run(fsck.Options{
				DataDir: cfg.Storage.DataDir,
				DB:      db,
				Repair:  repair,
			})
			if err != nil {
				return err
			}
			fmt.Printf("objects=%d missing_files=%d orphan_pack_bytes=%d truncated_packs=%d\n",
				report.Objects, report.MissingFiles, report.OrphanPackBytes, report.TruncatedPacks)
			return nil
		},
	}
	cmd.Flags().BoolVar(&repair, "repair", false, "Truncate orphan pack-file tails past recorded size")
	return cmd
}
