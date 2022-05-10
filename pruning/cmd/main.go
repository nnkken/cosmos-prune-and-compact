package main

import (
	"os"
	"path"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/likecoin/cosmos-prune-and-compact/pruning"
)

var cmdPruneAndCompact = cobra.Command{
	Use:  "prune-and-compact path/to/.liked keep-blocks",
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		dbPath := path.Join(args[0], "data")
		keepBlocks, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			panic(err)
		}
		if keepBlocks <= 0 {
			panic("keep blocks must be greater than 0")
		}
		pruning.Prune(dbPath, keepBlocks)
		pruning.Compact(dbPath)
	},
}

var cmdCompactOnly = cobra.Command{
	Use:  "compact-only path/to/.liked",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbPath := path.Join(args[0], "data")
		pruning.Compact(dbPath)
	},
}

func main() {
	var rootCmd = cobra.Command{}
	rootCmd.AddCommand(&cmdPruneAndCompact, &cmdCompactOnly)
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
