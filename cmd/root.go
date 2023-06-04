// Copyright JAMF Software, LLC

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	// Add subcommands
	rootCmd.AddCommand(leaderCmd)
	rootCmd.AddCommand(followerCmd)
	rootCmd.AddCommand(docsCmd)
	rootCmd.AddCommand(generatorCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(versionCmd)
}

var rootCmd = &cobra.Command{
	Use:   "regatta",
	Short: "Regatta is a read-optimized distributed key-value store.",
	Long: `Regatta can be run in two modes -- leader and follower. Write API is enabled in the leader mode
and the node (or cluster of leader nodes) acts as a source of truth for the follower nodes/clusters.
Write API is disabled in the follower mode and the follower node or cluster of follower nodes replicate the writes
done to the leader cluster to which the follower is connected to.`,
	Hidden:             true,
	SuggestFor:         []string{leaderCmd.Use, followerCmd.Use},
	DisableFlagParsing: true,
	DisableAutoGenTag:  true,
	CompletionOptions:  cobra.CompletionOptions{DisableDefaultCmd: true},
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
