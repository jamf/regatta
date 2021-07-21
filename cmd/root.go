package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	// Add subcommands
	rootCmd.AddCommand(leaderCmd)
	rootCmd.AddCommand(followerCmd)
	rootCmd.AddCommand(docsCmd)
}

var rootCmd = &cobra.Command{
	Use:                "regatta",
	Short:              "Regatta is read-optimized distributed key-value store.",
	Hidden:             true,
	SuggestFor:         []string{leaderCmd.Use, followerCmd.Use},
	DisableFlagParsing: true,
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
