// Copyright JAMF Software, LLC

package cmd

import (
	"fmt"
	"os"

	"github.com/jamf/regatta/regattaserver"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
)

func init() {
	encoding.RegisterCodec(regattaserver.Codec{})

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
	Short: "Regatta is read-optimized distributed key-value store.",
	Long: `Regatta can be run in two modes Leader and Follower, in the Leader mode write API is enabled 
and the node (or cluster of leader nodes) acts as a source of truth for the Follower nodes/clusters. In the Follower mode 
write API is disabled and the node or cluster of nodes replicates the writes done to the Leader cluster to which the
Follower one is connected to.`,
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
