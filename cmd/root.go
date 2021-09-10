package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wandera/regatta/regattaserver"
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
}

var rootCmd = &cobra.Command{
	Use:                "regatta",
	Short:              "Regatta is read-optimized distributed key-value store.",
	Hidden:             true,
	SuggestFor:         []string{leaderCmd.Use, followerCmd.Use},
	DisableFlagParsing: true,
	DisableAutoGenTag:  true,
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
