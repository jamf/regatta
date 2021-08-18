package cmd

import (
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
	rootCmd.AddCommand(logReaderCmd)
	rootCmd.AddCommand(generatorCmd)
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
		panic(err)
	}
}
