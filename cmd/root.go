package cmd

import (
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
)

func init() {
	encoding.RegisterCodec(vtgrpc.Codec{})

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
