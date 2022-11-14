// Copyright JAMF Software, LLC

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var Version = "UNKNOWN"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current version.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Copyright JAMF Software, LLC\n\nVersion: %s\n", Version)
	},
}
