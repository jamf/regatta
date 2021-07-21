package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

var docsDest string

func init() {
	docsCmd.PersistentFlags().StringVar(&docsDest, "destination", "docs", "Destination folder where docs should be generated.")
}

var docsCmd = &cobra.Command{
	Use:                "docs",
	Short:              "Generate docs",
	Hidden:             true,
	DisableFlagParsing: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		err := os.MkdirAll(docsDest, 0777)
		if err != nil {
			return err
		}
		err = doc.GenMarkdownTree(rootCmd, docsDest)
		if err != nil {
			return err
		}
		fmt.Printf("docs generated in '%s'\n", docsDest)
		return nil
	},
}
