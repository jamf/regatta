// Copyright JAMF Software, LLC

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

var (
	docsDest string
	linkBase string
)

const frontMatterTemplate = `---
title: %s
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
`

func init() {
	docsCmd.PersistentFlags().StringVar(&docsDest, "destination", "docs", "Destination folder where CLI docs should be generated.")
	docsCmd.PersistentFlags().StringVar(&linkBase, "link-base", "", "Base path for generated links.")
}

func linkHandler(filename string) string {
	file := filepath.Base(filename)
	command := strings.Split(file, ".")[0]
	return filepath.Join(linkBase, command)
}

func frontMatter(filename string) string {
	base := filepath.Base(filename)
	base = base[:len(base)-len(filepath.Ext(base))]
	command := strings.Join(strings.Split(base, "_"), " ")

	return fmt.Sprintf(frontMatterTemplate, command)
}

var docsCmd = &cobra.Command{
	Use:                "docs",
	Short:              "Generate Regatta CLI documentation.",
	Hidden:             true,
	DisableFlagParsing: false,
	RunE: func(cmd *cobra.Command, _ []string) error {
		// #nosec G301
		err := os.MkdirAll(docsDest, 0o777)
		if err != nil {
			return err
		}

		err = doc.GenMarkdownTreeCustom(rootCmd, docsDest, frontMatter, linkHandler)
		if err != nil {
			return err
		}

		fmt.Printf("docs generated in '%s'\n", docsDest)
		return nil
	},
}
