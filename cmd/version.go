// Copyright JAMF Software, LLC

package cmd

import (
	"cmp"
	"fmt"
	"runtime/debug"
	"slices"

	"github.com/jamf/regatta/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current version.",
	Run: func(cmd *cobra.Command, args []string) {
		additional := ""
		info, ok := debug.ReadBuildInfo()
		if ok {
			additional = fmt.Sprintf("Go: %s (%s/%s)\n", info.GoVersion, getBuildSetting(info.Settings, "GOOS"), getBuildSetting(info.Settings, "GOARCH"))
		}
		fmt.Printf("Copyright JAMF Software, LLC\n\nRegatta\nVersion: %s\n%s", version.Version, additional)
	},
}

func getBuildSetting(settings []debug.BuildSetting, name string) string {
	if idx, found := slices.BinarySearchFunc(settings, name, func(setting debug.BuildSetting, s string) int { return cmp.Compare(setting.Key, s) }); found {
		return settings[idx].Value
	}
	return ""
}
