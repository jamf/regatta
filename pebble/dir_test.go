// Copyright JAMF Software, LLC

package pebble

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestNodeDataDirUtils(t *testing.T) {
	r := require.New(t)
	fs := vfs.NewMem()
	dbDir := "/var/data"

	r.True(IsNewRun(fs, dbDir), "empty dir must be marked as new")
	r.NoError(CreateNodeDataDir(fs, dbDir))

	randomDBDirName := GetNewRandomDBDirName()
	r.NoError(SaveCurrentDBDirName(fs, dbDir, randomDBDirName))
	_, err := GetCurrentDBDirName(fs, dbDir)
	r.Error(err, "current tag should not be stored yet")

	r.NoError(CreateNodeDataDir(fs, filepath.Join(dbDir, randomDBDirName)))

	r.NoError(ReplaceCurrentDBFile(fs, dbDir))
	curr, err := GetCurrentDBDirName(fs, dbDir)
	r.NoError(err)
	_, err = fs.OpenDir(filepath.Join(dbDir, curr))
	r.NoError(err, curr, "random dir should exist and current tag should point to it")

	r.NoError(CleanupNodeDataDir(fs, dbDir))
}
