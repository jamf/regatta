// Copyright JAMF Software, LLC

package pebble

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

const (
	// currentDBFilename static filename with current Pebble DB directory name.
	currentDBFilename string = "current"
	// updatingDBFilename static filename with recovering Pebble DB directory name (before the switch).
	updatingDBFilename string = "current.updating"
)

var ErrIsNotDir = errors.New("is not a dir")

func syncDir(fs vfs.FS, dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := fs.Stat(dir)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return ErrIsNotDir
	}
	df, err := fs.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		_ = df.Close()
	}()
	return df.Sync()
}

// IsNewRun checks if the SM is created for the first time.
func IsNewRun(fs vfs.FS, dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := fs.Stat(fp); err != nil {
		return true
	}
	return false
}

// GetNodeDBDirName gets DB dir name prefix.
func GetNodeDBDirName(baseDir string, hostname string, name string) string {
	return filepath.Join(baseDir, hostname, name)
}

// GetNewRandomDBDirName gets new random DB dir name.
func GetNewRandomDBDirName() string {
	// #nosec G404 -- Weak random number generator can be used because we do not care whether the result can be predicted.
	return fmt.Sprintf("%d_%d", rand.Uint64(), time.Now().UnixNano())
}

// ReplaceCurrentDBFile does currentDBFilename switch (with fsync).
func ReplaceCurrentDBFile(fs vfs.FS, dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := fs.Rename(tmpFp, fp); err != nil {
		return err
	}
	return syncDir(fs, dir)
}

// SaveCurrentDBDirName saves DB name into updatingDBFilename (with fsync).
func SaveCurrentDBDirName(fs vfs.FS, dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}
	fp := filepath.Join(dir, updatingDBFilename)
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = syncDir(fs, dir)
	}()
	if _, err = f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err = f.Write([]byte(dbdir)); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}

// GetCurrentDBDirName reads currentDBFilename file and return its contents.
func GetCurrentDBDirName(fs vfs.FS, dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := fs.Open(fp)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = f.Close()
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		return "", err
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err = h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		return "", err
	}
	return string(content), nil
}

// CreateNodeDataDir creates new SM data dir.
func CreateNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return syncDir(fs, filepath.Dir(dir))
}

// CleanupNodeDataDir cleans up old data dir (should be called after successful switch).
func CleanupNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.RemoveAll(filepath.Join(dir, updatingDBFilename)); err != nil {
		return err
	}
	dbdir, err := GetCurrentDBDirName(fs, dir)
	if err != nil {
		return err
	}
	files, err := fs.List(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if fi == currentDBFilename {
			continue
		}
		toDelete := filepath.Join(dir, fi)
		if toDelete != filepath.Join(dir, dbdir) {
			if err := fs.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}
	return nil
}
