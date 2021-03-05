package raft

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

const (
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updating"
)

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

// functions below are used to manage the current data directory of Pebble DB.
func isNewRun(fs vfs.FS, dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := fs.Stat(fp); err != nil {
		return true
	}
	return false
}

func getNodeDBDirName(baseDir string, hostname string, clusterID uint64, nodeID uint64) string {
	return filepath.Join(baseDir, hostname, fmt.Sprintf("%d-%d", nodeID, clusterID))
}

func getNewRandomDBDirName() string {
	return fmt.Sprintf("%d_%d", rand.Uint64(), time.Now().UnixNano())
}

func replaceCurrentDBFile(fs vfs.FS, dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := fs.Rename(tmpFp, fp); err != nil {
		return err
	}
	return syncDir(fs, dir)
}

func saveCurrentDBDirName(fs vfs.FS, dir string, dbdir string) error {
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

func getCurrentDBDirName(fs vfs.FS, dir string) (string, error) {
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

func createNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return syncDir(fs, filepath.Dir(dir))
}

func cleanupNodeDataDir(fs vfs.FS, dir string) error {
	if err := fs.RemoveAll(filepath.Join(dir, updatingDBFilename)); err != nil {
		return err
	}
	dbdir, err := getCurrentDBDirName(fs, dir)
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
