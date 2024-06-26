// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	gvfs "github.com/lni/vfs"
)

// IFS is the vfs interface used by dragonboat.
type IFS = gvfs.FS

// MemFS is a memory backed file system for testing purposes.
type MemFS = gvfs.MemFS

// DefaultFS is a vfs instance using underlying OS fs.
var DefaultFS = gvfs.Default

// File is the file interface returned by IFS.
type File = gvfs.File

// NewMemFS creates a in-memory fs.
func NewMemFS() IFS {
	return gvfs.NewStrictMem()
}

// IsNotExist returns a boolean value indicating whether the specified error is
// to indicate that a file or directory does not exist.
func IsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

// IsExist returns a boolean value indicating whether the specified error is to
// indicate that a file or directory already exists.
func IsExist(err error) bool {
	return errors.Is(err, os.ErrExist)
}

// TempDir returns the directory use for storing temporary files.
func TempDir() string {
	return os.TempDir()
}

// Clean is a wrapper for filepath.Clean.
func Clean(dir string) string {
	return filepath.Clean(dir)
}

// ReportLeakedFD reports leaked file fds.
func ReportLeakedFD(fs IFS, t *testing.T) {
	gvfs.ReportLeakedFD(fs, t)
}
