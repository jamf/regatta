// Copyright JAMF Software, LLC

package pebble

import (
	"io"
	"os"

	pvfs "github.com/cockroachdb/pebble/vfs"
	gvfs "github.com/lni/vfs"
)

// FS is a wrapper struct that implements the pebble/vfs.FS interface.
type FS struct {
	fs gvfs.FS
}

// NewPebbleFS creates a new pebble/vfs.FS instance.
func NewPebbleFS(fs gvfs.FS) pvfs.FS {
	return &FS{fs}
}

// GetDiskUsage ...
func (p *FS) GetDiskUsage(path string) (pvfs.DiskUsage, error) {
	du, err := p.fs.GetDiskUsage(path)
	if err != nil {
		return pvfs.DiskUsage{}, err
	}
	return pvfs.DiskUsage{
		AvailBytes: du.AvailBytes,
		TotalBytes: du.TotalBytes,
		UsedBytes:  du.UsedBytes,
	}, err
}

// Create ...
func (p *FS) Create(name string) (pvfs.File, error) {
	return p.fs.Create(name)
}

// Link ...
func (p *FS) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

// Open ...
func (p *FS) Open(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

func (p *FS) OpenReadWrite(name string, opts ...pvfs.OpenOption) (pvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenDir ...
func (p *FS) OpenDir(name string) (pvfs.File, error) {
	return p.fs.OpenDir(name)
}

// Remove ...
func (p *FS) Remove(name string) error {
	return p.fs.Remove(name)
}

// RemoveAll ...
func (p *FS) RemoveAll(name string) error {
	return p.fs.RemoveAll(name)
}

// Rename ...
func (p *FS) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// ReuseForWrite ...
func (p *FS) ReuseForWrite(oldname, newname string) (pvfs.File, error) {
	return p.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll ...
func (p *FS) MkdirAll(dir string, perm os.FileMode) error {
	return p.fs.MkdirAll(dir, perm)
}

// Lock ...
func (p *FS) Lock(name string) (io.Closer, error) {
	return p.fs.Lock(name)
}

// List ...
func (p *FS) List(dir string) ([]string, error) {
	return p.fs.List(dir)
}

// Stat ...
func (p *FS) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

// PathBase ...
func (p *FS) PathBase(path string) string {
	return p.fs.PathBase(path)
}

// PathJoin ...
func (p *FS) PathJoin(elem ...string) string {
	return p.fs.PathJoin(elem...)
}

// PathDir ...
func (p *FS) PathDir(path string) string {
	return p.fs.PathDir(path)
}
