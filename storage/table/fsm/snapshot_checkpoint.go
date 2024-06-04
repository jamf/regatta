// Copyright JAMF Software, LLC

package fsm

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
	rp "github.com/jamf/regatta/pebble"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/storage/errors"
)

type checkpointContext struct {
	once sync.Once
	dir  string
	fs   vfs.FS
}

func (s *checkpointContext) Close() (err error) {
	s.once.Do(func() { err = s.fs.RemoveAll(s.dir) })
	return
}

type checkpoint struct {
	fsm *FSM
}

func (c *checkpoint) getHeader() snapshotHeader {
	h := snapshotHeader{}
	h.setSnapshotType(RecoveryTypeCheckpoint)
	return h
}

func (c *checkpoint) prepare() (any, error) {
	db := c.fsm.pebble.Load()
	if err := db.Flush(); err != nil {
		return nil, err
	}
	dir := path.Join(c.fsm.dirname, "checkpoint", rp.GetNewRandomDBDirName())
	if err := db.Checkpoint(dir); err != nil {
		return nil, err
	}

	ctx := &checkpointContext{dir: dir, fs: c.fsm.fs}
	runtime.SetFinalizer(ctx, func(s *checkpointContext) { _ = s.Close() })
	return ctx, nil
}

func (c *checkpoint) save(ctx any, w io.Writer, stopc <-chan struct{}) error {
	snapshot := ctx.(*checkpointContext)
	defer snapshot.Close()
	tw := tar.NewWriter(w)
	list, err := c.fsm.fs.List(snapshot.dir)
	if err != nil {
		return err
	}
	for _, fn := range list {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
		}
		info, err := c.fsm.fs.Stat(path.Join(snapshot.dir, fn))
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, fn)
		if err != nil {
			return err
		}

		header.Name = filepath.ToSlash(fn)

		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.IsDir() {
			data, err := c.fsm.fs.Open(path.Join(snapshot.dir, fn))
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
	}
	// produce tar
	return tw.Close()
}

func (c *checkpoint) recover(r io.Reader, stopc <-chan struct{}) error {
	if c.fsm.closed {
		return errors.ErrStateMachineClosed
	}
	if c.fsm.clusterID < 1 {
		return errors.ErrInvalidClusterID
	}
	if c.fsm.nodeID < 1 {
		return errors.ErrInvalidNodeID
	}

	randomDirName := rp.GetNewRandomDBDirName()
	dbdir := filepath.Join(c.fsm.dirname, randomDirName)

	c.fsm.log.Infof("recovering pebble state machine with dirname: '%s'", dbdir)
	tr := tar.NewReader(r)

	if err := c.fsm.fs.MkdirAll(dbdir, 0o755); err != nil {
		return err
	}

	for {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
		}
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}

		if !filepath.IsLocal(header.Name) {
			return fmt.Errorf("path traversal detected path: '%s'", header.Name)
		}
		target := filepath.Join(dbdir, filepath.Clean(header.Name))

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err := c.fsm.fs.Stat(target); err != nil {
				if err := c.fsm.fs.MkdirAll(target, 0o755); err != nil {
					return err
				}
			}
		case tar.TypeReg:
			fileToWrite, err := c.fsm.fs.Create(target)
			if err != nil {
				return err
			}
			_, err = io.Copy(fileToWrite, tr) // #nosec G110
			if err != nil {
				return err
			}

			if err := fileToWrite.Sync(); err != nil {
				return err
			}
			if err := fileToWrite.Close(); err != nil {
				return err
			}
		}
	}

	db, err := c.fsm.openDB(dbdir)
	if err != nil {
		return err
	}
	idx, err := readLocalIndex(db, sysLocalIndex)
	if err != nil {
		return err
	}
	if err := rp.SaveCurrentDBDirName(c.fsm.fs, c.fsm.dirname, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(c.fsm.fs, c.fsm.dirname); err != nil {
		return err
	}
	old := c.fsm.pebble.Swap(db)
	c.fsm.metrics.applied.Store(idx)
	c.fsm.log.Info("snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	c.fsm.log.Debugf("snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(c.fsm.fs, c.fsm.dirname)
}
