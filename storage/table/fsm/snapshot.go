// Copyright JAMF Software, LLC

package fsm

import (
	"archive/tar"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	rp "github.com/jamf/regatta/pebble"
	"github.com/jamf/regatta/storage/errors"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type snapshot struct {
	fsm *FSM
}

type snapshotContext struct {
	*pebble.Snapshot
	once sync.Once
}

func (s *snapshotContext) Close() (err error) {
	s.once.Do(func() { err = s.Snapshot.Close() })
	return
}

func (s *snapshot) prepare() (any, error) {
	db := s.fsm.pebble.Load()
	ctx := &snapshotContext{Snapshot: db.NewSnapshot()}
	runtime.SetFinalizer(ctx, func(s *snapshotContext) { _ = s.Close() })
	return ctx, nil
}

func (s *snapshot) save(ctx any, w io.Writer, stopc <-chan struct{}) error {
	snapshot := ctx.(*snapshotContext)
	iter := snapshot.NewIter(nil)
	defer func() {
		if err := iter.Close(); err != nil {
			s.fsm.log.Error(err)
		}
		if err := snapshot.Close(); err != nil {
			s.fsm.log.Error(err)
		}
	}()

	// write L6 SSTs as there is 0 overlap of keys
	options := rp.WriterOptions(6)
	memfile := &memFile{}
	sstWriter := sstable.NewWriter(memfile, options)
	// iterate through the whole kv space and send it to writer
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
			if err := sstWriter.Set(iter.Key(), iter.Value()); err != nil {
				return err
			}
			// write SST when maxBatchSize reached and create new writer
			if sstWriter.EstimatedSize() >= maxBatchSize {
				if err := sstWriter.Close(); err != nil {
					return err
				}
				if err := writeLenDelimited(memfile, w); err != nil {
					return err
				}
				memfile.Reset()
				sstWriter = sstable.NewWriter(memfile, options)
			}
		}
	}
	// write the remaining KVs into last SST
	if err := sstWriter.Close(); err != nil {
		return err
	}
	return writeLenDelimited(memfile, w)
}

func (s *snapshot) recover(r io.Reader, stopc <-chan struct{}) (er error) {
	if s.fsm.closed {
		return errors.ErrStateMachineClosed
	}
	if s.fsm.clusterID < 1 {
		return errors.ErrInvalidClusterID
	}
	if s.fsm.nodeID < 1 {
		return errors.ErrInvalidNodeID
	}

	randomDirName := rp.GetNewRandomDBDirName()
	dbdir := filepath.Join(s.fsm.dirname, randomDirName)

	s.fsm.log.Infof("recovering pebble state machine with dirname: '%s'", dbdir)
	db, err := rp.OpenDB(
		dbdir,
		rp.WithFS(s.fsm.fs),
		rp.WithCache(s.fsm.blockCache),
		rp.WithLogger(s.fsm.log),
		rp.WithEventListener(makeLoggingEventListener(s.fsm.log)),
	)
	if err != nil {
		return err
	}

	var (
		count int
		size  uint64
		buff  []byte
		files []string
	)

read:
	for {
		select {
		case <-stopc:
			_ = db.Close()
			if err := rp.CleanupNodeDataDir(s.fsm.fs, s.fsm.dirname); err != nil {
				s.fsm.log.Debugf("unable to cleanup directory")
			}
			return sm.ErrSnapshotStopped
		default:
			if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
				if err == io.EOF {
					// this was the last SST
					break read
				}
				return err
			}
			if uint64(cap(buff)) < size {
				buff = make([]byte, size)
			}
			name := filepath.Join(s.fsm.dirname, fmt.Sprintf("ingest-%d.sst", count))
			f, err := s.fsm.fs.Create(name)
			if err != nil {
				return err
			}
			files = append(files, name)
			count++
			if _, err = io.CopyBuffer(f, io.LimitReader(r, int64(size)), buff); err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	if err := db.Ingest(files); err != nil {
		return err
	}

	if err := rp.SaveCurrentDBDirName(s.fsm.fs, s.fsm.dirname, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(s.fsm.fs, s.fsm.dirname); err != nil {
		return err
	}
	old := s.fsm.pebble.Swap(db)
	s.fsm.log.Info("snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	s.fsm.log.Debugf("snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(s.fsm.fs, s.fsm.dirname)
}

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

func (c *checkpoint) prepare() (any, error) {
	db := c.fsm.pebble.Load()
	if err := db.Flush(); err != nil {
		return nil, err
	}
	dir := path.Join(c.fsm.dirname, "checkpoint")
	if err := c.fsm.fs.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

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
			// #nosec G110 -- Tar stream is not compressed
			if _, err := io.Copy(fileToWrite, tr); err != nil {
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

	db, err := rp.OpenDB(
		dbdir,
		rp.WithFS(c.fsm.fs),
		rp.WithCache(c.fsm.blockCache),
		rp.WithLogger(c.fsm.log),
		rp.WithEventListener(makeLoggingEventListener(c.fsm.log)),
	)
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
	c.fsm.log.Info("snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	c.fsm.log.Debugf("snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(c.fsm.fs, c.fsm.dirname)
}
