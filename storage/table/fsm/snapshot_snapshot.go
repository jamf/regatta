// Copyright JAMF Software, LLC

package fsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	rp "github.com/jamf/regatta/pebble"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/storage/errors"
)

type snapshotContext struct {
	*pebble.Snapshot
	once sync.Once
}

func (s *snapshotContext) Close() (err error) {
	s.once.Do(func() { err = s.Snapshot.Close() })
	return
}

type snapshot struct {
	fsm *FSM
}

func (s *snapshot) getHeader() snapshotHeader {
	h := snapshotHeader{}
	h.setSnapshotType(RecoveryTypeSnapshot)
	return h
}

func (s *snapshot) prepare() (any, error) {
	db := s.fsm.pebble.Load()
	ctx := &snapshotContext{Snapshot: db.NewSnapshot()}
	runtime.SetFinalizer(ctx, func(s *snapshotContext) { _ = s.Close() })
	return ctx, nil
}

func (s *snapshot) save(ctx any, w io.Writer, stopc <-chan struct{}) error {
	snapshot := ctx.(*snapshotContext)
	iter, err := snapshot.NewIter(nil)
	if err != nil {
		return err
	}
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
	db, err := s.fsm.openDB(dbdir)
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
	idx, err := readLocalIndex(db, sysLocalIndex)
	if err != nil {
		return err
	}
	if err := rp.SaveCurrentDBDirName(s.fsm.fs, s.fsm.dirname, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(s.fsm.fs, s.fsm.dirname); err != nil {
		return err
	}
	old := s.fsm.pebble.Swap(db)
	s.fsm.metrics.applied.Store(idx)
	s.fsm.log.Info("snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	s.fsm.log.Debugf("snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(s.fsm.fs, s.fsm.dirname)
}
