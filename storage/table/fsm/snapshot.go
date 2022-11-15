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
	sm "github.com/lni/dragonboat/v4/statemachine"
	rp "github.com/jamf/regatta/pebble"
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

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *FSM) PrepareSnapshot() (interface{}, error) {
	db := p.pebble.Load()
	ctx := &snapshotContext{Snapshot: db.NewSnapshot()}
	runtime.SetFinalizer(ctx, func(s *snapshotContext) { _ = s.Close() })
	return ctx, nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *FSM) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	snapshot := ctx.(*snapshotContext)
	iter := snapshot.NewIter(nil)
	defer func() {
		if err := iter.Close(); err != nil {
			p.log.Error(err)
		}
		if err := snapshot.Close(); err != nil {
			p.log.Error(err)
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

// RecoverFromSnapshot recovers the state machine state from snapshot specified by
// the io.Reader object. The snapshot is recovered into a new DB first and then
// atomically swapped with the existing DB to complete the recovery.
func (p *FSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) (er error) {
	if p.closed {
		return errors.ErrStateMachineClosed
	}
	if p.clusterID < 1 {
		return errors.ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return errors.ErrInvalidNodeID
	}

	randomDirName := rp.GetNewRandomDBDirName()
	dbdir := filepath.Join(p.dirname, randomDirName)

	p.log.Infof("recovering pebble state machine with dirname: '%s', walDirName: '%s'", dbdir)
	db, err := rp.OpenDB(
		dbdir,
		rp.WithFS(p.fs),
		rp.WithCache(p.blockCache),
		rp.WithLogger(p.log),
		rp.WithEventListener(makeLoggingEventListener(p.log)),
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
			if err := rp.CleanupNodeDataDir(p.fs, p.dirname); err != nil {
				p.log.Debugf("unable to cleanup directory")
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
			name := filepath.Join(p.dirname, fmt.Sprintf("ingest-%d.sst", count))
			f, err := p.fs.Create(name)
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

	if err := rp.SaveCurrentDBDirName(p.fs, p.dirname, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(p.fs, p.dirname); err != nil {
		return err
	}
	old := p.pebble.Swap(db)
	p.log.Info("snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	p.log.Debugf("snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(p.fs, p.dirname)
}
