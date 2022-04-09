package fsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
)

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *FSM) PrepareSnapshot() (interface{}, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.NewSnapshot(), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *FSM) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	snapshot := ctx.(*pebble.Snapshot)
	iter := snapshot.NewIter(nil)
	defer func() {
		if err := iter.Close(); err != nil {
			p.log.Error(err)
		}
		if err := snapshot.Close(); err != nil {
			p.log.Error(err)
		}
	}()

	// calculate the total snapshot size and send to writer
	count := uint64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	err := binary.Write(w, binary.LittleEndian, count)
	if err != nil {
		return err
	}

	// iterate through the whole kv space and send it to writer
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
			kv := &proto.KeyValue{
				Key:   iter.Key(),
				Value: iter.Value(),
			}
			entry, err := kv.MarshalVT()
			if err != nil {
				return err
			}
			err = binary.Write(w, binary.LittleEndian, uint64(kv.SizeVT()))
			if err != nil {
				return err
			}
			if _, err := w.Write(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// RecoverFromSnapshot recovers the state machine state from snapshot specified by
// the io.Reader object. The snapshot is recovered into a new DB first and then
// atomically swapped with the existing DB to complete the recovery.
func (p *FSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) (er error) {
	if p.closed {
		return storage.ErrStateMachineClosed
	}
	if p.clusterID < 1 {
		return storage.ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return storage.ErrInvalidNodeID
	}

	randomDirName := rp.GetNewRandomDBDirName()
	dbdir := filepath.Join(p.dirname, randomDirName)
	walDirPath := path.Join(p.walDirname, randomDirName)

	p.log.Infof("recovering pebble state machine with dirname: '%s', walDirName: '%s'", dbdir, walDirPath)
	db, err := rp.OpenDB(
		dbdir,
		rp.WithFS(p.fs),
		rp.WithWALDir(walDirPath),
		rp.WithCache(p.blockCache),
		rp.WithLogger(p.log),
		rp.WithEventListener(makeLoggingEventListener(p.log)),
	)
	if err != nil {
		return err
	}

	var total uint64
	if err := binary.Read(r, binary.LittleEndian, &total); err != nil {
		return err
	}

	kv := &proto.KeyValue{}
	buffer := make([]byte, 0, 128*1024)
	p.log.Debugf("Starting snapshot recover to %s DB", dbdir)

	count := 0
	var files []string
	rollSST := func() (*sstable.Writer, vfs.File, error) {
		name := filepath.Join(p.dirname, fmt.Sprintf("ingest-%d.sst", count))
		f, err := p.fs.Create(name)
		if err != nil {
			return nil, nil, err
		}
		files = append(files, name)
		count = count + 1
		return sstable.NewWriter(f, rp.WriterOptions()), f, nil
	}

	w, f, err := rollSST()
	if err != nil {
		return err
	}
	syncAndCloseSST := func() (err error) {
		defer func() {
			err = w.Close()
		}()
		return f.Sync()
	}

	var first, last []byte

	for i := uint64(0); i < total; i++ {
		select {
		case <-stopc:
			_ = db.Close()
			if err := rp.CleanupNodeDataDir(p.fs, p.dirname); err != nil {
				p.log.Debugf("unable to cleanup directory")
			}
			return sm.ErrSnapshotStopped
		default:
			p.log.Debugf("recover i %d", i)
			var toRead uint64
			if err := binary.Read(r, binary.LittleEndian, &toRead); err != nil {
				return err
			}

			if cap(buffer) < int(toRead) {
				buffer = make([]byte, toRead)
			}

			if _, err := io.ReadFull(r, buffer[:toRead]); err != nil {
				return err
			}
			kv.Reset()
			if err := kv.UnmarshalVT(buffer[:toRead]); err != nil {
				return err
			}

			if first == nil {
				first = kv.Key
			}
			last = kv.Key

			if err := w.Set(kv.Key, kv.Value); err != nil {
				return err
			}

			if w.EstimatedSize() >= maxBatchSize {
				if err := syncAndCloseSST(); err != nil {
					return err
				}
				w, f, err = rollSST()
				if err != nil {
					return err
				}
			}
		}
	}

	if err := syncAndCloseSST(); err != nil {
		return err
	}

	if err := db.Ingest(files); err != nil {
		return err
	}

	if err := db.Compact(first, last, false); err != nil {
		return err
	}

	if err := rp.SaveCurrentDBDirName(p.fs, p.dirname, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(p.fs, p.dirname); err != nil {
		return err
	}
	old := (*pebble.DB)(atomic.SwapPointer(&p.pebble, unsafe.Pointer(db)))
	p.log.Debugf("Snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	p.log.Debugf("Snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(p.fs, p.dirname)
}
