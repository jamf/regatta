package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/oxtoacart/bpool"
	rp "github.com/wandera/regatta/pebble"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/raft"
	"github.com/wandera/regatta/storage/table/key"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

var (
	bufferPool    = bpool.NewSizedBufferPool(256, 128)
	localIndexKey = key.Key{
		KeyType: key.TypeSystem,
		Key:     []byte("index"),
	}
)

const (
	// maxBatchSize maximum size of inmemory batch before commit.
	maxBatchSize = 16 * 1024 * 1024
)

func NewFSM(tableName, stateMachineDir string, walDirname string, fs vfs.FS) sm.CreateOnDiskStateMachineFunc {
	if fs == nil {
		fs = vfs.Default
	}
	return func(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
		return &FSM{
			pebble:     nil,
			tableName:  tableName,
			clusterID:  clusterID,
			nodeID:     nodeID,
			dirname:    stateMachineDir,
			walDirname: walDirname,
			fs:         fs,
			log:        zap.S().Named("table").Named(tableName),
		}
	}
}

// FSM is a statemachine.IOnDiskStateMachine impl.
type FSM struct {
	pebble     unsafe.Pointer
	wo         *pebble.WriteOptions
	fs         vfs.FS
	clusterID  uint64
	nodeID     uint64
	tableName  string
	dirname    string
	walDirname string
	closed     bool
	log        *zap.SugaredLogger
}

func (p *FSM) Open(_ <-chan struct{}) (uint64, error) {
	if p.clusterID < 1 {
		return 0, raft.ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return 0, raft.ErrInvalidNodeID
	}
	hostname, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	dir := rp.GetNodeDBDirName(p.dirname, hostname, fmt.Sprintf("%s-%d", p.tableName, p.clusterID))
	if err := rp.CreateNodeDataDir(p.fs, dir); err != nil {
		return 0, err
	}

	randomDir := rp.GetNewRandomDBDirName()
	var dbdir string
	if rp.IsNewRun(p.fs, dir) {
		dbdir = filepath.Join(dir, randomDir)
		if err := rp.SaveCurrentDBDirName(p.fs, dir, randomDir); err != nil {
			return 0, err
		}
		if err := rp.ReplaceCurrentDBFile(p.fs, dir); err != nil {
			return 0, err
		}
	} else {
		if err := rp.CleanupNodeDataDir(p.fs, dir); err != nil {
			return 0, err
		}
		var err error
		randomDir, err = rp.GetCurrentDBDirName(p.fs, dir)
		if err != nil {
			return 0, err
		}
		dbdir = filepath.Join(dir, randomDir)
		if _, err := p.fs.Stat(filepath.Join(dir, randomDir)); err != nil {
			return 0, err
		}
	}

	walDirPath := p.getWalDirPath(hostname, randomDir, dbdir)

	p.log.Infof("opening pebble state machine with dirname: '%s', walDirName: '%s'", dbdir, walDirPath)
	db, err := rp.OpenDB(p.fs, dbdir, walDirPath)
	if err != nil {
		return 0, err
	}
	atomic.StorePointer(&p.pebble, unsafe.Pointer(db))
	p.wo = &pebble.WriteOptions{Sync: false}

	buf := bytes.NewBuffer(make([]byte, 0))
	if _, err := key.NewEncoder(buf).Encode(&localIndexKey); err != nil {
		return 0, err
	}
	indexVal, closer, err := db.Get(buf.Bytes())
	if err != nil {
		if err != pebble.ErrNotFound {
			return 0, err
		}
		return 0, nil
	}

	defer func() {
		if err := closer.Close(); err != nil {
			p.log.Warn(err)
		}
	}()

	return binary.LittleEndian.Uint64(indexVal), nil
}

func (p *FSM) getWalDirPath(hostname string, randomDir string, dbdir string) string {
	var walDirPath string
	if p.walDirname != "" {
		walDirPath = path.Join(p.walDirname, hostname, fmt.Sprintf("%s-%d", p.tableName, p.clusterID), randomDir)
	} else {
		walDirPath = dbdir
	}
	return walDirPath
}

// Update updates the object.
func (p *FSM) Update(updates []sm.Entry) ([]sm.Entry, error) {
	cmd := proto.Command{}
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	buf := bytes.NewBuffer(make([]byte, key.LatestVersionLen))
	enc := key.NewEncoder(buf)

	batch := db.NewBatch()
	defer batch.Close()

	for i := 0; i < len(updates); i++ {
		err := pb.Unmarshal(updates[i].Cmd, &cmd)
		if err != nil {
			return nil, err
		}
		buf.Reset()
		k := key.Key{
			KeyType: key.TypeUser,
			Key:     cmd.Kv.Key,
		}
		if _, err := enc.Encode(&k); err != nil {
			return nil, err
		}

		switch cmd.Type {
		case proto.Command_PUT:
			if err := batch.Set(buf.Bytes(), cmd.Kv.Value, nil); err != nil {
				return nil, err
			}
		case proto.Command_DELETE:
			if err := batch.Delete(buf.Bytes(), nil); err != nil {
				return nil, err
			}
		}
		updates[i].Result = sm.Result{Value: 1}
	}

	buf.Reset()
	if _, err := enc.Encode(&localIndexKey); err != nil {
		return nil, err
	}

	idx := make([]byte, 8)
	binary.LittleEndian.PutUint64(idx, updates[len(updates)-1].Index)
	if err := batch.Set(buf.Bytes(), idx, nil); err != nil {
		return nil, err
	}

	if err := batch.Commit(p.wo); err != nil {
		return nil, err
	}
	return updates, nil
}

// Lookup locally looks up the data.
func (p *FSM) Lookup(l interface{}) (interface{}, error) {
	switch req := l.(type) {
	case *proto.RangeRequest:
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)
		enc := key.NewEncoder(buf)
		k := key.Key{
			KeyType: key.TypeUser,
			Key:     req.Key,
		}
		if _, err := enc.Encode(&k); err != nil {
			return nil, err
		}
		db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
		value, closer, err := db.Get(buf.Bytes())
		if err != nil {
			return nil, err
		}

		defer func() {
			if err := closer.Close(); err != nil {
				p.log.Error(err)
			}
		}()

		tmp := make([]byte, len(value))
		copy(tmp, value)

		return &proto.RangeResponse{
			Kvs: []*proto.KeyValue{
				{
					Key:   req.Key,
					Value: tmp,
				},
			},
			Count: 1,
		}, nil
	case *proto.HashRequest:
		hash, err := p.GetHash()
		if err != nil {
			return nil, err
		}
		return &proto.HashResponse{Hash: hash}, nil
	}

	return nil, raft.ErrUnknownQueryType
}

// Sync synchronizes all in-core state of the state machine to permanent
// storage so the state machine can continue from its latest state after
// reboot.
func (p *FSM) Sync() error {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.Flush()
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *FSM) PrepareSnapshot() (interface{}, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.NewSnapshot(), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *FSM) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
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

	// iterate through he whole kv space and send it to writer
	for iter.First(); iter.Valid(); iter.Next() {
		kv := &proto.KeyValue{
			Key:   iter.Key(),
			Value: iter.Value(),
		}
		entry, err := pb.Marshal(kv)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, uint64(pb.Size(kv)))
		if err != nil {
			return err
		}
		if _, err := w.Write(entry); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot recovers the state machine state from snapshot specified by
// the io.Reader object. The snapshot is recovered into a new DB first and then
// atomically swapped with the existing DB to complete the recovery.
func (p *FSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) (er error) {
	if p.closed {
		return raft.ErrStateMachineClosed
	}
	if p.clusterID < 1 {
		return raft.ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return raft.ErrInvalidNodeID
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	dir := rp.GetNodeDBDirName(p.dirname, hostname, fmt.Sprintf("%s-%d", p.tableName, p.clusterID))

	randomDirName := rp.GetNewRandomDBDirName()
	dbdir := filepath.Join(dir, randomDirName)
	walDirPath := p.getWalDirPath(hostname, randomDirName, dbdir)

	p.log.Infof("recovering pebble state machine with dirname: '%s', walDirName: '%s'", dbdir, walDirPath)
	db, err := rp.OpenDB(p.fs, dbdir, walDirPath)
	if err != nil {
		return err
	}

	br := bufio.NewReaderSize(r, maxBatchSize)
	var total uint64
	if err := binary.Read(br, binary.LittleEndian, &total); err != nil {
		return err
	}

	kv := proto.KeyValue{}
	buffer := make([]byte, 0, 128*1024)
	b := db.NewBatch()
	defer b.Close()
	p.log.Debugf("Starting snapshot recover to %s DB", dbdir)
	var batchSize uint64
	for i := uint64(0); i < total; i++ {
		select {
		case <-stopc:
			_ = db.Close()
			if err := rp.CleanupNodeDataDir(p.fs, dir); err != nil {
				p.log.Debugf("unable to cleanup directory")
			}
			return sm.ErrSnapshotStopped
		default:
			p.log.Debugf("recover i %d", i)
			var toRead uint64
			if err := binary.Read(br, binary.LittleEndian, &toRead); err != nil {
				return err
			}

			batchSize = batchSize + toRead
			if cap(buffer) < int(toRead) {
				buffer = make([]byte, toRead)
			}

			if _, err := io.ReadFull(br, buffer[:toRead]); err != nil {
				return err
			}
			if err := pb.Unmarshal(buffer[:toRead], &kv); err != nil {
				return err
			}

			if err := b.Set(kv.Key, kv.Value, nil); err != nil {
				return err
			}

			if batchSize >= maxBatchSize {
				err := b.Commit(nil)
				if err != nil {
					return err
				}
				b = db.NewBatch()
				batchSize = 0
			}
		}
	}

	if err := b.Commit(&pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}

	if err := rp.SaveCurrentDBDirName(p.fs, dir, randomDirName); err != nil {
		return err
	}
	if err := rp.ReplaceCurrentDBFile(p.fs, dir); err != nil {
		return err
	}
	old := (*pebble.DB)(atomic.SwapPointer(&p.pebble, unsafe.Pointer(db)))
	p.log.Debugf("Snapshot recovery finished")

	if old != nil {
		_ = old.Close()
	}
	p.log.Debugf("Snapshot recovery cleanup")
	return rp.CleanupNodeDataDir(p.fs, dir)
}

// Close closes the KVStateMachine IStateMachine.
func (p *FSM) Close() error {
	p.closed = true
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	if db == nil {
		return nil
	}
	return db.Close()
}

// GetHash gets the DB hash for test comparison.
func (p *FSM) GetHash() (uint64, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	snap := db.NewSnapshot()
	iter := snap.NewIter(nil)
	defer func() {
		if err := iter.Close(); err != nil {
			p.log.Error(err)
		}
	}()

	// Compute Hash
	hash64 := fnv.New64()
	// iterate through he whole kv space and send it to hash func
	for iter.First(); iter.Valid(); iter.Next() {
		_, err := hash64.Write(iter.Key())
		if err != nil {
			return 0, err
		}
		_, err = hash64.Write(iter.Value())
		if err != nil {
			return 0, err
		}
	}

	return hash64.Sum64(), nil
}
