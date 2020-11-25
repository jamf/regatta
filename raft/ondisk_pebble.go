package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/oxtoacart/bpool"
	"go.uber.org/zap"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

const (
	kindUser   byte = 0x0
	kindSystem byte = 0x1
)

var (
	raftLogIndexKey = []byte{kindSystem, 0x1}
	bufferPool      = bpool.NewSizedBufferPool(256, 128)
	indexPool       = bpool.NewByteSlicePool(258, 8)
)

const (
	// levels is number of Pebble levels.
	levels = 7
	// targetFileSizeBase base file size (in L0).
	targetFileSizeBase = 16 * 1024 * 1024
	// blockSize FS block size.
	blockSize = 32 * 1024
	// targetFileSizeGrowFactor the factor of growth of targetFileSizeBase between levels.
	targetFileSizeGrowFactor = 2
	// writeBufferSize inmemory write buffer size.
	writeBufferSize = 4 * 1024 * 1024
	// maxWriteBufferNumber number of write buffers.
	maxWriteBufferNumber = 4
	// l0FileNumCompactionTrigger number of files in L0 to trigger automatic compaction.
	l0FileNumCompactionTrigger = 8
	// l0StopWritesTrigger number of files in L0 to stop accepting more writes.
	l0StopWritesTrigger = 24
	// maxBytesForLevelBase base for amount of data stored in a single level.
	maxBytesForLevelBase = 256 * 1024 * 1024
	// cacheSize LRU cache size.
	cacheSize = 8 * 1024 * 1024
	// maxLogFileSize maximum size of WAL files.
	maxLogFileSize = 128 * 1024 * 1024
	// maxBatchSize maximum size of inmemory batch before commit.
	maxBatchSize = 16 * 1024 * 1024
	// walMinSyncInterval minimum time between calls to WAL file Sync.
	walMinSyncInterval = 500 * time.Microsecond
)

func NewPebbleStateMachine(clusterID uint64, nodeID uint64, stateMachineDir string, walDirname string, fs vfs.FS) sm.IOnDiskStateMachine {
	if fs == nil {
		fs = vfs.Default
	}

	return &KVPebbleStateMachine{
		pebble:     nil,
		clusterID:  clusterID,
		nodeID:     nodeID,
		dirname:    stateMachineDir,
		walDirname: walDirname,
		fs:         fs,
		log:        zap.S().Named("ondisk"),
	}
}

// KVPebbleStateMachine is a IStateMachine struct used for testing purpose.
type KVPebbleStateMachine struct {
	pebble     unsafe.Pointer
	wo         *pebble.WriteOptions
	fs         vfs.FS
	clusterID  uint64
	nodeID     uint64
	dirname    string
	walDirname string
	closed     bool
	log        *zap.SugaredLogger
}

func (p *KVPebbleStateMachine) openDB(dbdir string, walDirname string) (*pebble.DB, error) {
	p.log.Infof("opening pebble state machine with dirname: '%s', walDirName: '%s'", dbdir, walDirname)

	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()

	lvlOpts := make([]pebble.LevelOptions, levels)
	sz := targetFileSizeBase
	for l := int64(0); l < levels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.SnappyCompression,
			BlockSize:      blockSize,
			TargetFileSize: int64(sz),
			FilterPolicy:   bloom.FilterPolicy(10),
			FilterType:     pebble.TableFilter,
		}
		sz = sz * targetFileSizeGrowFactor
		lvlOpts[l] = opt
	}
	// Do not create bloom filters for the last level (i.e. the largest level
	// which contains data in the LSM store). This configuration reduces the size
	// of the bloom filters by 10x. This is significant given that bloom filters
	// require 1.25 bytes (10 bits) per key which can translate into 100s of megabytes of
	// memory given typical key and value sizes. The downside is that bloom
	// filters will only be usable on the higher levels, but that seems
	// acceptable. We'll achieve 80-90% of the benefit of having bloom filters on every level for only 10% of the
	// memory cost.
	lvlOpts[len(lvlOpts)-1].FilterPolicy = nil

	return pebble.Open(dbdir, &pebble.Options{
		Cache:                       cache,
		FS:                          p.fs,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		LBaseMaxBytes:               maxBytesForLevelBase,
		Levels:                      lvlOpts,
		Logger:                      zap.S().Named("pebble"),
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		WALDir:                      walDirname,
		WALMinSyncInterval: func() time.Duration {
			// TODO make interval dynamic based on the load
			return walMinSyncInterval
		},
	})
}

func (p *KVPebbleStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	if p.clusterID < 1 {
		return 0, ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return 0, ErrInvalidNodeID
	}

	hostname, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	dir := getNodeDBDirName(p.dirname, hostname, p.clusterID, p.nodeID)
	if err := createNodeDataDir(p.fs, dir); err != nil {
		return 0, err
	}

	randomDir := getNewRandomDBDirName()
	var dbdir string
	if isNewRun(p.fs, dir) {
		dbdir = filepath.Join(dir, randomDir)
		if err := saveCurrentDBDirName(p.fs, dir, randomDir); err != nil {
			return 0, err
		}
		if err := replaceCurrentDBFile(p.fs, dir); err != nil {
			return 0, err
		}

		if isMigration(p.fs, dir) {
			if err := migrateDBDir(p.fs, dir, randomDir); err != nil {
				return 0, err
			}
		}
	} else {
		if err := cleanupNodeDataDir(p.fs, dir); err != nil {
			return 0, err
		}
		var err error
		randomDir, err = getCurrentDBDirName(p.fs, dir)
		if err != nil {
			return 0, err
		}
		dbdir = filepath.Join(dir, randomDir)
		if _, err := p.fs.Stat(filepath.Join(dir, randomDir)); err != nil {
			if os.IsNotExist(err) {
				return 0, err
			}
		}
	}

	walDirPath := p.getWalDirPath(hostname, randomDir, dbdir)

	db, err := p.openDB(dbdir, walDirPath)
	if err != nil {
		return 0, err
	}
	atomic.StorePointer(&p.pebble, unsafe.Pointer(db))
	p.wo = &pebble.WriteOptions{Sync: false}

	indexVal, closer, err := db.Get(raftLogIndexKey)
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

func (p *KVPebbleStateMachine) getWalDirPath(hostname string, randomDir string, dbdir string) string {
	var walDirPath string
	if p.walDirname != "" {
		walDirPath = path.Join(p.walDirname, hostname, fmt.Sprintf("%d-%d", p.nodeID, p.clusterID), randomDir)
	} else {
		walDirPath = dbdir
	}
	return walDirPath
}

// Update updates the object.
func (p *KVPebbleStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	cmd := proto.Command{}
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	batch := db.NewBatch()
	defer batch.Close()

	for i := 0; i < len(updates); i++ {
		err := pb.Unmarshal(updates[i].Cmd, &cmd)
		if err != nil {
			return nil, err
		}

		buf.Reset()
		buf.WriteByte(kindUser)
		buf.Write(cmd.Table)
		buf.Write(cmd.Kv.Key)

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

	idxBuffer := indexPool.GetSlice()
	defer indexPool.PutSlice(idxBuffer)
	binary.LittleEndian.PutUint64(idxBuffer.Bytes(), updates[len(updates)-1].Index)
	if err := batch.Set(raftLogIndexKey, idxBuffer.Bytes(), nil); err != nil {
		return nil, err
	}

	if err := batch.Commit(p.wo); err != nil {
		return nil, err
	}
	return updates, nil
}

// Lookup locally looks up the data.
func (p *KVPebbleStateMachine) Lookup(key interface{}) (interface{}, error) {
	switch req := key.(type) {
	case *proto.RangeRequest:
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)
		buf.WriteByte(kindUser)
		buf.Write(req.Table)
		buf.Write(req.Key)
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

	return nil, ErrUnknownQueryType
}

// Sync synchronizes all in-core state of the state machine to permanent
// storage so the state machine can continue from its latest state after
// reboot.
func (p *KVPebbleStateMachine) Sync() error {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.Flush()
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *KVPebbleStateMachine) PrepareSnapshot() (interface{}, error) {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.NewSnapshot(), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *KVPebbleStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
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
func (p *KVPebbleStateMachine) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) (er error) {
	if p.closed {
		return ErrStateMachineClosed
	}
	if p.clusterID < 1 {
		return ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return ErrInvalidNodeID
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	dir := getNodeDBDirName(p.dirname, hostname, p.clusterID, p.nodeID)

	randomDirName := getNewRandomDBDirName()
	dbdir := filepath.Join(dir, randomDirName)
	walDirPath := p.getWalDirPath(hostname, randomDirName, dbdir)

	db, err := p.openDB(dbdir, walDirPath)
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
			db.Close()
			if err := cleanupNodeDataDir(p.fs, dir); err != nil {
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

	if err := saveCurrentDBDirName(p.fs, dir, randomDirName); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(p.fs, dir); err != nil {
		return err
	}
	old := (*pebble.DB)(atomic.SwapPointer(&p.pebble, unsafe.Pointer(db)))
	p.log.Debugf("Snapshot recovery finished")

	if old != nil {
		old.Close()
	}
	p.log.Debugf("Snapshot recovery cleanup")
	return cleanupNodeDataDir(p.fs, dir)
}

// Close closes the KVStateMachine IStateMachine.
func (p *KVPebbleStateMachine) Close() error {
	p.closed = true
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.Close()
}

// GetHash gets the DB hash for test comparison.
func (p *KVPebbleStateMachine) GetHash() (uint64, error) {
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
