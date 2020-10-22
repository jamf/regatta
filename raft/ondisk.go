package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"

	"github.com/cockroachdb/pebble/vfs"
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

var raftLogIndexKey = []byte{kindSystem, 0x1}

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
	cacheSize = 1024
	// maxLogFileSize maximum size of WAL files.
	maxLogFileSize = 128 * 1024 * 1024
)

func NewPebbleStateMachine(clusterID uint64, nodeID uint64, stateMachineDir string, walDirname string, fs vfs.FS) sm.IOnDiskStateMachine {
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

// KVStateMachine is a IStateMachine struct used for testing purpose.
type KVPebbleStateMachine struct {
	pebble     *pebble.DB
	fs         vfs.FS
	clusterID  uint64
	nodeID     uint64
	dirname    string
	walDirname string
	log        *zap.SugaredLogger
}

func (p *KVPebbleStateMachine) openDB() (*pebble.DB, error) {
	if p.clusterID < 1 {
		return nil, ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return nil, ErrInvalidNodeID
	}
	var walDirname string
	dirname := fmt.Sprintf("%s-%d-%d", p.dirname, p.clusterID, p.nodeID)
	if p.walDirname != "" {
		walDirname = fmt.Sprintf("%s-%d-%d", p.walDirname, p.clusterID, p.nodeID)
	} else {
		walDirname = dirname
	}

	p.log.Infof("opening pebble state machine with dirname: '%s', walDirName: '%s'", dirname, walDirname)

	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()

	lvlOpts := make([]pebble.LevelOptions, levels)
	sz := targetFileSizeBase
	for l := int64(0); l < levels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: int64(sz),
		}
		sz = sz * targetFileSizeGrowFactor
		lvlOpts[l] = opt
	}
	// Compress the lowest level
	lvlOpts[levels-1].Compression = pebble.SnappyCompression

	var fs vfs.FS
	if p.fs != nil {
		fs = p.fs
	}
	return pebble.Open(dirname, &pebble.Options{
		Cache:                       cache,
		FS:                          fs,
		Levels:                      lvlOpts,
		Logger:                      zap.S().Named("pebble"),
		WALDir:                      walDirname,
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		LBaseMaxBytes:               maxBytesForLevelBase,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
	})
}

func (p *KVPebbleStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	db, err := p.openDB()
	if err != nil {
		return 0, err
	}
	p.pebble = db

	indexVal, closer, err := p.pebble.Get(raftLogIndexKey)
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

// Update updates the object.
func (p *KVPebbleStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	cmd := proto.Command{}
	buf := bytes.NewBuffer(make([]byte, 0))
	batch := p.pebble.NewBatch()
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

		raftIndexVal := make([]byte, 8)
		binary.LittleEndian.PutUint64(raftIndexVal, updates[i].Index)
		if err := batch.Set(raftLogIndexKey, raftIndexVal, nil); err != nil {
			return nil, err
		}

		updates[i].Result = sm.Result{Value: 1}
		buf.Reset()
	}

	if err := batch.Commit(nil); err != nil {
		return nil, err
	}
	return updates, nil
}

// Lookup locally looks up the data.
func (p *KVPebbleStateMachine) Lookup(key interface{}) (interface{}, error) {
	switch req := key.(type) {
	case *proto.RangeRequest:
		buf := bytes.NewBuffer(make([]byte, 0))
		buf.WriteByte(kindUser)
		buf.Write(req.Table)
		buf.Write(req.Key)
		value, closer, err := p.pebble.Get(buf.Bytes())
		if err != nil {
			return nil, err
		}

		defer func() {
			if err := closer.Close(); err != nil {
				p.log.Error(err)
			}
		}()

		buf.Reset()
		buf.Write(value)
		return &proto.RangeResponse{
			Kvs: []*proto.KeyValue{
				{
					Key:   req.Key,
					Value: buf.Bytes(),
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
	return p.pebble.Flush()
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *KVPebbleStateMachine) PrepareSnapshot() (interface{}, error) {
	return p.pebble.NewSnapshot(), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *KVPebbleStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
	snapshot := ctx.(*pebble.Snapshot)
	totalLen := make([]byte, 8)
	entryLen := make([]byte, 4)
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
	binary.LittleEndian.PutUint64(totalLen, count)
	if _, err := w.Write(totalLen); err != nil {
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
		binary.LittleEndian.PutUint32(entryLen, uint32(len(entry)))
		if _, err := w.Write(entryLen); err != nil {
			return err
		}
		if _, err := w.Write(entry); err != nil {
			return err
		}
		count++
	}
	return nil
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (p *KVPebbleStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	lenBuf := make([]byte, 8)

	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(lenBuf)
	lenBuf = lenBuf[:4]
	for i := uint64(0); i < total; i++ {
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return err
		}
		toRead := binary.LittleEndian.Uint32(lenBuf)
		data := make([]byte, toRead)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		kv := proto.KeyValue{}
		if err := pb.Unmarshal(data, &kv); err != nil {
			return err
		}
		if err := p.pebble.Set(kv.Key, kv.Value, nil); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the KVStateMachine IStateMachine.
func (p *KVPebbleStateMachine) Close() error {
	return p.pebble.Close()
}

// GetHash gets the DB hash for test comparison.
func (p *KVPebbleStateMachine) GetHash() (uint64, error) {
	snap := p.pebble.NewSnapshot()
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
