package fsm

import (
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
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/table/key"
	"go.uber.org/zap"
)

var (
	bufferPool    = bpool.NewSizedBufferPool(256, 128)
	wildcard      = []byte{0}
	sysLocalIndex = mustEncodeKey(key.Key{
		KeyType: key.TypeSystem,
		Key:     []byte("index"),
	})
	sysLeaderIndex = mustEncodeKey(key.Key{
		KeyType: key.TypeSystem,
		Key:     []byte("leader_index"),
	})
)

const (
	// maxBatchSize maximum size of inmemory batch before commit.
	maxBatchSize = 16 * 1024 * 1024
)

const (
	// ResultFailure failed to apply update.
	ResultFailure = iota
	// ResultSuccess applied update.
	ResultSuccess
)

func New(tableName, stateMachineDir string, walDirname string, fs vfs.FS, blockCache *pebble.Cache) sm.CreateOnDiskStateMachineFunc {
	if fs == nil {
		fs = vfs.Default
	}
	return func(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
		hostname, _ := os.Hostname()
		dbDirName := rp.GetNodeDBDirName(stateMachineDir, hostname, fmt.Sprintf("%s-%d", tableName, clusterID))
		if walDirname != "" {
			walDirname = rp.GetNodeDBDirName(walDirname, hostname, fmt.Sprintf("%s-%d", tableName, clusterID))
		} else {
			walDirname = dbDirName
		}

		return &FSM{
			pebble:     nil,
			tableName:  tableName,
			clusterID:  clusterID,
			nodeID:     nodeID,
			dirname:    dbDirName,
			walDirname: walDirname,
			fs:         fs,
			blockCache: blockCache,
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
	blockCache *pebble.Cache
}

func (p *FSM) Open(_ <-chan struct{}) (uint64, error) {
	if p.clusterID < 1 {
		return 0, storage.ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return 0, storage.ErrInvalidNodeID
	}

	if err := rp.CreateNodeDataDir(p.fs, p.dirname); err != nil {
		return 0, err
	}

	randomDir := rp.GetNewRandomDBDirName()
	var dbdir string
	if rp.IsNewRun(p.fs, p.dirname) {
		dbdir = filepath.Join(p.dirname, randomDir)
		if err := rp.SaveCurrentDBDirName(p.fs, p.dirname, randomDir); err != nil {
			return 0, err
		}
		if err := rp.ReplaceCurrentDBFile(p.fs, p.dirname); err != nil {
			return 0, err
		}
	} else {
		if err := rp.CleanupNodeDataDir(p.fs, p.dirname); err != nil {
			return 0, err
		}
		var err error
		randomDir, err = rp.GetCurrentDBDirName(p.fs, p.dirname)
		if err != nil {
			return 0, err
		}
		dbdir = filepath.Join(p.dirname, randomDir)
		if _, err := p.fs.Stat(filepath.Join(p.dirname, randomDir)); err != nil {
			return 0, err
		}
	}

	walDirPath := path.Join(p.walDirname, randomDir)

	p.log.Infof("opening pebble state machine with dirname: '%s', walDirName: '%s'", dbdir, walDirPath)
	db, err := rp.OpenDB(p.fs, dbdir, walDirPath, p.blockCache)
	if err != nil {
		return 0, err
	}
	atomic.StorePointer(&p.pebble, unsafe.Pointer(db))
	p.wo = &pebble.WriteOptions{Sync: false}

	return readLocalIndex(db, sysLocalIndex)
}

func readLocalIndex(db pebble.Reader, indexKey []byte) (idx uint64, err error) {
	indexVal, closer, err := db.Get(indexKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return 0, err
		}
		return 0, nil
	}

	defer func() {
		err = closer.Close()
	}()

	return binary.LittleEndian.Uint64(indexVal), nil
}

// Sync synchronizes all in-core state of the state machine to permanent
// storage so the state machine can continue from its latest state after
// reboot.
func (p *FSM) Sync() error {
	db := (*pebble.DB)(atomic.LoadPointer(&p.pebble))
	return db.Flush()
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

// fillEntriesFunc fills proto.RangeResponse response.
type fillEntriesFunc func(k key.Key, value []byte, response *proto.RangeResponse) error

// iterator prepares new pebble.Iterator with upper and lower bound.
func iterator(db *pebble.DB, req *proto.RangeRequest) (*pebble.Iterator, fillEntriesFunc, error) {
	lowerBuf := bytes.NewBuffer(make([]byte, 0, key.LatestKeyLen(len(req.Key))))
	err := encodeUserKey(lowerBuf, req.Key)
	if err != nil {
		return nil, nil, err
	}

	iterOptions := &pebble.IterOptions{LowerBound: lowerBuf.Bytes()}
	if req.RangeEnd != nil && !bytes.Equal(req.RangeEnd, wildcard) {
		upperBuf := bytes.NewBuffer(make([]byte, 0, key.LatestKeyLen(len(req.RangeEnd))))
		err = encodeUserKey(upperBuf, req.RangeEnd)
		if err != nil {
			return nil, nil, err
		}
		iterOptions.UpperBound = upperBuf.Bytes()
	}

	fill := addKVPair
	if req.KeysOnly {
		fill = addKeyOnly
	} else if req.CountOnly {
		fill = addCountOnly
	}

	return db.NewIter(iterOptions), fill, nil
}

// iterate until the provided pebble.Iterator is no longer valid or the limit is reached.
// Apply a function on the key/value pair in every iteration filling proto.RangeResponse.
func iterate(iter *pebble.Iterator, limit int, f fillEntriesFunc, response *proto.RangeResponse) error {
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		k := key.Key{}
		r := bytes.NewReader(iter.Key())
		decoder := key.NewDecoder(r)
		if err := decoder.Decode(&k); err != nil {
			return err
		}

		if k.KeyType != key.TypeUser {
			continue
		}

		if i == limit && limit != 0 {
			response.More = iter.Next()
			break
		}
		i++

		if err := f(k, iter.Value(), response); err != nil {
			return err
		}
	}
	return nil
}

// addKVPair adds a key/value pair from the provided iterator to the proto.RangeResponse.
func addKVPair(k key.Key, valueBytes []byte, response *proto.RangeResponse) error {
	tmpVal := make([]byte, len(valueBytes))
	copy(tmpVal, valueBytes)

	kv := &proto.KeyValue{
		Key:   k.Key,
		Value: tmpVal,
	}
	response.Kvs = append(response.Kvs, kv)
	response.Count++
	return nil
}

// addKeyOnly adds a key from the provided iterator to the proto.RangeResponse.
func addKeyOnly(k key.Key, _ []byte, response *proto.RangeResponse) error {
	response.Kvs = append(response.Kvs, &proto.KeyValue{Key: k.Key})
	response.Count++
	return nil
}

// addCountOnly increments number of keys from the provided iterator to the proto.RangeResponse.
func addCountOnly(_ key.Key, _ []byte, response *proto.RangeResponse) error {
	response.Count++
	return nil
}

// encodeUserKey into provided writer.
func encodeUserKey(dst io.Writer, keyBytes []byte) error {
	enc := key.NewEncoder(dst)
	k := &key.Key{
		KeyType: key.TypeUser,
		Key:     keyBytes,
	}

	if _, err := enc.Encode(k); err != nil {
		return err
	}

	return nil
}

func mustEncodeKey(k key.Key) []byte {
	// Pre-encode system keys
	buff := bytes.NewBuffer(make([]byte, 0))
	enc := key.NewEncoder(buff)
	n, err := enc.Encode(&k)
	if err != nil {
		panic(err)
	}
	encoded := make([]byte, n)
	copy(encoded, buff.Bytes())
	return encoded
}
