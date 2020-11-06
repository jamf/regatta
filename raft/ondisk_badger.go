package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/snappy"
	"github.com/lni/dragonboat/v3/logger"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

const (
	// maxTableSize max inmemory table size.
	maxTableSize = 4 * 1024 * 1024
	// indexCacheSize inmemory indices cache size.
	indexCacheSize = 32 * 1024 * 1024
	// valueLogFileSize maximum size of a value log file.
	valueLogFileSize = 128 * 1024 * 1024
	// valueLogThreshold threshold for storing value data in LSM instead of in value log.
	valueLogThreshold = 1024
	// gcTick how often to trigger value log garbage collection.
	gcTick = 5 * time.Minute
	// gcDiscardRatio threshold for rewriting the value log file (if more than the threshold could be reclaimed then reclaim).
	gcDiscardRatio = 0.25
	// compressionSnappy bit for entry.UserData that marks the value as compressed.
	compressionSnappy = 0x1
)

func NewBadgerStateMachine(clusterID uint64, nodeID uint64, stateMachineDir string, walDirname string) sm.IOnDiskStateMachine {
	return &KVBadgerStateMachine{
		db:         nil,
		clusterID:  clusterID,
		nodeID:     nodeID,
		dirname:    stateMachineDir,
		walDirname: walDirname,
		log:        zap.S().Named("ondisk"),
		gcEnabled:  true,
	}
}

// KVBadgerStateMachine is a IStateMachine struct used for testing purpose.
type KVBadgerStateMachine struct {
	db         *badger.DB
	clusterID  uint64
	nodeID     uint64
	dirname    string
	walDirname string
	log        *zap.SugaredLogger
	inMemory   bool
	stopGC     func()
	gcEnabled  bool
}

func (p *KVBadgerStateMachine) openDB() (*badger.DB, error) {
	if p.clusterID < 1 {
		return nil, ErrInvalidClusterID
	}
	if p.nodeID < 1 {
		return nil, ErrInvalidNodeID
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	var walDirname string
	var dirname string

	if !p.inMemory {
		dirname = path.Join(p.dirname, hostname, fmt.Sprintf("%d-%d", p.nodeID, p.clusterID))
		if p.walDirname != "" {
			walDirname = path.Join(p.walDirname, hostname, fmt.Sprintf("%d-%d", p.nodeID, p.clusterID))
		} else {
			walDirname = dirname
		}

		err = os.MkdirAll(dirname, 0777)
		if err != nil {
			return nil, err
		}

		err = os.MkdirAll(walDirname, 0777)
		if err != nil {
			return nil, err
		}
	}

	p.log.Infof("opening badger state machine with dirname: '%s', walDirName: '%s'", dirname, walDirname)

	l := NewLogger("badger")
	l.SetLevel(logger.INFO)
	o := badger.DefaultOptions(dirname).
		WithValueDir(walDirname).
		WithLogger(l).
		WithCompression(options.Snappy).
		WithSyncWrites(false).
		WithInMemory(p.inMemory).
		WithMaxTableSize(maxTableSize).
		WithIndexCacheSize(indexCacheSize).
		WithValueLogFileSize(valueLogFileSize).
		WithValueThreshold(valueLogThreshold)
	return badger.Open(o)
}

func (p *KVBadgerStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	db, err := p.openDB()
	if err != nil {
		return 0, err
	}
	p.db = db

	if p.gcEnabled {
		p.stopGC = p.runGC()
	}

	indexVal := make([]byte, 8)
	err = p.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(raftLogIndexKey)
		if err != nil {
			return err
		}
		indexVal, err = it.ValueCopy(indexVal)
		return err
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return 0, err
		}
		return 0, nil
	}

	return binary.LittleEndian.Uint64(indexVal), nil
}

func (p *KVBadgerStateMachine) runGC() func() {
	stopChan := make(chan struct{})
	go func() {
		// Run value log GC.
		defer func() {
			stopChan <- struct{}{}
		}()
		var count int
		ticker := time.NewTicker(gcTick)
		defer ticker.Stop()
		for {
			select {
			case <-stopChan:
				p.log.Infof("number of times value log GC was successful: %d", count)
				return
			case <-ticker.C:
				for {
					p.log.Infof("starting a value log GC")
					err := p.db.RunValueLogGC(gcDiscardRatio)
					if err != nil {
						p.log.Infof("result of value log GC: %v", err)
						break
					}
					p.log.Info("result of value log GC: Success")
					count++
				}
			}
		}
	}()
	return func() {
		stopChan <- struct{}{}
	}
}

// Update updates the object.
func (p *KVBadgerStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

	for i := 0; i < len(updates); i++ {
		cmd := &proto.Command{}
		err := pb.Unmarshal(updates[i].Cmd, cmd)
		if err != nil {
			return nil, err
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1+len(cmd.Table)+len(cmd.Kv.Key)))
		buf.WriteByte(kindUser)
		buf.Write(cmd.Table)
		buf.Write(cmd.Kv.Key)

		switch cmd.Type {
		case proto.Command_PUT:
			ent := newEntry(buf.Bytes(), cmd.Kv.Value)
			if err := batch.SetEntry(ent); err != nil {
				return nil, err
			}
		case proto.Command_DELETE:
			if err := batch.Delete(buf.Bytes()); err != nil {
				return nil, err
			}
		}
		updates[i].Result = sm.Result{Value: 1}
	}

	raftIndexVal := make([]byte, 8)
	binary.LittleEndian.PutUint64(raftIndexVal, updates[len(updates)-1].Index)
	if err := batch.Set(raftLogIndexKey, raftIndexVal); err != nil {
		return nil, err
	}

	if err := batch.Flush(); err != nil {
		return nil, err
	}
	return updates, nil
}

func newEntry(key, value []byte) *badger.Entry {
	var enc byte
	if len(value) > valueLogThreshold {
		value = snappy.Encode(nil, value)
		enc = compressionSnappy
	}
	ent := badger.NewEntry(key, value).WithMeta(enc)
	return ent
}

func getValue(item *badger.Item, val []byte) ([]byte, error) {
	val, err := item.ValueCopy(val)
	if err != nil {
		return nil, err
	}
	if item.UserMeta()&compressionSnappy != 0 {
		val, err = snappy.Decode(nil, val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

// Lookup locally looks up the data.
func (p *KVBadgerStateMachine) Lookup(key interface{}) (interface{}, error) {
	switch req := key.(type) {
	case *proto.RangeRequest:
		buf := bytes.NewBuffer(make([]byte, 0))
		buf.WriteByte(kindUser)
		buf.Write(req.Table)
		buf.Write(req.Key)
		val := make([]byte, 0)
		err := p.db.View(func(txn *badger.Txn) error {
			it, err := txn.Get(buf.Bytes())
			if err != nil {
				return err
			}
			val, err = getValue(it, val)
			return err
		})
		if err != nil {
			return nil, err
		}
		return &proto.RangeResponse{
			Kvs: []*proto.KeyValue{
				{
					Key:   req.Key,
					Value: val,
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
func (p *KVBadgerStateMachine) Sync() error {
	return p.db.Sync()
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed.
func (p *KVBadgerStateMachine) PrepareSnapshot() (interface{}, error) {
	return p.db.NewTransaction(false), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *KVBadgerStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
	txn := ctx.(*badger.Txn)
	defer txn.Discard()

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	// calculate the total snapshot size and send to writer
	count := uint64(0)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		count++
	}
	err := binary.Write(w, binary.LittleEndian, count)
	if err != nil {
		return err
	}

	val := make([]byte, 0)
	// iterate through he whole kv space and send it to writer
	for iter.Rewind(); iter.Valid(); iter.Next() {
		val, err = getValue(iter.Item(), val)
		if err != nil {
			return err
		}
		kv := &proto.KeyValue{
			Key:   iter.Item().Key(),
			Value: val,
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
	return txn.Commit()
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (p *KVBadgerStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	br := bufio.NewReaderSize(r, maxBatchSize)
	var total uint64
	if err := binary.Read(br, binary.LittleEndian, &total); err != nil {
		return err
	}

	buffer := make([]byte, 0, 128*1024)
	b := p.db.NewWriteBatch()
	defer b.Cancel()

	var batchSize uint64
	for i := uint64(0); i < total; i++ {
		var toRead uint64
		if err := binary.Read(br, binary.LittleEndian, &toRead); err != nil {
			return err
		}

		batchSize = batchSize + toRead
		if cap(buffer) < int(toRead) {
			buffer = make([]byte, toRead)
		}

		kv := &proto.KeyValue{}
		if _, err := io.ReadFull(br, buffer[:toRead]); err != nil {
			return err
		}
		if err := pb.Unmarshal(buffer[:toRead], kv); err != nil {
			return err
		}

		if err := b.Set(kv.Key, kv.Value); err != nil {
			return err
		}

		if batchSize >= maxBatchSize {
			err := b.Flush()
			if err != nil {
				return err
			}
			b = p.db.NewWriteBatch()
			batchSize = 0
		}
	}
	return b.Flush()
}

// Close closes the KVStateMachine IStateMachine.
func (p *KVBadgerStateMachine) Close() error {
	if p.stopGC != nil {
		p.stopGC()
	}
	return p.db.Close()
}

// GetHash gets the DB hash for test comparison.
func (p *KVBadgerStateMachine) GetHash() (uint64, error) {
	hash64 := fnv.New64()
	err := p.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		// Compute Hash
		// iterate through he whole kv space and send it to hash func
		val := make([]byte, 0)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			_, err := hash64.Write(iter.Item().Key())
			if err != nil {
				return err
			}
			val, err := getValue(iter.Item(), val)
			if err != nil {
				return err
			}
			_, err = hash64.Write(val)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return hash64.Sum64(), nil
}
