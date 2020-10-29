package raft

import (
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
	"github.com/lni/dragonboat/v3/logger"
	"go.uber.org/zap"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

func NewBadgerStateMachine(clusterID uint64, nodeID uint64, stateMachineDir string, walDirname string) sm.IOnDiskStateMachine {
	return &KVBadgerStateMachine{
		db:         nil,
		clusterID:  clusterID,
		nodeID:     nodeID,
		dirname:    stateMachineDir,
		walDirname: walDirname,
		log:        zap.S().Named("ondisk"),
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
	dirname := path.Join(p.dirname, hostname, fmt.Sprintf("%d-%d", p.nodeID, p.clusterID))
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

	p.log.Infof("opening badger state machine with dirname: '%s', walDirName: '%s'", dirname, walDirname)

	l := NewLogger("badger")
	l.SetLevel(logger.INFO)
	o := badger.DefaultOptions(dirname).
		WithValueDir(walDirname).
		WithLogger(l).
		WithCompression(options.Snappy).
		WithSyncWrites(false)
	return badger.Open(o)
}

func (p *KVBadgerStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	db, err := p.openDB()
	if err != nil {
		return 0, err
	}
	p.db = db

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

// Update updates the object.
func (p *KVBadgerStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	cmd := proto.Command{}
	buf := bytes.NewBuffer(make([]byte, 0))
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

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
			if err := batch.Set(buf.Bytes(), cmd.Kv.Value); err != nil {
				return nil, err
			}
		case proto.Command_DELETE:
			if err := batch.Delete(buf.Bytes()); err != nil {
				return nil, err
			}
		}

		raftIndexVal := make([]byte, 8)
		binary.LittleEndian.PutUint64(raftIndexVal, updates[i].Index)
		if err := batch.Set(raftLogIndexKey, raftIndexVal); err != nil {
			return nil, err
		}

		updates[i].Result = sm.Result{Value: 1}
		buf.Reset()
	}

	if err := batch.Flush(); err != nil {
		return nil, err
	}
	return updates, nil
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
			val, err = it.ValueCopy(val)
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
	ts := time.Now().UnixNano() / int64(time.Millisecond)
	return p.db.NewStreamAt(uint64(ts)), nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *KVBadgerStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
	st := ctx.(*badger.Stream)
	_, err := st.Backup(w, 0)
	return err
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (p *KVBadgerStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	return p.db.Load(r, 10)
}

// Close closes the KVStateMachine IStateMachine.
func (p *KVBadgerStateMachine) Close() error {
	return p.db.Close()
}

// GetHash gets the DB hash for test comparison.
func (p *KVBadgerStateMachine) GetHash() (uint64, error) {
	hash64 := fnv.New64()
	err := p.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		// Compute Hash
		// iterate through he whole kv space and send it to hash func
		val := make([]byte, 0)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			_, err := hash64.Write(iter.Item().Key())
			if err != nil {
				return err
			}
			val, err := iter.Item().ValueCopy(val)
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
