package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

const (
	kindSystem byte = iota
	kindUser   byte = iota
)

const (
	raftLogIndex byte = iota
)

func NewPebbleStateMachine(clusterID uint64, nodeID uint64, stateMachineDir string, walDirname string) sm.IOnDiskStateMachine {
	return &KVPebbleStateMachine{
		pebble:     nil,
		clusterID:  clusterID,
		nodeID:     nodeID,
		dirname:    stateMachineDir,
		walDirname: walDirname,
	}
}

// KVStateMachine is a IStateMachine struct used for testing purpose.
type KVPebbleStateMachine struct {
	pebble     *pebble.DB
	clusterID  uint64
	nodeID     uint64
	dirname    string
	walDirname string
}

func (p *KVPebbleStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	var err error
	var walDirname string

	dirname := fmt.Sprintf("%s-%d-%d", p.dirname, p.clusterID, p.nodeID)
	if p.walDirname != "" {
		walDirname = fmt.Sprintf("%s-%d-%d", p.walDirname, p.clusterID, p.nodeID)
	}
	zap.S().Infof("opening pebble state machine with dirname: '%s', walDirName: '%s'", dirname, walDirname)

	p.pebble, err = pebble.Open(dirname, &pebble.Options{
		WALDir: walDirname,
	})
	if err != nil {
		zap.S().Panic(err)
	}

	indexVal, closer, err := p.pebble.Get([]byte{kindSystem, raftLogIndex})
	if err != nil {
		return 0, nil
	}

	defer func() {
		if err := closer.Close(); err != nil {
			zap.S().Warn(err)
		}
	}()

	// TODO example: consider adding to field d.lastApplied
	return binary.LittleEndian.Uint64(indexVal), nil
}

// Update updates the object.
func (p *KVPebbleStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	cmd := proto.Command{}
	buf := bytes.NewBuffer(make([]byte, 0))
	batch := p.pebble.NewBatch()
	for _, update := range updates {
		cmd.Reset()
		err := pb.Unmarshal(update.Cmd, &cmd)
		if err != nil {
			update.Result = sm.Result{Value: 0}
			return updates, err
		}

		buf.Reset()
		buf.WriteByte(kindUser)
		buf.Write(cmd.Table)
		buf.Write(cmd.Kv.Key)

		raftIndexVal := make([]byte, 8)

		switch cmd.Type {
		case proto.Command_PUT:
			if err := batch.Set(buf.Bytes(), cmd.Kv.Value, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				return updates, err
			}

			buf.Reset()
			buf.WriteByte(kindSystem)
			buf.WriteByte(raftLogIndex)
			binary.LittleEndian.PutUint64(raftIndexVal, update.Index)
			if err := batch.Set(buf.Bytes(), raftIndexVal, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				return updates, err
			}

			update.Result = sm.Result{Value: 1}
		case proto.Command_DELETE:
			batch := p.pebble.NewBatch()

			if err := batch.Delete(buf.Bytes(), nil); err != nil {
				update.Result = sm.Result{Value: 0}
				return updates, err
			}

			buf.Reset()
			buf.WriteByte(kindSystem)
			buf.WriteByte(raftLogIndex)
			binary.LittleEndian.PutUint64(raftIndexVal, update.Index)
			if err := batch.Set(buf.Bytes(), raftIndexVal, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				return updates, err
			}

			update.Result = sm.Result{Value: 1}
		}
		buf.Reset()
	}

	if err := batch.Commit(nil); err != nil {
		return updates, err
	}

	return updates, nil
}

// Lookup locally looks up the data.
func (p *KVPebbleStateMachine) Lookup(key interface{}) (interface{}, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(kindUser)
	buf.Write(key.([]byte))
	value, closer, err := p.pebble.Get(buf.Bytes())
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := closer.Close(); err != nil {
			zap.S().Error(err)
		}
	}()

	buf.Reset()
	buf.Write(value)
	return buf.Bytes(), nil
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
	return nil, fmt.Errorf("not implemented PrepareSnapshot()")
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (p *KVPebbleStateMachine) SaveSnapshot(_ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return fmt.Errorf("not implemented SaveSnapshot()")
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (p *KVPebbleStateMachine) RecoverFromSnapshot(_ io.Reader, _ <-chan struct{}) error {
	return fmt.Errorf("not implemented RecoverFromSnapshot()")
}

// Close closes the KVStateMachine IStateMachine.
func (p *KVPebbleStateMachine) Close() error {
	return p.pebble.Close()
}
