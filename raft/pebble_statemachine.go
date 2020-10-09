package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

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

func NewPebbleStateMachine(_ uint64, _ uint64) sm.IOnDiskStateMachine {
	return &KVPebbleStateMachine{
		pebble: nil, // to be init in the Open
		dirname: PersistentDirname,
	}
}

// TODO Q: How do we test this? Should the methods like Lookup be tested in unit tests? If so, we need to provide in memory FS for pebble.
// KVStateMachine is a IStateMachine struct used for testing purpose.
type KVPebbleStateMachine struct {
	pebble  *pebble.DB
	dirname string
}

func (p *KVPebbleStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	var err error

	zap.S().Debugf("opening pebble state machine with dirname: %s", p.dirname)
	p.pebble, err = pebble.Open(p.dirname, &pebble.Options{})
	if err != nil {
		zap.S().Panic(err) // TODO this is fatal, right? Cannot continue.
	}

	indexVal, closer, err := p.pebble.Get([]byte{kindSystem, raftLogIndex})

	defer func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				zap.S().Panic("failed to close") // TODO this is fatal, right? Cannot continue.
			}
		}
	}()

	if err != nil {
		return 0, nil
	}

	return binary.LittleEndian.Uint64(indexVal), nil
}

// TODO flush/sync needed?
// Update updates the object.
func (p *KVPebbleStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	// TODO inefficient implementation, rework to batch update on the pebble side
	// this will break if the change is applied only partially - there will be inconsistence between pebble and the raft log
	cmd := proto.Command{}
	for _, update := range updates {
		cmd.Reset()
		err := pb.Unmarshal(update.Cmd, &cmd)
		if err != nil {
			update.Result = sm.Result{Value: 0}
			// TODO not sure about the error handling here in case the incomplete update is done
			return updates, err
		}

		// pre-allocate the userDataKey slice for the better performance
		userDataKey := make([]byte, int(reflect.TypeOf(kindUser).Size())+len(cmd.Table)+len(cmd.Kv.Key)) // TODO Isn't using reflect unnecessarily inefficient?
		userDataKey[0] = kindUser
		copy(userDataKey[reflect.TypeOf(kindUser).Size():], cmd.Table)
		copy(userDataKey[int(reflect.TypeOf(kindUser).Size())+len(cmd.Table):], cmd.Kv.Key)

		raftIndexKey := []byte{kindSystem, raftLogIndex}
		raftIndexVal := make([]byte, reflect.TypeOf(uint64(0)).Size()) // TODO I've read this might be unsafe though
		switch cmd.Type {
		case proto.Command_PUT:
			if err := p.pebble.Set(userDataKey, cmd.Kv.Value, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				// TODO not sure about the error handling here in case the incomplete update is done
				return updates, err
			}

			binary.LittleEndian.PutUint64(raftIndexVal, update.Index)
			if err := p.pebble.Set(raftIndexKey, raftIndexVal, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				// TODO not sure about the error handling here in case the incomplete update is done
				return updates, err
			}
			update.Result = sm.Result{Value: 1}
		case proto.Command_DELETE:
			if err := p.pebble.Delete(userDataKey, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				// TODO not sure about the error handling here in case the incomplete update is done
				return updates, err
			}

			binary.LittleEndian.PutUint64(raftIndexVal, update.Index)
			if err := p.pebble.Set(raftIndexKey, raftIndexVal, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				// TODO not sure about the error handling here in case the incomplete update is done
				return updates, err
			}
			update.Result = sm.Result{Value: 1}
		}
	}

	return updates, nil
}

// Lookup locally looks up the data.
func (p *KVPebbleStateMachine) Lookup(key interface{}) (interface{}, error) {
	queryKey := make([]byte, len(key.([]byte))+1)
	queryKey[0] = kindUser
	copy(queryKey[1:], key.([]byte))
	value, closer, err := p.pebble.Get(queryKey)

	defer func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				zap.S().Panic("failed to close") // TODO this is fatal, right? Cannot continue.
			}
		}
	}()

	if err != nil {
		return nil, err
	}

	// TODO this is not efficient!
	// closer.Close() must be however called in order to prevent a memory leak
	// but after calling closer.Close() the value becomes invalid
	// consider passing closer to the return value as well and close it after the value is returned
	// to the client
	return append([]byte(nil), value...), nil
}

// Sync synchronizes all in-core state of the state machine to permanent
// storage so the state machine can continue from its latest state after
// reboot.
func (p *KVPebbleStateMachine) Sync() error {
	return nil // TODO not implemented, it should not be required in the initial non-batch update style
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
