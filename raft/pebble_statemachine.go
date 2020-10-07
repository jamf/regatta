package raft

import (
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

func NewPebbleStateMachine(_ uint64, _ uint64) sm.IOnDiskStateMachine {
	return &KVPebbleStateMachine{
		storage: nil, // to be init in the Open
		dirname: PersistentDirname,
	}
}

// TODO Q: How do we test this? Should the methods like Lookup be tested in unit tests? If so, we need to provide in memory FS for pebble.
// KVStateMachine is a IStateMachine struct used for testing purpose.
type KVPebbleStateMachine struct {
	storage *pebble.DB
	dirname string
}

func (p *KVPebbleStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	var err error

	zap.S().Debugf("opening pebble state machine with dirname: %s", p.dirname)
	p.storage, err = pebble.Open(p.dirname, &pebble.Options{})
	if err != nil {
		panic(err) // TODO this is fatal, right? Cannot continue.
	}

	// TODO return correct most recent raft log index from the persistent storage
	// store it as some system key?
	return 0, nil
}

// TODO flush/sync needed?
// TODO not handling the raft index
// Update updates the object.
func (p *KVPebbleStateMachine) Update(updates []sm.Entry) ([]sm.Entry, error) {
	// TODO inefficient implementation, rework to batch update on the pebble side
	// this will break if the change is applied only partially - there will be inconsistence between pebble and the raft log
	for _, update := range updates {
		cmd := proto.Command{}
		err := pb.Unmarshal(update.Cmd, &cmd)
		if err != nil {
			update.Result = sm.Result{Value: 0}
			// TODO not sure about the error handling here in case the incomplete update is done
			return updates, err
		}

		key := string(append(cmd.Table, cmd.Kv.Key...))
		switch cmd.Type {
		case proto.Command_PUT:
			if err := p.storage.Set([]byte(key), cmd.Kv.Value, nil); err != nil {
				update.Result = sm.Result{Value: 0}
				// TODO not sure about the error handling here in case the incomplete update is done
				return updates, err
			}
			update.Result = sm.Result{Value: 1}
		case proto.Command_DELETE:
			if err := p.storage.Delete([]byte(key), nil); err != nil {
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
	value, closer, err := p.storage.Get(key.([]byte))

	defer func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				panic("failed to close") // TODO this is fatal, right? Cannot continue.
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
	return p.storage.Close()
}
