package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"io"
	"io/ioutil"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	pb "google.golang.org/protobuf/proto"
)

func NewStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &PersistentStateMachine{
		storage: make(map[string][]byte),
	}
}

// PersistentStateMachine is a IStateMachine struct used for testing purpose.
type PersistentStateMachine struct {
	storage map[string][]byte
}

// Lookup locally looks up the data.
func (n *PersistentStateMachine) Lookup(key interface{}) (interface{}, error) {
	if value, ok := n.storage[string(key.([]byte))]; ok {
		return value, nil
	}
	return nil, storage.ErrNotFound
}

// Update updates the object.
func (n *PersistentStateMachine) Update(data []byte) (sm.Result, error) {
	cmd := proto.Command{}
	err := pb.Unmarshal(data, &cmd)
	if err != nil {
		return sm.Result{}, err
	}

	key := string(append(cmd.Table, cmd.Kv.Key...))
	switch cmd.Type {
	case proto.Command_PUT:
		n.storage[key] = cmd.Kv.Value
		return sm.Result{Value: 1}, nil
	case proto.Command_DELETE:
		if _, ok := n.storage[key]; ok {
			delete(n.storage, key)
			return sm.Result{Value: 1}, nil
		}
	}

	return sm.Result{Value: 0}, nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (n *PersistentStateMachine) SaveSnapshot(w io.Writer, fileCollection sm.ISnapshotFileCollection, done <-chan struct{}) error {
	data, err := json.Marshal(n.storage)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *PersistentStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &n.storage)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the PersistentStateMachine IStateMachine.
func (n *PersistentStateMachine) Close() error { return nil }

// GetHash returns a uint64 value representing the current state of the object.
func (n *PersistentStateMachine) GetHash() (uint64, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(n.storage)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b.Bytes()), nil
}
