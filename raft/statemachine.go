package raft

import (
	"bytes"
	"crypto/md5"
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
	return &KVStateMachine{
		storage: make(map[string][]byte),
	}
}

// KVStateMachine is a IStateMachine struct used for testing purpose.
type KVStateMachine struct {
	storage map[string][]byte
}

// Lookup locally looks up the data.
func (n *KVStateMachine) Lookup(key interface{}) (interface{}, error) {
	if key == storage.QueryHash {
		return n.GetHash()
	}
	if value, ok := n.storage[string(key.([]byte))]; ok {
		return value, nil
	}
	return nil, storage.ErrNotFound
}

// Update updates the object.
func (n *KVStateMachine) Update(data []byte) (sm.Result, error) {
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
func (n *KVStateMachine) SaveSnapshot(w io.Writer, fileCollection sm.ISnapshotFileCollection, done <-chan struct{}) error {
	data, err := json.Marshal(n.storage)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *KVStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &n.storage)
}

// Close closes the KVStateMachine IStateMachine.
func (n *KVStateMachine) Close() error { return nil }

// GetHash returns a uint64 value representing the current state of the object.
func (n *KVStateMachine) GetHash() (uint64, error) {
	// Encode to bin format
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(n.storage)
	if err != nil {
		return 0, err
	}
	// Compute MD5
	sum := md5.Sum(b.Bytes())
	return binary.LittleEndian.Uint64(sum[:]), nil
}
