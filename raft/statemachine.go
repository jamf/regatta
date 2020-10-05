package raft

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

func NewStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &PersistentStateMachine{}
}

// PersistentStateMachine is a IStateMachine struct used for testing purpose.
type PersistentStateMachine struct {
	MillisecondToSleep uint64
	NoAlloc            bool
}

// Lookup locally looks up the data.
func (n *PersistentStateMachine) Lookup(key interface{}) (interface{}, error) {
	return make([]byte, 1), nil
}

// NALookup locally looks up the data.
func (n *PersistentStateMachine) NALookup(key []byte) ([]byte, error) {
	return key, nil
}

// Update updates the object.
func (n *PersistentStateMachine) Update(data []byte) (sm.Result, error) {
	if n.MillisecondToSleep > 0 {
		time.Sleep(time.Duration(n.MillisecondToSleep) * time.Millisecond)
	}
	if n.NoAlloc {
		return sm.Result{Value: uint64(len(data))}, nil
	}
	v := make([]byte, len(data))
	copy(v, data)
	return sm.Result{Value: uint64(len(data)), Data: v}, nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (n *PersistentStateMachine) SaveSnapshot(w io.Writer, fileCollection sm.ISnapshotFileCollection, done <-chan struct{}) error {
	data, err := json.Marshal(n)
	if err != nil {
		panic(err)
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
	var sn PersistentStateMachine
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &sn)
	if err != nil {
		panic("failed to unmarshal snapshot")
	}

	return nil
}

// Close closes the PersistentStateMachine IStateMachine.
func (n *PersistentStateMachine) Close() error { return nil }

// GetHash returns a uint64 value representing the current state of the object.
func (n *PersistentStateMachine) GetHash() (uint64, error) {
	return 0, nil
}
