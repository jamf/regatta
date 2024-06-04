// Copyright JAMF Software, LLC

package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/config"
	dbsm "github.com/jamf/regatta/raft/statemachine"
)

const (
	ResultCodeFailure = iota
	ResultCodeSuccess
	ResultCodeVersionMismatch
)

func NewLFSM() dbsm.CreateConcurrentStateMachineFunc {
	return func(clusterID, nodeID uint64) dbsm.IConcurrentStateMachine {
		l := &LFSM{
			clusterID: clusterID,
			nodeID:    nodeID,
			store:     NewMapStore(),
		}
		return l
	}
}

type LFSM struct {
	clusterID uint64
	nodeID    uint64
	store     *MapStore
}

func (fsm *LFSM) Update(entries []dbsm.Entry) ([]dbsm.Entry, error) {
	for i, ent := range entries {
		var update Update
		if err := json.Unmarshal(ent.Cmd, &update); err != nil {
			return entries, fmt.Errorf("invalid entry %#v, %w", ent, err)
		}

		if v, err := fsm.store.Get(update.KVPair.Key); err == nil {
			// Reject entries with mismatched versions
			if v.Ver != update.KVPair.Ver {
				data, _ := json.Marshal(v)
				entries[i].Result = dbsm.Result{
					Value: ResultCodeVersionMismatch,
					Data:  data,
				}
				continue
			}
		}
		update.KVPair.Ver = ent.Index
		switch update.Op {
		case UpdateOpSet:
			_, err := fsm.store.Set(update.KVPair.Key, update.KVPair.Value, update.KVPair.Ver)
			if err != nil {
				return nil, err
			}
		case UpdateOpDelete:
			err := fsm.store.Delete(update.KVPair.Key, update.KVPair.Ver)
			if err != nil {
				return nil, err
			}
		}

		b, _ := json.Marshal(update.KVPair)
		entries[i].Result = dbsm.Result{
			Value: ResultCodeSuccess,
			Data:  b,
		}
	}

	return entries, nil
}

func (fsm *LFSM) Lookup(e interface{}) (interface{}, error) {
	switch q := e.(type) {
	case QueryExist:
		return fsm.store.Exists(q.Key)
	case QueryKey:
		return fsm.store.Get(q.Key)
	case QueryAll:
		return fsm.store.GetAll(q.Pattern)
	case QueryAllValues:
		return fsm.store.GetAllValues(q.Pattern)
	case QueryList:
		return fsm.store.List(q.Path)
	case QueryListDir:
		return fsm.store.ListDir(q.Path)
	}
	return nil, fmt.Errorf("invalid query %#v", e)
}

func (fsm *LFSM) PrepareSnapshot() (interface{}, error) {
	return json.Marshal(fsm.store)
}

func (fsm *LFSM) SaveSnapshot(ctx interface{}, w io.Writer, _ dbsm.ISnapshotFileCollection, _ <-chan struct{}) error {
	_, err := io.Copy(w, bytes.NewReader(ctx.([]byte)))
	return err
}

func (fsm *LFSM) RecoverFromSnapshot(r io.Reader, _ []dbsm.SnapshotFile, _ <-chan struct{}) error {
	return json.NewDecoder(r).Decode(fsm.store)
}

func (fsm *LFSM) Close() (err error) {
	return
}

type QueryKey struct {
	Key string
}

type QueryExist struct {
	Key string
}

type QueryAll struct {
	Pattern string
}

type QueryAllValues struct {
	Pattern string
}

type QueryList struct {
	Path string
}

type QueryListDir struct {
	Path string
}

const (
	UpdateOpSet    = "set"
	UpdateOpDelete = "delete"

	proposalTimeout = 30 * time.Second
)

type Update struct {
	Op     string
	KVPair Pair
}

type RaftStore struct {
	NodeHost  *raft.NodeHost
	ClusterID uint64
}

func (r *RaftStore) Delete(key string, ver uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	b, err := json.Marshal(Update{Op: UpdateOpDelete, KVPair: Pair{Key: key, Ver: ver}})
	if err != nil {
		return err
	}
	res, err := r.NodeHost.SyncPropose(ctx, r.NodeHost.GetNoOPSession(r.ClusterID), b)
	if err != nil {
		return err
	}
	if res.Value == ResultCodeVersionMismatch {
		return ErrVersionMismatch
	}
	return nil
}

func (r *RaftStore) Exists(key string) (bool, error) {
	ok, err := r.NodeHost.StaleRead(r.ClusterID, QueryExist{Key: key})
	if err != nil {
		return false, err
	}
	return ok.(bool), nil
}

func (r *RaftStore) Get(key string) (Pair, error) {
	val, err := r.NodeHost.StaleRead(r.ClusterID, QueryKey{Key: key})
	if err != nil {
		return Pair{}, err
	}
	return val.(Pair), nil
}

func (r *RaftStore) GetAll(pattern string) ([]Pair, error) {
	val, err := r.NodeHost.StaleRead(r.ClusterID, QueryAll{Pattern: pattern})
	if err != nil {
		return nil, err
	}
	return val.([]Pair), nil
}

func (r *RaftStore) GetAllValues(pattern string) ([]string, error) {
	val, err := r.NodeHost.StaleRead(r.ClusterID, QueryAllValues{Pattern: pattern})
	if err != nil {
		return nil, err
	}
	return val.([]string), nil
}

func (r *RaftStore) List(filePath string) ([]string, error) {
	val, err := r.NodeHost.StaleRead(r.ClusterID, QueryList{Path: filePath})
	if err != nil {
		return nil, err
	}
	return val.([]string), nil
}

func (r *RaftStore) ListDir(filePath string) ([]string, error) {
	val, err := r.NodeHost.StaleRead(r.ClusterID, QueryListDir{Path: filePath})
	if err != nil {
		return nil, err
	}
	return val.([]string), nil
}

// Set sets the Pair entry associated with key to value and checks the version while doing so.
func (r *RaftStore) Set(key string, value string, ver uint64) (Pair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	pair := Pair{Key: key, Value: value, Ver: ver}
	b, err := json.Marshal(Update{Op: UpdateOpSet, KVPair: pair})
	if err != nil {
		return Pair{}, err
	}
	res, err := r.NodeHost.SyncPropose(ctx, r.NodeHost.GetNoOPSession(r.ClusterID), b)
	if err != nil {
		return Pair{}, err
	}
	err = json.Unmarshal(res.Data, &pair)
	if err != nil {
		return Pair{}, err
	}
	if res.Value == ResultCodeVersionMismatch {
		return pair, ErrVersionMismatch
	}
	return pair, nil
}

func (r *RaftStore) Start(cfg RaftConfig) error {
	if r.NodeHost.HasNodeInfo(r.ClusterID, cfg.NodeID) {
		return r.NodeHost.StartConcurrentReplica(
			map[uint64]raft.Target{},
			false,
			NewLFSM(),
			kvRaftConfig(cfg.NodeID, r.ClusterID, cfg),
		)
	}
	return r.NodeHost.StartConcurrentReplica(
		cfg.InitialMembers,
		false,
		NewLFSM(),
		kvRaftConfig(cfg.NodeID, r.ClusterID, cfg),
	)
}

func (r *RaftStore) HasLeader() bool {
	_, _, ok, _ := r.NodeHost.GetLeaderID(r.ClusterID)
	return ok
}

func (r *RaftStore) WaitForLeader(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, _, ok, _ := r.NodeHost.GetLeaderID(r.ClusterID)
			if ok {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

type RaftConfig struct {
	NodeID             uint64
	ElectionRTT        uint64
	HeartbeatRTT       uint64
	SnapshotEntries    uint64
	CompactionOverhead uint64
	MaxInMemLogSize    uint64
	InitialMembers     map[uint64]raft.Target
}

func kvRaftConfig(nodeID, clusterID uint64, cfg RaftConfig) config.Config {
	return config.Config{
		ReplicaID:           nodeID,
		ShardID:             clusterID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         cfg.ElectionRTT,
		HeartbeatRTT:        cfg.HeartbeatRTT,
		SnapshotEntries:     cfg.SnapshotEntries,
		CompactionOverhead:  cfg.CompactionOverhead,
		OrderedConfigChange: true,
		MaxInMemLogSize:     cfg.MaxInMemLogSize,
	}
}
