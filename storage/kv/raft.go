package kv

import (
	"context"
	"io"
	"time"

	"github.com/lni/dragonboat/v3"
)

import (
	"encoding/json"
	"fmt"

	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	ResultCodeFailure = iota
	ResultCodeSuccess
	ResultCodeVersionMismatch
)

type Entry struct {
	Key string `json:"key"`
	Ver uint64 `json:"ver"`
	Val string `json:"val"`
}

func NewLFSM() dbsm.CreateConcurrentStateMachineFunc {
	return func(clusterID, nodeID uint64) dbsm.IConcurrentStateMachine {
		data := make(map[string]Pair)
		return &LFSM{
			clusterID: clusterID,
			nodeID:    nodeID,
			data:      data,
			store:     &MapStore{m: data},
		}
	}
}

type LFSM struct {
	clusterID uint64
	nodeID    uint64
	data      map[string]Pair
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
	fsm.store.RLock()
	defer fsm.store.RUnlock()
	ctx := make(map[string]interface{})
	for k, v := range fsm.data {
		ctx[k] = v
	}
	return ctx, nil
}

func (fsm *LFSM) SaveSnapshot(ctx interface{}, w io.Writer, _ dbsm.ISnapshotFileCollection, _ <-chan struct{}) error {
	return json.NewEncoder(w).Encode(ctx)
}

func (fsm *LFSM) RecoverFromSnapshot(r io.Reader, _ []dbsm.SnapshotFile, _ <-chan struct{}) (err error) {
	fsm.store.Lock()
	defer fsm.store.Unlock()
	return json.NewDecoder(r).Decode(&fsm.data)
}

func (fsm *LFSM) Close() (err error) {
	return
}

type QueryKey struct {
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

	proposalTimeout = 1 * time.Second
)

type Update struct {
	Op     string
	KVPair Pair
}

type RaftStore struct {
	NodeHost  *dragonboat.NodeHost
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

func (r *RaftStore) Exists(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	_, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryKey{Key: key})
	return err == nil
}

func (r *RaftStore) Get(key string) (Pair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	val, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryKey{Key: key})
	if err != nil {
		return Pair{}, err
	}
	return val.(Pair), nil
}

func (r *RaftStore) GetAll(pattern string) (Pairs, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	val, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryAll{Pattern: pattern})
	if err != nil {
		return nil, err
	}
	return val.(Pairs), nil
}

func (r *RaftStore) GetAllValues(pattern string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	val, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryAllValues{Pattern: pattern})
	if err != nil {
		return nil, err
	}
	return val.([]string), nil
}

func (r *RaftStore) List(filePath string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	val, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryList{Path: filePath})
	if err != nil {
		return nil, err
	}
	return val.([]string), nil
}

func (r *RaftStore) ListDir(filePath string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	val, err := r.NodeHost.SyncRead(ctx, r.ClusterID, QueryListDir{Path: filePath})
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
