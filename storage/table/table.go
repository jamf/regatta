// Copyright JAMF Software, LLC

package table

import (
	"context"
	"io"

	"github.com/jamf/regatta/raft/client"
	sm "github.com/jamf/regatta/raft/statemachine"
	"github.com/jamf/regatta/regattapb"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table/fsm"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/jamf/regatta/util/iter"
)

type raftHandler interface {
	SyncRead(ctx context.Context, id uint64, req interface{}) (interface{}, error)
	StaleRead(id uint64, req interface{}) (interface{}, error)
	SyncPropose(ctx context.Context, session *client.Session, bytes []byte) (sm.Result, error)
	GetNoOPSession(id uint64) *client.Session
}

// MaxValueLen 2MB max value.
const MaxValueLen = 2 * 1024 * 1024

// Table stored representation of a table.
type Table struct {
	Name      string `json:"name"`
	ClusterID uint64 `json:"cluster_id"`
	RecoverID uint64 `json:"recover_id"`
}

// AsActive returns ActiveTable wrapper of this table.
func (t Table) AsActive(host raftHandler) ActiveTable {
	return ActiveTable{nh: host, session: host.GetNoOPSession(t.ClusterID), Table: t}
}

// ActiveTable could be queried and new proposals could be made through it.
type ActiveTable struct {
	Table
	nh      raftHandler
	session *client.Session
}

func readTable[S any](t *ActiveTable, ctx context.Context, linearizable bool, req any) (S, error) {
	var (
		err error
		val interface{}
	)
	if linearizable {
		val, err = t.nh.SyncRead(ctx, t.ClusterID, req)
	} else {
		val, err = t.nh.StaleRead(t.ClusterID, req)
	}
	if err != nil {
		return *new(S), err
	}
	return val.(S), nil
}

func proposeTable[S any](t *ActiveTable, ctx context.Context, cmd *regattapb.Command) (S, uint64, error) {
	bytes, err := cmd.MarshalVT()
	if err != nil {
		return *new(S), 0, err
	}
	res, err := t.nh.SyncPropose(ctx, t.session, bytes)
	if err != nil {
		return *new(S), 0, err
	}
	pr := &regattapb.CommandResult{}
	if err := pr.UnmarshalVTUnsafe(res.Data); err != nil {
		return *new(S), 0, err
	}
	if len(pr.Responses) == 0 {
		return *new(S), 0, serrors.ErrNoResultFound
	}
	switch r := pr.Responses[0].Response.(type) {
	case S:
		return r, pr.Revision, nil
	default:
		return *new(S), 0, serrors.ErrUnknownResultType
	}
}

// Range performs a Range query in the Raft data, supplied context must have a deadline set.
func (t *ActiveTable) Range(ctx context.Context, req *regattapb.RangeRequest) (*regattapb.RangeResponse, error) {
	if len(req.Key) > key.LatestVersionLen {
		return nil, serrors.ErrKeyLengthExceeded
	}
	if len(req.RangeEnd) > key.LatestVersionLen {
		return nil, serrors.ErrKeyLengthExceeded
	}

	response, err := readTable[*regattapb.ResponseOp_Range](t, ctx, req.Linearizable, &regattapb.RequestOp_Range{
		Key:       req.Key,
		RangeEnd:  req.RangeEnd,
		Limit:     req.Limit,
		KeysOnly:  req.KeysOnly,
		CountOnly: req.CountOnly,
	})
	if err != nil {
		return nil, err
	}
	return &regattapb.RangeResponse{
		Kvs:   response.Kvs,
		Count: response.Count,
		More:  response.More,
	}, nil
}

// Put performs a Put proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Put(ctx context.Context, req *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, serrors.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, serrors.ErrKeyLengthExceeded
	}
	if len(req.Value) > MaxValueLen {
		return nil, serrors.ErrValueLengthExceeded
	}
	cmd := &regattapb.Command{
		Type:  regattapb.Command_PUT,
		Table: req.Table,
		Kv: &regattapb.KeyValue{
			Key:   req.Key,
			Value: req.Value,
		},
		PrevKvs: req.PrevKv,
	}
	r, rev, err := proposeTable[*regattapb.ResponseOp_ResponsePut](t, ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &regattapb.PutResponse{PrevKv: r.ResponsePut.PrevKv, Header: &regattapb.ResponseHeader{Revision: rev}}, nil
}

// Delete performs a DeleteRange proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Delete(ctx context.Context, req *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	if len(req.Key) == 0 {
		return nil, serrors.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, serrors.ErrKeyLengthExceeded
	}
	cmd := &regattapb.Command{
		Type:  regattapb.Command_DELETE,
		Table: req.Table,
		Kv: &regattapb.KeyValue{
			Key: req.Key,
		},
		PrevKvs:  req.PrevKv,
		RangeEnd: req.RangeEnd,
		Count:    req.Count,
	}
	r, rev, err := proposeTable[*regattapb.ResponseOp_ResponseDeleteRange](t, ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &regattapb.DeleteRangeResponse{Deleted: r.ResponseDeleteRange.Deleted, PrevKvs: r.ResponseDeleteRange.PrevKvs, Header: &regattapb.ResponseHeader{Revision: rev}}, nil
}

func (t *ActiveTable) Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	// Do not propose read-only transactions through the log
	if req.IsReadonly() {
		return readTable[*regattapb.TxnResponse](t, ctx, true, req)
	}

	cmd := &regattapb.Command{
		Type:  regattapb.Command_TXN,
		Table: req.Table,
		Txn: &regattapb.Txn{
			Compare: req.Compare,
			Success: req.Success,
			Failure: req.Failure,
		},
	}

	bytes, err := cmd.MarshalVT()
	if err != nil {
		return nil, err
	}
	res, err := t.nh.SyncPropose(ctx, t.session, bytes)
	if err != nil {
		return nil, err
	}
	txr := &regattapb.CommandResult{}
	if err := txr.UnmarshalVTUnsafe(res.Data); err != nil {
		return nil, err
	}
	return &regattapb.TxnResponse{
		Succeeded: fsm.UpdateResult(res.Value) == fsm.ResultSuccess,
		Responses: txr.Responses,
		Header:    &regattapb.ResponseHeader{Revision: txr.Revision},
	}, nil
}

// Iterator returns open pebble.Iterator it is an API consumer responsibility to close it.
func (t *ActiveTable) Iterator(ctx context.Context, req *regattapb.RangeRequest) (iter.Seq[*regattapb.ResponseOp_Range], error) {
	return readTable[iter.Seq[*regattapb.ResponseOp_Range]](t, ctx, req.Linearizable, fsm.IteratorRequest{RangeOp: &regattapb.RequestOp_Range{
		Key:       req.Key,
		RangeEnd:  req.RangeEnd,
		Limit:     req.Limit,
		KeysOnly:  req.KeysOnly,
		CountOnly: req.CountOnly,
	}})
}

// Snapshot streams snapshot to the provided writer.
func (t *ActiveTable) Snapshot(ctx context.Context, writer io.Writer) (*fsm.SnapshotResponse, error) {
	return readTable[*fsm.SnapshotResponse](t, ctx, true, fsm.SnapshotRequest{Writer: writer, Stopper: ctx.Done()})
}

// LocalIndex returns local index.
func (t *ActiveTable) LocalIndex(ctx context.Context, linearizable bool) (*fsm.IndexResponse, error) {
	return readTable[*fsm.IndexResponse](t, ctx, linearizable, fsm.LocalIndexRequest{})
}

// LeaderIndex returns leader index.
func (t *ActiveTable) LeaderIndex(ctx context.Context, linearizable bool) (*fsm.IndexResponse, error) {
	return readTable[*fsm.IndexResponse](t, ctx, linearizable, fsm.LeaderIndexRequest{})
}

// Reset resets the leader index to 0.
func (t *ActiveTable) Reset(ctx context.Context) error {
	li := uint64(0)
	cmd := &regattapb.Command{
		Type:        regattapb.Command_DUMMY,
		Table:       []byte(t.Name),
		LeaderIndex: &li,
	}
	bts, err := cmd.MarshalVT()
	if err != nil {
		return err
	}
	_, err = t.nh.SyncPropose(ctx, t.session, bts)
	return err
}
