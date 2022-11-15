// Copyright JAMF Software, LLC

package table

import (
	"context"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table/fsm"
	"github.com/jamf/regatta/storage/table/key"
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
	return ActiveTable{nh: host, Table: t}
}

// ActiveTable could be queried and new proposals could be made through it.
type ActiveTable struct {
	Table
	nh raftHandler
}

// Range performs a Range query in the Raft data, supplied context must have a deadline set.
func (t *ActiveTable) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if len(req.Key) > key.LatestVersionLen {
		return nil, errors.ErrKeyLengthExceeded
	}
	if len(req.RangeEnd) > key.LatestVersionLen {
		return nil, errors.ErrKeyLengthExceeded
	}
	var (
		err   error
		val   interface{}
		reqOp = &proto.RequestOp_Range{
			Key:       req.Key,
			RangeEnd:  req.RangeEnd,
			Limit:     req.Limit,
			KeysOnly:  req.KeysOnly,
			CountOnly: req.CountOnly,
		}
	)

	if req.Linearizable {
		val, err = t.nh.SyncRead(ctx, t.ClusterID, reqOp)
	} else {
		val, err = t.nh.StaleRead(t.ClusterID, reqOp)
	}

	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, errors.ErrKeyNotFound
		}
		return nil, err
	}

	response := val.(*proto.ResponseOp_Range)
	return &proto.RangeResponse{
		Kvs:   response.Kvs,
		Count: response.Count,
		More:  response.More,
	}, nil
}

// Put performs a Put proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, errors.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, errors.ErrKeyLengthExceeded
	}
	if len(req.Value) > MaxValueLen {
		return nil, errors.ErrValueLengthExceeded
	}
	cmd := &proto.Command{
		Type:  proto.Command_PUT,
		Table: req.Table,
		Kv: &proto.KeyValue{
			Key:   req.Key,
			Value: req.Value,
		},
		PrevKvs: req.PrevKv,
	}
	bytes, err := cmd.MarshalVT()
	if err != nil {
		return nil, err
	}
	res, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	pr := &proto.CommandResult{}
	if err := pr.UnmarshalVT(res.Data); err != nil {
		return nil, err
	}
	if len(pr.Responses) == 0 {
		return nil, errors.ErrNoResultFound
	}
	switch r := pr.Responses[0].Response.(type) {
	case *proto.ResponseOp_ResponsePut:
		return &proto.PutResponse{PrevKv: r.ResponsePut.PrevKv, Header: &proto.ResponseHeader{Revision: pr.Revision}}, nil
	default:
		return nil, errors.ErrUnknownResultType
	}
}

// Delete performs a DeleteRange proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if len(req.Key) == 0 {
		return nil, errors.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, errors.ErrKeyLengthExceeded
	}
	cmd := &proto.Command{
		Type:  proto.Command_DELETE,
		Table: req.Table,
		Kv: &proto.KeyValue{
			Key: req.Key,
		},
		PrevKvs:  req.PrevKv,
		RangeEnd: req.RangeEnd,
	}
	bytes, err := cmd.MarshalVT()
	if err != nil {
		return nil, err
	}

	res, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	dr := &proto.CommandResult{}
	if err := dr.UnmarshalVT(res.Data); err != nil {
		return nil, err
	}
	if len(dr.Responses) == 0 {
		return nil, errors.ErrNoResultFound
	}
	switch r := dr.Responses[0].Response.(type) {
	case *proto.ResponseOp_ResponseDeleteRange:
		return &proto.DeleteRangeResponse{Deleted: r.ResponseDeleteRange.Deleted, PrevKvs: r.ResponseDeleteRange.PrevKvs, Header: &proto.ResponseHeader{Revision: dr.Revision}}, nil
	default:
		return nil, errors.ErrUnknownResultType
	}
}

func (t *ActiveTable) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	// Do not propose read-only transactions through the log
	if isReadonlyTransaction(req) {
		val, err := t.nh.SyncRead(ctx, t.ClusterID, req)
		if err != nil {
			return nil, err
		}
		return val.(*proto.TxnResponse), nil
	}

	cmd := &proto.Command{
		Type:  proto.Command_TXN,
		Table: req.Table,
		Txn: &proto.Txn{
			Compare: req.Compare,
			Success: req.Success,
			Failure: req.Failure,
		},
	}

	bytes, err := cmd.MarshalVT()
	if err != nil {
		return nil, err
	}
	res, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	txr := &proto.CommandResult{}
	if err := txr.UnmarshalVT(res.Data); err != nil {
		return nil, err
	}
	return &proto.TxnResponse{
		Succeeded: fsm.UpdateResult(res.Value) == fsm.ResultSuccess,
		Responses: txr.Responses,
		Header:    &proto.ResponseHeader{Revision: txr.Revision},
	}, nil
}

func isReadonlyTransaction(req *proto.TxnRequest) bool {
	for _, op := range req.Success {
		if _, ok := op.Request.(*proto.RequestOp_RequestRange); !ok {
			return false
		}
	}

	for _, op := range req.Failure {
		if _, ok := op.Request.(*proto.RequestOp_RequestRange); !ok {
			return false
		}
	}
	return true
}

// Snapshot streams snapshot to the provided writer.
func (t *ActiveTable) Snapshot(ctx context.Context, writer io.Writer) (*fsm.SnapshotResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, fsm.SnapshotRequest{Writer: writer, Stopper: ctx.Done()})
	if err != nil {
		return nil, err
	}
	return val.(*fsm.SnapshotResponse), nil
}

// LocalIndex returns local index.
func (t *ActiveTable) LocalIndex(ctx context.Context) (*fsm.IndexResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, fsm.LocalIndexRequest{})
	if err != nil {
		return nil, err
	}
	return val.(*fsm.IndexResponse), nil
}

// LeaderIndex returns leader index.
func (t *ActiveTable) LeaderIndex(ctx context.Context) (*fsm.IndexResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, fsm.LeaderIndexRequest{})
	if err != nil {
		return nil, err
	}
	return val.(*fsm.IndexResponse), nil
}

// Reset resets the leader index to 0.
func (t *ActiveTable) Reset(ctx context.Context) error {
	li := uint64(0)
	cmd := &proto.Command{
		Type:        proto.Command_DUMMY,
		Table:       []byte(t.Name),
		LeaderIndex: &li,
	}
	bts, err := cmd.MarshalVT()
	if err != nil {
		return err
	}
	_, err = t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bts)
	return err
}
