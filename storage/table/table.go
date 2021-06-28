package table

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/client"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/table/key"
	pb "google.golang.org/protobuf/proto"
)

type raftHandler interface {
	SyncRead(ctx context.Context, id uint64, req interface{}) (interface{}, error)
	StaleRead(id uint64, req interface{}) (interface{}, error)
	SyncPropose(ctx context.Context, session *client.Session, bytes []byte) (sm.Result, error)
	GetNoOPSession(id uint64) *client.Session
}

// maxValueLen 2MB max value.
const maxValueLen = 2 * 1024 * 1024

// Table stored representation of a table.
type Table struct {
	Name      string `json:"name"`
	ClusterID uint64 `json:"cluster_id"`
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
	var (
		err error
		val interface{}
	)
	if req.Linearizable {
		val, err = t.nh.SyncRead(ctx, t.ClusterID, req)
	} else {
		val, err = t.nh.StaleRead(t.ClusterID, req)
	}

	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	response := val.(*proto.RangeResponse)
	return response, nil
}

// Put performs a Put proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, storage.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, storage.ErrKeyLengthExceeded
	}
	if len(req.Value) > maxValueLen {
		return nil, storage.ErrValueLengthExceeded
	}
	cmd := &proto.Command{
		Type:  proto.Command_PUT,
		Table: req.Table,
		Kv: &proto.KeyValue{
			Key:   req.Key,
			Value: req.Value,
		},
	}
	bytes, err := pb.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	if _, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes); err != nil {
		return nil, err
	}
	return &proto.PutResponse{}, nil
}

// Delete performs a DeleteRange proposal into the Raft, supplied context must have a deadline set.
func (t *ActiveTable) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if len(req.Key) == 0 {
		return nil, storage.ErrEmptyKey
	}
	if len(req.Key) > key.LatestVersionLen {
		return nil, storage.ErrKeyLengthExceeded
	}
	cmd := &proto.Command{
		Type:  proto.Command_DELETE,
		Table: req.Table,
		Kv: &proto.KeyValue{
			Key: req.Key,
		},
	}
	bytes, err := pb.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	res, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteRangeResponse{Deleted: int64(res.Value)}, nil
}

// Reset not implemented yet (should reset Follower data to fetch them from the Leader again).
func (t *ActiveTable) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	return nil, nil
}

// Hash calculates a fnv hash of a stored data, suitable for tests only.
func (t *ActiveTable) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, req)
	if err != nil {
		return nil, err
	}
	return val.(*proto.HashResponse), nil
}
