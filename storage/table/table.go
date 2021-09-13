package table

import (
	"context"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/client"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/table/key"
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
		return nil, storage.ErrKeyLengthExceeded
	}
	if len(req.RangeEnd) > key.LatestVersionLen {
		return nil, storage.ErrKeyLengthExceeded
	}
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
	if len(req.Value) > MaxValueLen {
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
	bytes, err := cmd.MarshalVT()
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
	bytes, err := cmd.MarshalVT()
	if err != nil {
		return nil, err
	}

	res, err := t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteRangeResponse{Deleted: int64(res.Value)}, nil
}

// Snapshot streams snapshot to the provided writer.
func (t *ActiveTable) Snapshot(ctx context.Context, writer io.Writer) (*SnapshotResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, SnapshotRequest{Writer: writer, Stopper: ctx.Done()})
	if err != nil {
		return nil, err
	}
	return val.(*SnapshotResponse), nil
}

// LocalIndex returns local index.
func (t *ActiveTable) LocalIndex(ctx context.Context) (*IndexResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, LocalIndexRequest{})
	if err != nil {
		return nil, err
	}
	return val.(*IndexResponse), nil
}

// LeaderIndex returns leader index.
func (t *ActiveTable) LeaderIndex(ctx context.Context) (*IndexResponse, error) {
	val, err := t.nh.SyncRead(ctx, t.ClusterID, LeaderIndexRequest{})
	if err != nil {
		return nil, err
	}
	return val.(*IndexResponse), nil
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

// SnapshotRequest to write Command snapshot into provided writer.
type SnapshotRequest struct {
	Writer  io.Writer
	Stopper <-chan struct{}
}

// SnapshotResponse returns local index to which the snapshot was created.
type SnapshotResponse struct {
	Index uint64
}

// LocalIndexRequest to read local index.
type LocalIndexRequest struct{}

// LeaderIndexRequest to read leader index.
type LeaderIndexRequest struct{}

// IndexResponse returns local index.
type IndexResponse struct {
	Index uint64
}

// PathRequest request data disk paths.
type PathRequest struct{}

// PathResponse returns SM data paths.
type PathResponse struct {
	Path    string
	WALPath string
}
