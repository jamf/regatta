package table

import (
	"context"
	"encoding/binary"
	"hash/fnv"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/client"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	pb "google.golang.org/protobuf/proto"
)

type raftHandler interface {
	SyncRead(ctx context.Context, id uint64, req interface{}) (interface{}, error)
	StaleRead(id uint64, req interface{}) (interface{}, error)
	SyncPropose(ctx context.Context, session *client.Session, bytes []byte) (sm.Result, error)
	GetNoOPSession(id uint64) *client.Session
}

type Table struct {
	Name      string `json:"name"`
	ClusterID uint64 `json:"cluster_id"`
}

func (t Table) AsActive(host raftHandler) *ActiveTable {
	return &ActiveTable{nh: host, Table: t}
}

type ActiveTable struct {
	Table
	nh raftHandler
}

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
		if err != pebble.ErrNotFound {
			return nil, err
		}
		return nil, storage.ErrNotFound
	}
	response := val.(*proto.RangeResponse)
	return response, nil
}

func (t *ActiveTable) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, storage.ErrEmptyKey
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
	_, err = t.nh.SyncPropose(ctx, t.nh.GetNoOPSession(t.ClusterID), bytes)
	if err != nil {
		return nil, err
	}
	return &proto.PutResponse{}, nil
}

func (t *ActiveTable) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if len(req.Key) == 0 {
		return nil, storage.ErrEmptyKey
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

func (t *ActiveTable) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	panic("implement me")
}

func (t *ActiveTable) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	h64 := fnv.New64()
	val, err := t.nh.SyncRead(ctx, t.ClusterID, req)
	if err != nil {
		return nil, err
	}
	err = binary.Write(h64, binary.LittleEndian, val.(*proto.HashResponse).Hash)
	if err != nil {
		return nil, err
	}
	return &proto.HashResponse{Hash: h64.Sum64()}, nil
}
