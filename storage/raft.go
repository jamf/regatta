package storage

import (
	"context"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/wandera/regatta/proto"
	pb "google.golang.org/protobuf/proto"
)

type Raft struct {
	*dragonboat.NodeHost
	Session *client.Session
}

func (r *Raft) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()
	val, err := r.SyncRead(dc, r.Session.ClusterID, req)
	if err != nil {
		return nil, err
	}
	return val.(*proto.RangeResponse), nil
}

func (r *Raft) Put(ctx context.Context, req *proto.PutRequest) (Result, error) {
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
		return Result{}, err
	}

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()
	res, err := r.SyncPropose(dc, r.Session, bytes)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Value: res.Value,
		Data:  res.Data,
	}, nil
}

func (r *Raft) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (Result, error) {
	cmd := &proto.Command{
		Type:  proto.Command_DELETE,
		Table: req.Table,
		Kv: &proto.KeyValue{
			Key: req.Key,
		},
	}
	bytes, err := pb.Marshal(cmd)
	if err != nil {
		return Result{}, err
	}

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()
	res, err := r.SyncPropose(dc, r.Session, bytes)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Value: res.Value,
		Data:  res.Data,
	}, nil
}

func (r *Raft) Reset(ctx context.Context, req *proto.ResetRequest) error {
	panic("not implemented")
}

func (r *Raft) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	val, err := r.StaleRead(r.Session.ClusterID, req)
	if err != nil {
		return nil, err
	}
	return val.(*proto.HashResponse), nil
}
