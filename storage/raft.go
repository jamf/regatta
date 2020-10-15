package storage

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble"
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
	if len(req.Table) == 0 {
		return nil, ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return nil, ErrEmptyKey
	}

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()

	var (
		val interface{}
		err error
	)
	if req.Linearizable {
		val, err = r.SyncRead(dc, r.Session.ClusterID, req)
	} else {
		val, err = r.StaleRead(r.Session.ClusterID, req)
	}

	if err != nil {
		if err != pebble.ErrNotFound {
			return nil, err
		}
		return nil, ErrNotFound
	}
	response := val.(*proto.RangeResponse)
	response.Header = raftHeader(r.NodeHost, r.Session.ClusterID)
	return response, nil
}

func (r *Raft) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if len(req.Table) == 0 {
		return nil, ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return nil, ErrEmptyKey
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

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()
	// TODO Query previous value
	_, err = r.SyncPropose(dc, r.Session, bytes)
	if err != nil {
		return nil, err
	}
	return &proto.PutResponse{Header: raftHeader(r.NodeHost, r.Session.ClusterID)}, nil
}

func (r *Raft) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if len(req.Table) == 0 {
		return nil, ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return nil, ErrEmptyKey
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

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()
	res, err := r.SyncPropose(dc, r.Session, bytes)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteRangeResponse{
		Header:  raftHeader(r.NodeHost, r.Session.ClusterID),
		Deleted: int64(res.Value),
	}, nil
}

func (r *Raft) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	panic("not implemented")
}

func (r *Raft) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	val, err := r.StaleRead(r.Session.ClusterID, req)
	if err != nil {
		return nil, err
	}
	response := val.(*proto.HashResponse)
	response.Header = raftHeader(r.NodeHost, r.Session.ClusterID)
	return response, nil
}

func raftHeader(nh *dragonboat.NodeHost, clusterID uint64) *proto.ResponseHeader {
	nhi := nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
	cil := dragonboat.ClusterInfo{}
	if len(nhi.ClusterInfoList) > 0 {
		cil = nhi.ClusterInfoList[0]
	}
	leaderID, _, _ := nh.GetLeaderID(clusterID)
	return &proto.ResponseHeader{
		ClusterId:    cil.ClusterID,
		MemberId:     cil.NodeID,
		RaftLeaderId: leaderID,
	}
}
