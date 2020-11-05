package storage

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/raft"
	pb "google.golang.org/protobuf/proto"
)

func NewRaft(nh *dragonboat.NodeHost, partitioner Partitioner, metadata *raft.Metadata) KVStorage {
	return &raftKV{
		nh:          nh,
		meta:        metadata,
		partitioner: partitioner,
		sessionPool: struct {
			sessions map[uint64]*client.Session
			mtx      sync.RWMutex
		}{
			sessions: make(map[uint64]*client.Session),
		},
	}
}

type raftKV struct {
	nh          *dragonboat.NodeHost
	meta        *raft.Metadata
	partitioner Partitioner
	sessionPool struct {
		sessions map[uint64]*client.Session
		mtx      sync.RWMutex
	}
}

func (r *raftKV) sessionForKey(key []byte) *client.Session {
	id := r.partitioner.ClusterID(key)
	r.sessionPool.mtx.RLock()
	session, ok := r.sessionPool.sessions[id]
	r.sessionPool.mtx.RUnlock()
	if ok {
		return session
	}

	session = r.nh.GetNoOPSession(id)
	r.sessionPool.mtx.Lock()
	defer r.sessionPool.mtx.Unlock()
	r.sessionPool.sessions[id] = session
	return session
}

func (r *raftKV) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if len(req.Table) == 0 {
		return nil, ErrEmptyTable
	}
	if len(req.Key) == 0 {
		return nil, ErrEmptyKey
	}

	dc, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()

	id := r.partitioner.ClusterID(req.Key)
	var (
		val interface{}
		err error
	)
	if req.Linearizable {
		val, err = r.nh.SyncRead(dc, id, req)
	} else {
		val, err = r.nh.StaleRead(id, req)
	}

	if err != nil {
		if err != pebble.ErrNotFound {
			return nil, err
		}
		return nil, ErrNotFound
	}
	response := val.(*proto.RangeResponse)
	response.Header = raftHeader(r.meta, id)
	return response, nil
}

func (r *raftKV) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
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
	sess := r.sessionForKey(req.Key)
	_, err = r.nh.SyncPropose(dc, sess, bytes)
	if err != nil {
		return nil, err
	}
	return &proto.PutResponse{Header: raftHeader(r.meta, sess.ClusterID)}, nil
}

func (r *raftKV) Delete(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
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
	sess := r.sessionForKey(req.Key)
	res, err := r.nh.SyncPropose(dc, sess, bytes)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteRangeResponse{
		Header:  raftHeader(r.meta, sess.ClusterID),
		Deleted: int64(res.Value),
	}, nil
}

func (r *raftKV) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	panic("not implemented")
}

func (r *raftKV) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	h64 := fnv.New64()
	for clusterID := uint64(1); clusterID <= r.partitioner.Capacity(); clusterID++ {
		val, err := r.nh.StaleRead(clusterID, req)
		if err != nil {
			return nil, err
		}
		err = binary.Write(h64, binary.LittleEndian, val.(*proto.HashResponse).Hash)
		if err != nil {
			return nil, err
		}
	}
	return &proto.HashResponse{Hash: h64.Sum64()}, nil
}

func raftHeader(nh *raft.Metadata, clusterID uint64) *proto.ResponseHeader {
	v := nh.Get(clusterID)
	return &proto.ResponseHeader{
		ClusterId:    clusterID,
		MemberId:     v.NodeID,
		RaftLeaderId: v.LeaderID,
		RaftTerm:     v.Term,
	}
}
