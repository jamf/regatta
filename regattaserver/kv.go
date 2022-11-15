// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"

	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/storage/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer implements KV service from proto/regatta.proto.
type KVServer struct {
	proto.UnimplementedKVServer
	Storage KVService
}

// Range implements proto/regatta.proto KV.Range method.
// Currently, only subset of functionality is implemented.
// The versioning functionality is not available.
func (s *KVServer) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if req.GetLimit() < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be a positive number")
	} else if req.GetKeysOnly() && req.GetCountOnly() {
		return nil, status.Errorf(codes.InvalidArgument, "keys_only and count_only must not be set at the same time")
	} else if req.GetMinModRevision() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "min_mod_revision not implemented")
	} else if req.GetMaxModRevision() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "max_mod_revision not implemented")
	} else if req.GetMinCreateRevision() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "min_create_revision not implemented")
	} else if req.GetMaxCreateRevision() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "max_create_revision not implemented")
	}

	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	val, err := s.Storage.Range(ctx, req)
	if err != nil {
		if err == errors.ErrTableNotFound {
			return nil, status.Errorf(codes.NotFound, "table not found")
		} else if err == errors.ErrKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return val, nil
}

// Put implements proto/regatta.proto KV.Put method.
func (s *KVServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	r, err := s.Storage.Put(ctx, req)
	if err != nil {
		if err == errors.ErrTableNotFound {
			return nil, status.Errorf(codes.NotFound, "table not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return r, nil
}

// DeleteRange implements proto/regatta.proto KV.DeleteRange method.
func (s *KVServer) DeleteRange(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	r, err := s.Storage.Delete(ctx, req)
	if err != nil {
		if err == errors.ErrTableNotFound {
			return nil, status.Errorf(codes.NotFound, "table not found")
		} else if err == errors.ErrKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return r, nil
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is allowed to modify the same key several times within one txn (the result will be the last Op that modified the key).
func (s *KVServer) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	r, err := s.Storage.Txn(ctx, req)
	if err != nil {
		if err == errors.ErrTableNotFound {
			return nil, status.Errorf(codes.NotFound, "table not found")
		} else if err == errors.ErrKeyNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return r, nil
}

// ReadonlyKVServer implements read part of KV service from proto/regatta.proto.
type ReadonlyKVServer struct {
	KVServer
}

// Put implements proto/regatta.proto KV.Put method.
func (r *ReadonlyKVServer) Put(_ context.Context, _ *proto.PutRequest) (*proto.PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented for follower")
}

// DeleteRange implements proto/regatta.proto KV.DeleteRange method.
func (r *ReadonlyKVServer) DeleteRange(_ context.Context, _ *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteRange not implemented for follower")
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is allowed to modify the same key several times within one txn (the result will be the last Op that modified the key).
// Readonly transactions allowed using follower API.
func (r *ReadonlyKVServer) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnResponse, error) {
	if isReadonlyTransaction(req) {
		return r.KVServer.Txn(ctx, req)
	}
	return nil, status.Errorf(codes.Unimplemented, "writable Txn not implemented for follower")
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
