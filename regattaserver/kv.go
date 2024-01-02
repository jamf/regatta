// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"errors"

	"github.com/jamf/regatta/regattapb"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/util/iter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer implements KV service from proto/regatta.proto.
type KVServer struct {
	regattapb.UnimplementedKVServer
	Storage KVService
}

// Range implements proto/regatta.proto KV.Range method.
// Currently, only subset of functionality is implemented.
// The versioning functionality is not available.
func (s *KVServer) Range(ctx context.Context, req *regattapb.RangeRequest) (*regattapb.RangeResponse, error) {
	if req.GetLimit() < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be a positive number")
	} else if req.GetKeysOnly() && req.GetCountOnly() {
		return nil, status.Error(codes.InvalidArgument, "keys_only and count_only must not be set at the same time")
	} else if req.GetMinModRevision() > 0 {
		return nil, status.Error(codes.Unimplemented, "min_mod_revision not implemented")
	} else if req.GetMaxModRevision() > 0 {
		return nil, status.Error(codes.Unimplemented, "max_mod_revision not implemented")
	} else if req.GetMinCreateRevision() > 0 {
		return nil, status.Error(codes.Unimplemented, "min_create_revision not implemented")
	} else if req.GetMaxCreateRevision() > 0 {
		return nil, status.Error(codes.Unimplemented, "max_create_revision not implemented")
	}

	if len(req.GetTable()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must be set")
	}

	val, err := s.Storage.Range(ctx, req)
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return nil, status.Error(codes.NotFound, "table not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return val, nil
}

// IterateRange gets the keys in the range from the key-value store.
func (s *KVServer) IterateRange(req *regattapb.RangeRequest, srv regattapb.KV_IterateRangeServer) error {
	if req.GetLimit() < 0 {
		return status.Errorf(codes.InvalidArgument, "limit must be a positive number")
	} else if req.GetKeysOnly() && req.GetCountOnly() {
		return status.Error(codes.InvalidArgument, "keys_only and count_only must not be set at the same time")
	} else if req.GetMinModRevision() > 0 {
		return status.Error(codes.Unimplemented, "min_mod_revision not implemented")
	} else if req.GetMaxModRevision() > 0 {
		return status.Error(codes.Unimplemented, "max_mod_revision not implemented")
	} else if req.GetMinCreateRevision() > 0 {
		return status.Error(codes.Unimplemented, "min_create_revision not implemented")
	} else if req.GetMaxCreateRevision() > 0 {
		return status.Error(codes.Unimplemented, "max_create_revision not implemented")
	}

	if len(req.GetTable()) == 0 {
		return status.Error(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return status.Error(codes.InvalidArgument, "key must be set")
	}

	ctx := srv.Context()
	r, err := s.Storage.IterateRange(ctx, req)
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return status.Error(codes.NotFound, "table not found")
		}
		return status.Error(codes.Internal, err.Error())
	}
	pull, stop := iter.Pull(r)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			response, valid := pull()
			if !valid {
				return nil
			}
			if err := srv.Send(response); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		}
	}
}

// Put implements proto/regatta.proto KV.Put method.
func (s *KVServer) Put(ctx context.Context, req *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	r, err := s.Storage.Put(ctx, req)
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return nil, status.Error(codes.NotFound, "table not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return r, nil
}

// DeleteRange implements proto/regatta.proto KV.DeleteRange method.
func (s *KVServer) DeleteRange(ctx context.Context, req *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	r, err := s.Storage.Delete(ctx, req)
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return nil, status.Error(codes.NotFound, "table not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return r, nil
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is allowed to modify the same key several times within one txn (the result will be the last Op that modified the key).
func (s *KVServer) Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	r, err := s.Storage.Txn(ctx, req)
	if err != nil {
		if errors.Is(err, serrors.ErrTableNotFound) {
			return nil, status.Error(codes.NotFound, "table not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return r, nil
}

// ReadonlyKVServer implements read part of KV service from proto/regatta.proto.
type ReadonlyKVServer struct {
	KVServer
}

// Put implements proto/regatta.proto KV.Put method.
func (r *ReadonlyKVServer) Put(_ context.Context, _ *regattapb.PutRequest) (*regattapb.PutResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Put not implemented for follower")
}

// DeleteRange implements proto/regatta.proto KV.DeleteRange method.
func (r *ReadonlyKVServer) DeleteRange(_ context.Context, _ *regattapb.DeleteRangeRequest) (*regattapb.DeleteRangeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteRange not implemented for follower")
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is allowed to modify the same key several times within one txn (the result will be the last Op that modified the key).
// Readonly transactions allowed using follower API.
func (r *ReadonlyKVServer) Txn(ctx context.Context, req *regattapb.TxnRequest) (*regattapb.TxnResponse, error) {
	if req.IsReadonly() {
		return r.KVServer.Txn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "writable Txn not implemented for follower")
}
