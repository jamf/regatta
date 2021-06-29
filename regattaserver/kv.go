package regattaserver

import (
	"context"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer implements KV service from proto/regatta.proto.
type KVServer struct {
	proto.UnimplementedKVServer
	Storage       storage.KVStorage
	ManagedTables []string
}

// Range implements proto/regatta.proto KV.Range method.
// Currently only subset of functionality is implemented.
// You can get exactly one kv, no versioning, no output configuration.
func (s *KVServer) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if req.GetRangeEnd() != nil {
		return nil, status.Errorf(codes.Unimplemented, "range_end not implemented")
	} else if req.GetLimit() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "limit not implemented")
	} else if req.GetKeysOnly() {
		return nil, status.Errorf(codes.Unimplemented, "keys_only not implemented")
	} else if req.GetCountOnly() {
		return nil, status.Errorf(codes.Unimplemented, "count_only not implemented")
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
		if err == storage.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return val, nil
}

// Put implements proto/regatta.proto KV.Put method.
// Currently only subset of functionality is implemented.
// You cannot get previous value.
func (s *KVServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if req.GetPrevKv() {
		return nil, status.Errorf(codes.Unimplemented, "prev_kv not implemented")
	}

	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	tableString := string(req.GetTable())
	for _, t := range s.ManagedTables {
		if t == tableString {
			return nil, status.Errorf(codes.InvalidArgument, "table is read-only")
		}
	}

	_, err := s.Storage.Put(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &proto.PutResponse{}, nil
}

// DeleteRange implements proto/regatta.proto KV.DeleteRange method.
// Currently only subset of functionality is implemented.
// You can only delete one kv. You cannot get previous values.
func (s *KVServer) DeleteRange(ctx context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if req.GetRangeEnd() != nil {
		return nil, status.Errorf(codes.Unimplemented, "range_end not implemented")
	} else if req.GetPrevKv() {
		return nil, status.Errorf(codes.Unimplemented, "prev_kv not implemented")
	}

	if len(req.GetTable()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "table must be set")
	}

	if len(req.GetKey()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "key must be set")
	}

	tableString := string(req.GetTable())
	for _, t := range s.ManagedTables {
		if t == tableString {
			return nil, status.Errorf(codes.InvalidArgument, "table is read-only")
		}
	}

	r, err := s.Storage.Delete(ctx, req)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return r, nil
}
