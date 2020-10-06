package regattaserver

import (
	"context"
	"crypto/tls"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// KVServer implements KV service from proto/regatta.proto.
type KVServer struct {
	proto.UnimplementedKVServer
	Storage storage.KVStorage
}

// Register creates KV server and registers it to regatta server.
func (s *KVServer) Register(regatta *RegattaServer) error {
	proto.RegisterKVServer(regatta.GrpcServer, s)

	opts := []grpc.DialOption{
		// we do not need to check certificate between grpc-gateway and grpc server internally
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})),
	}

	err := proto.RegisterKVHandlerFromEndpoint(context.Background(), regatta.GWMux, regatta.Addr, opts)
	if err != nil {
		zap.S().Errorf("Cannot register handler: %v", err)
		return err
	}
	return nil
}

// Range implements proto/regatta.proto KV.Range method.
// Currently only subset of functionality is implemented.
// You can get exactly one kv, no versioning, no output configuration.
func (s *KVServer) Range(ctx context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if req.GetRangeEnd() != nil {
		return nil, status.Errorf(codes.Unimplemented, "range_end not implemented")
	} else if req.GetLimit() > 0 {
		return nil, status.Errorf(codes.Unimplemented, "limit not implemented")
	} else if req.GetLinearizable() {
		return nil, status.Errorf(codes.Unimplemented, "linearizable not implemented")
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
	return &proto.RangeResponse{
		Kvs: []*proto.KeyValue{
			{
				Key:   req.Key,
				Value: val,
			},
		},
		Count: 1,
	}, nil
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

	r, err := s.Storage.Delete(ctx, req)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &proto.DeleteRangeResponse{
		Deleted: int64(r.Value),
	}, nil
}
