package regattaserver

import (
	"context"
	"crypto/tls"
	"log"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// KVServer implements KV service from proto/regatta.proto
type KVServer struct {
	proto.UnimplementedKVServer
	Storage *storage.SimpleStorage
}

// Register creates KV server and registers it to regatta server
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
		log.Printf("Cannot register handler: %v\n", err)
		return err
	}
	return nil
}

func (s *KVServer) Range(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	if val, err := s.Storage.Get(req.Table, req.Key); err != nil {
		return nil, err
	} else {
		return &proto.RangeResponse{
			Header: nil,
			Kvs: []*proto.KeyValue{
				{
					Key:            req.Key,
					CreateRevision: 0,
					ModRevision:    0,
					Value:          val,
				},
			},
			More:  false,
			Count: 1,
		}, nil
	}
}

func (s *KVServer) Put(_ context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if _, err := s.Storage.Put(req.Table, req.Key, req.Value); err != nil {
		return nil, err
	} else {
		return &proto.PutResponse{
			Header: nil,
			PrevKv: nil,
		}, nil
	}
}

func (s *KVServer) DeleteRange(_ context.Context, req *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if _, err := s.Storage.Delete(req.Table, req.Key); err != nil {
		return nil, err
	} else {
		return &proto.DeleteRangeResponse{
			Header:  nil,
			Deleted: 0,
			PrevKvs: nil,
		}, nil
	}
}
