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

// MaintenanceServer implements Maintenance service from proto/regatta.proto.
type MaintenanceServer struct {
	proto.UnimplementedMaintenanceServer
	Storage storage.KVStorage
}

// Register creates Maintenance server and registers it to regatta server.
func (s *MaintenanceServer) Register(regatta *RegattaServer) error {
	proto.RegisterMaintenanceServer(regatta, s)

	opts := []grpc.DialOption{
		// we do not need to check certificate between grpc-gateway and grpc server internally
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})),
	}

	err := proto.RegisterMaintenanceHandlerFromEndpoint(regatta.gwContext, regatta.gwMux, regatta.Addr, opts)
	if err != nil {
		zap.S().Errorf("Cannot register handler: %v", err)
		return err
	}
	return nil
}

// Reset implements proto/regatta.proto Maintenance.Reset method.
func (s *MaintenanceServer) Reset(ctx context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	r, err := s.Storage.Reset(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return r, nil
}

// Hash implements proto/regatta.proto Maintenance.Hash method.
func (s *MaintenanceServer) Hash(ctx context.Context, req *proto.HashRequest) (*proto.HashResponse, error) {
	hsh, err := s.Storage.Hash(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return hsh, nil
}
