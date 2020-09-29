package regattaserver

import (
	"context"
	"crypto/tls"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// MaintenanceServer implements Maintenance service from proto/regatta.proto.
type MaintenanceServer struct {
	proto.UnimplementedMaintenanceServer
	Storage *storage.SimpleStorage
}

// Register creates Maintenance server and registers it to regatta server.
func (s *MaintenanceServer) Register(regatta *RegattaServer) error {
	proto.RegisterMaintenanceServer(regatta.GrpcServer, s)

	opts := []grpc.DialOption{
		// we do not need to check certificate between grpc-gateway and grpc server internally
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})),
	}

	err := proto.RegisterMaintenanceHandlerFromEndpoint(context.Background(), regatta.GWMux, regatta.Addr, opts)
	if err != nil {
		zap.S().Errorf("Cannot register handler: %v", err)
		return err
	}
	return nil
}

// Reset implements proto/regatta.proto Maintenance.Reset method.
func (s *MaintenanceServer) Reset(_ context.Context, req *proto.ResetRequest) (*proto.ResetResponse, error) {
	s.Storage.Reset()
	return &proto.ResetResponse{}, nil
}
