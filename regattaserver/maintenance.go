package regattaserver

import (
	"context"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaintenanceServer implements Maintenance service from proto/regatta.proto.
type MaintenanceServer struct {
	proto.UnimplementedMaintenanceServer
	Storage KVService
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
