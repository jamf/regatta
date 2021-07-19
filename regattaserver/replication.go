package regattaserver

import (
	"context"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetadataServer implements Metadata service from proto/replication.proto.
type MetadataServer struct {
	proto.UnimplementedMetadataServer
	Manager *tables.Manager
}

func (m *MetadataServer) Get(context.Context, *proto.MetadataRequest) (*proto.MetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
