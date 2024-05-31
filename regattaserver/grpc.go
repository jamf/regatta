// Copyright JAMF Software, LLC

package regattaserver

import (
	"net"
	"time"

	_ "github.com/jamf/regatta/regattaserver/encoding/gzip"
	_ "github.com/jamf/regatta/regattaserver/encoding/proto"
	_ "github.com/jamf/regatta/regattaserver/encoding/snappy"
	_ "github.com/jamf/regatta/regattaserver/encoding/zstd"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// defaultOpts default GRPC server options
// * Allow for earlier keepalives.
// * Allow keepalives without stream.
// * Allow for handlers to return before shutting down.
var defaultOpts = []grpc.ServerOption{
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second, PermitWithoutStream: true}),
	grpc.WaitForHandlers(true),
}

// RegattaServer is server where gRPC services can be registered in.
type RegattaServer struct {
	*grpc.Server
	listener net.Listener
	log      *zap.SugaredLogger
}

// NewServer returns initialized gRPC server.
func NewServer(l net.Listener, logger *zap.SugaredLogger, opts ...grpc.ServerOption) *RegattaServer {
	rs := new(RegattaServer)
	rs.listener = l
	rs.log = logger
	rs.Server = grpc.NewServer(append(defaultOpts, opts...)...)
	reflection.Register(rs.Server)
	return rs
}

func (s *RegattaServer) Addr() net.Addr {
	return s.listener.Addr()
}

// Serve starts underlying gRPC server.
func (s *RegattaServer) Serve() error {
	s.log.Infof("serve gRPC on: %s", s.listener.Addr())
	return s.Server.Serve(s.listener)
}

// Shutdown stops underlying gRPC server.
func (s *RegattaServer) Shutdown() {
	s.log.Infof("stopping gRPC on: %s", s.Addr())
	s.Server.GracefulStop()
	s.Server.Stop()
	s.log.Infof("stopped gRPC on: %s", s.Addr())
}
