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
	addr    string
	network string
	log     *zap.SugaredLogger
}

// NewServer returns initialized gRPC server.
func NewServer(addr, network string, logger *zap.SugaredLogger, opts ...grpc.ServerOption) *RegattaServer {
	rs := new(RegattaServer)
	rs.addr = addr
	rs.network = network
	rs.log = logger
	rs.Server = grpc.NewServer(append(defaultOpts, opts...)...)
	reflection.Register(rs.Server)
	return rs
}

func (s *RegattaServer) Addr() string {
	return s.addr
}

// ListenAndServe starts underlying gRPC server.
func (s *RegattaServer) ListenAndServe() error {
	s.log.Infof("listen gRPC on: %s", s.addr)
	l, err := net.Listen(s.network, s.addr)
	if err != nil {
		return err
	}
	return s.Server.Serve(l)
}

// Shutdown stops underlying gRPC server.
func (s *RegattaServer) Shutdown() {
	s.log.Infof("stopping gRPC on: %s", s.addr)
	s.Server.GracefulStop()
	s.Server.Stop()
	s.log.Infof("stopped gRPC on: %s", s.addr)
}
