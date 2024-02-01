// Copyright JAMF Software, LLC

package regattaserver

import (
	"net"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	_ "github.com/jamf/regatta/regattaserver/encoding/gzip"
	"github.com/jamf/regatta/regattaserver/encoding/proto"
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
var defaultOpts = []grpc.ServerOption{
	grpc.ForceServerCodec(proto.Codec{}),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second, PermitWithoutStream: true}),
}

// RegattaServer is server where gRPC services can be registered in.
type RegattaServer struct {
	addr       string
	network    string
	grpcServer *grpc.Server
	log        *zap.SugaredLogger
}

// NewServer returns initialized gRPC server.
func NewServer(addr, network string, opts ...grpc.ServerOption) *RegattaServer {
	rs := new(RegattaServer)
	rs.addr = addr
	rs.network = network
	rs.log = zap.S().Named("server")
	rs.grpcServer = grpc.NewServer(append(defaultOpts, opts...)...)
	reflection.Register(rs.grpcServer)
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
	// This should be called after all APIs were already registered
	grpc_prometheus.Register(s.grpcServer)
	return s.grpcServer.Serve(l)
}

// Shutdown stops underlying gRPC server.
func (s *RegattaServer) Shutdown() {
	s.log.Infof("stopping gRPC on: %s", s.addr)
	s.grpcServer.GracefulStop()
	s.log.Infof("stopped gRPC on: %s", s.addr)
}

// RegisterService implements grpc.ServiceRegistrar interface so internals of this type does not need to be exposed.
func (s *RegattaServer) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	s.grpcServer.RegisterService(desc, impl)
}
