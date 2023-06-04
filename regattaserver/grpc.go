// Copyright JAMF Software, LLC

package regattaserver

import (
	"net"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	_ "github.com/jamf/regatta/regattaserver/encoding/gzip"
	_ "github.com/jamf/regatta/regattaserver/encoding/proto"
	_ "github.com/jamf/regatta/regattaserver/encoding/snappy"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// RegattaServer is server where gRPC services can be registered in.
type RegattaServer struct {
	Addr       string
	grpcServer *grpc.Server
	log        *zap.SugaredLogger
}

// NewServer returns initialized gRPC server.
func NewServer(addr string, reflectionAPI bool, opts ...grpc.ServerOption) *RegattaServer {
	rs := new(RegattaServer)
	rs.Addr = addr
	rs.log = zap.S().Named("server")
	rs.grpcServer = grpc.NewServer(opts...)

	if reflectionAPI {
		reflection.Register(rs.grpcServer)
		rs.log.Info("reflection API is active")
	}

	return rs
}

// ListenAndServe starts underlying gRPC server.
func (s *RegattaServer) ListenAndServe() error {
	s.log.Infof("listen gRPC on: %s", s.Addr)
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	// This should be called after all APIs were already registered
	grpc_prometheus.Register(s.grpcServer)
	return s.grpcServer.Serve(l)
}

// Shutdown stops underlying gRPC server.
func (s *RegattaServer) Shutdown() {
	s.log.Infof("stopping gRPC on: %s", s.Addr)
	s.grpcServer.GracefulStop()
	s.log.Infof("stopped gRPC on: %s", s.Addr)
}

// RegisterService implements grpc.ServiceRegistrar interface so internals of this type does not need to be exposed.
func (s *RegattaServer) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	s.grpcServer.RegisterService(desc, impl)
}
