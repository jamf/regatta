package regattaserver

import (
	"net"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const maxConnectionAge = 60 * time.Second

// RegattaServer is server where grpc/http services can be registered in.
type RegattaServer struct {
	addr       string
	grpcServer *grpc.Server
	log        *zap.SugaredLogger
}

// NewServer returns initialized grpc/http server.
func NewServer(addr string, certFilename string, keyFilename string, reflectionAPI bool) *RegattaServer {
	rs := new(RegattaServer)
	rs.addr = addr
	rs.log = zap.S().Named("server")

	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewServerTLSFromFile(certFilename, keyFilename); err != nil {
		rs.log.Panicf("cannot create server credentials: %v", err)
	}
	grpc_prometheus.EnableHandlingTimeHistogram()
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: maxConnectionAge,
		}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	}
	rs.grpcServer = grpc.NewServer(opts...)

	if reflectionAPI {
		reflection.Register(rs.grpcServer)
		rs.log.Info("reflection API is active")
	}

	return rs
}

// ListenAndServe starts underlying http server.
func (s *RegattaServer) ListenAndServe() error {
	s.log.Infof("listen gRPC on: %s", s.addr)
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	// This should be called after all APIs were already registered
	grpc_prometheus.Register(s.grpcServer)
	return s.grpcServer.Serve(l)
}

// Shutdown stops underlying http server.
func (s *RegattaServer) Shutdown() {
	s.log.Infof("stopping gRPC on: %s", s.addr)
	s.grpcServer.GracefulStop()
	s.log.Infof("stopped gRPC on: %s", s.addr)
}

// RegisterService implements grpc.ServiceRegistrar interface so internals of this type does not need to be exposed.
func (s *RegattaServer) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	s.grpcServer.RegisterService(desc, impl)
}
