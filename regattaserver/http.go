package regattaserver

import (
	"crypto/tls"
	"net/http"
	"strings"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

// RegattaServer is server where grpc/http services can be registered in.
type RegattaServer struct {
	Addr       string
	GrpcServer *grpc.Server
	httpServer *http.Server
	GWMux      *gwruntime.ServeMux
}

// NewServer returns initialized grpc/http server.
func NewServer(
	addr string, certFilename string, keyFilename string, reflectionAPI bool,
) *RegattaServer {
	rs := new(RegattaServer)
	rs.Addr = addr

	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewServerTLSFromFile(certFilename, keyFilename); err != nil {
		zap.S().Fatalf("Cannot create server credentials: %v", err)
	}
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
	}
	rs.GrpcServer = grpc.NewServer(opts...)

	if reflectionAPI {
		reflection.Register(rs.GrpcServer)
		zap.S().Warn("Reflection API is active")
	}

	mux := http.NewServeMux()
	rs.GWMux = gwruntime.NewServeMux()

	mux.Handle("/", rs.GWMux)

	cert, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		zap.S().Fatalf("Failed to parse key pair:", err)
	}

	rs.httpServer = &http.Server{
		Addr:    rs.Addr,
		Handler: grpcHandlerFunc(rs.GrpcServer, mux),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		},
		ErrorLog: zap.NewStdLog(zap.L()),
	}

	return rs
}

// grpcHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}

// ListenAndServe starts underlying http server.
func (s *RegattaServer) ListenAndServe() error {
	zap.S().Infof("grpc/rest on: %s", s.Addr)
	return s.httpServer.ListenAndServeTLS("", "")
}
