package regattaserver

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// RegattaServer is server where grpc/http services can be registered in
type RegattaServer struct {
	Addr       string
	GrpcServer *grpc.Server
	httpServer *http.Server
	GWMux      *gwruntime.ServeMux
}

// NewServer returns initialized grpc/http server
func NewServer(
	hostname string, port int, certFilename string, keyFilename string,
) *RegattaServer {
	rs := new(RegattaServer)
	rs.Addr = fmt.Sprintf("%s:%d", hostname, port)

	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewServerTLSFromFile(certFilename, keyFilename); err != nil {
		log.Fatalf("Cannot create server credentials: %v", err)
	}
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
	}
	rs.GrpcServer = grpc.NewServer(opts...)

	mux := http.NewServeMux()
	rs.GWMux = gwruntime.NewServeMux()

	mux.Handle("/", rs.GWMux)

	cert, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		log.Fatalln("Failed to parse key pair:", err)
	}

	rs.httpServer = &http.Server{
		Addr:    rs.Addr,
		Handler: grpcHandlerFunc(rs.GrpcServer, mux),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		},
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

// ListenAndServe starts underlying http server
func (s *RegattaServer) ListenAndServe() error {
	log.Printf("grpc/rest on: %s\n", s.Addr)
	return s.httpServer.ListenAndServeTLS("", "")
}
