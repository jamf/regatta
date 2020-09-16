package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	"github.com/wandera/regatta/insecure"
	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type regattaServer struct {
	proto.UnimplementedRegattaServer
}

func newServer() *regattaServer {
	return new(regattaServer)
}

func (s *regattaServer) Get(ctx context.Context, key *proto.Key) (*proto.Value, error) {
	return &proto.Value{Value: key.GetKey()}, nil
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

func main() {
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewClientTLSFromCert(insecure.CertPool, "localhost")),
	}
	grpcServer := grpc.NewServer(opts...)
	proto.RegisterRegattaServer(grpcServer, newServer())
	ctx := context.Background()

	mux := http.NewServeMux()
	gwmux := runtime.NewServeMux()
	dcreds := credentials.NewTLS(&tls.Config{
		ServerName: "localhost",
		RootCAs:    insecure.CertPool,
	})
	dopts := []grpc.DialOption{grpc.WithTransportCredentials(dcreds)}
	err := proto.RegisterRegattaHandlerFromEndpoint(ctx, gwmux, "localhost:443", dopts)
	if err != nil {
		log.Fatalf("serve: %v\n", err)
	}

	mux.Handle("/", gwmux)

	conn, err := net.Listen("tcp", "localhost:443")
	if err != nil {
		panic(err)
	}

	srv := &http.Server{
		Addr:    "localhost:443",
		Handler: grpcHandlerFunc(grpcServer, mux),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{insecure.Cert},
			NextProtos:   []string{"h2"},
		},
	}

	fmt.Printf("grpc/rest on port: %d\n", 443)
	err = srv.Serve(tls.NewListener(conn, srv.TLSConfig))
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
