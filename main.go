package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	domain         = "localhost"
	port           = "8443"
	addr           = domain + ":" + port
	certFilename   = "hack/server.crt"
	keyFilename    = "hack/server.key"
	caCertFilename = "hack/server.crt"
)

type kvServer struct {
	proto.UnimplementedKVServer
}

func newServer() *kvServer {
	return new(kvServer)
}

func (s *kvServer) Range(_ context.Context, req *proto.RangeRequest) (*proto.RangeResponse, error) {
	return &proto.RangeResponse{
		Header: nil,
		Kvs: []*proto.KeyValue{
			{
				Key:            req.Key,
				CreateRevision: 0,
				ModRevision:    0,
				Value:          []byte("abc"),
			},
		},
		More:  false,
		Count: 1,
	}, nil
}

func (*kvServer) Put(context.Context, *proto.PutRequest) (*proto.PutResponse, error) {
	return &proto.PutResponse{
		Header: nil,
		PrevKv: nil,
	}, nil
}
func (*kvServer) DeleteRange(context.Context, *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	return &proto.DeleteRangeResponse{
		Header:  nil,
		Deleted: 0,
		PrevKvs: nil,
	}, nil
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
	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewServerTLSFromFile(certFilename, keyFilename); err != nil {
		log.Fatalf("Cannot create credentials: %v", err)
	}
	sopts := []grpc.ServerOption{
		grpc.Creds(creds),
	}
	grpcServer := grpc.NewServer(sopts...)
	proto.RegisterKVServer(grpcServer, newServer())

	ctx := context.Background()

	mux := http.NewServeMux()
	gwmux := runtime.NewServeMux()
	if creds, err = credentials.NewClientTLSFromFile(caCertFilename, ""); err != nil {
		log.Fatalf("Cannot create credentials: %v", err)
	}
	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	err = proto.RegisterKVHandlerFromEndpoint(ctx, gwmux, addr, dopts)
	if err != nil {
		log.Fatalf("serve: %v\n", err)
	}

	mux.Handle("/", gwmux)

	cert, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		log.Fatalln("Failed to parse key pair:", err)
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: grpcHandlerFunc(grpcServer, mux),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		},
	}

	fmt.Printf("grpc/rest on port: %s\n", port)
	err = srv.ListenAndServeTLS("", "")
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
