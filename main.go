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
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewClientTLSFromCert(insecure.CertPool, "localhost")),
	}
	grpcServer := grpc.NewServer(opts...)
	proto.RegisterKVServer(grpcServer, newServer())
	ctx := context.Background()

	mux := http.NewServeMux()
	gwmux := runtime.NewServeMux()
	dcreds := credentials.NewTLS(&tls.Config{
		ServerName: "localhost",
		RootCAs:    insecure.CertPool,
	})
	dopts := []grpc.DialOption{grpc.WithTransportCredentials(dcreds)}
	err := proto.RegisterKVHandlerFromEndpoint(ctx, gwmux, "localhost:443", dopts)
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
