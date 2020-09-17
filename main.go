package main

import (
	"context"
	"log"
	"net"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
)

type regattaServer struct {
	proto.UnimplementedRegattaServer
}

func (s *regattaServer) Get(ctx context.Context, key *proto.Key) (*proto.Value, error) {
	return &proto.Value{Value: []byte("12345")}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	proto.RegisterRegattaServer(grpcServer, &regattaServer{})
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
