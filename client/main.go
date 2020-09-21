// Package main implements a client for regatta service.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/wandera/regatta/insecure"
	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	address    = "localhost:443"
	defaultKey = "key"
)

func main() {
	// Set up a connection to the server.
	var opts []grpc.DialOption

	creds := credentials.NewClientTLSFromCert(insecure.CertPool, "localhost")
	opts = append(opts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewRegattaClient(conn)

	// Contact the server and print out its response.
	key := defaultKey
	if len(os.Args) > 1 {
		key = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Get(ctx, &proto.Key{Key: []byte(key)})
	if err != nil {
		log.Fatalf("could not get value: %v", err)
	}
	log.Printf("Value: %s", r.GetValue())
}
