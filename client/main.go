// Package main implements a client for regatta service.
package main

import (
	"context"
	"log"
	"time"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	address        = "localhost:8443"
	caCertFilename = "hack/server.crt"
)

func main() {
	// Set up a connection to the server.
	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewClientTLSFromFile(caCertFilename, ""); err != nil {
		log.Fatalf("Cannot create credentials: %v", err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewKVClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Range(ctx, &proto.RangeRequest{
		Table:             []byte("table"),
		Key:               []byte("key"),
		RangeEnd:          nil,
		Limit:             0,
		Linearizable:      false,
		KeysOnly:          false,
		CountOnly:         false,
		MinModRevision:    0,
		MaxModRevision:    0,
		MinCreateRevision: 0,
		MaxCreateRevision: 0,
	})
	if err != nil {
		log.Fatalf("could not get value: %v", err)
	}
	log.Printf("Value: %s", r.GetKvs())
}
