// Package main implements a client for regatta service.
package main

import (
	"context"
	"log"
	"time"

	"github.com/wandera/regatta/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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
	kvc := proto.NewKVClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r1, err := kvc.Range(ctx, &proto.RangeRequest{
		Table: []byte("table_1"),
		Key:   []byte("key_1"),
	})
	if err != nil {
		log.Fatalf("could not get value: %v", err)
	}
	log.Printf("Value: %s", r1.GetKvs())

	r2, err := kvc.Put(ctx, &proto.PutRequest{
		Table: []byte("table_1"),
		Key:   []byte("key_3"),
		Value: []byte("table_1value_3"),
	})
	if err != nil {
		log.Fatalf("could not put value: %v", err)
	}
	log.Printf("Value: %s", r2.GetHeader())

	r1, err = kvc.Range(ctx, &proto.RangeRequest{
		Table: []byte("table_1"),
		Key:   []byte("key_3"),
	})
	if err != nil {
		log.Fatalf("could not get value: %v", err)
	}
	log.Printf("Value: %s", r1.GetKvs())

	mc := proto.NewMaintenanceClient(conn)
	r3, err := mc.Hash(ctx, &proto.HashRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			log.Println("Hash method not implemented")
		} else {
			log.Fatalf("could not get hash: %v", err)
		}
	}
	log.Printf("Value: %s", r3.GetHeader())

	_, _ = mc.Reset(ctx, &proto.ResetRequest{})
	_, err = kvc.Range(ctx, &proto.RangeRequest{
		Table: []byte("table_1"),
		Key:   []byte("key_1"),
	})
	if err != nil {
		log.Printf("could not get value: %v", err)
	}
}
