---
title: Contributing
layout: default
nav_order: 9
---

# Contributing

## Development prerequisites

* [Go](https://golang.org/) >= 1.19
* [Protocol Buffer compiler](https://grpc.io/docs/protoc-installation/) >= 3
* [Go Protocol Bufffer compiler plugin](https://pkg.go.dev/github.com/golang/protobuf/protoc-gen-go)
  -- `go get -d google.golang.org/protobuf/cmd/protoc-gen-go`
* [Go Vitess Protocol Buffers compiler](https://github.com/planetscale/vtprotobuf/)
  -- `go get -d github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto`
* [Go gRPC generator Protocol Buffer compiler plugin](https://pkg.go.dev/google.golang.org/grpc/cmd/protoc-gen-go-grpc)
  -- `go get -d google.golang.org/grpc/cmd/protoc-gen-go-grpc`
* [Go documentation Protocol Buffer compiler plugin](https://github.com/pseudomuto/protoc-gen-doc)
  -- `go get -d github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc`

## Testing prerequisites

* [Docker](https://www.docker.com)
* [kind](https://kind.sigs.k8s.io/)
* [gRPC curl](https://github.com/fullstorydev/grpcurl)

## Useful links

* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)
* [Raft algorithm](https://raft.github.io)
* [Pebble](https://github.com/cockroachdb/pebble)


