---
title: Contributing
layout: default
nav_order: 9
---

# Contributing

Regatta is still rough around the edges and there are A LOT of things left to be implemented.
If you like this project and would like to contribute, ask questions, or just reach out, we encourage you to do so!
**Feel free to reach out in [GitHub discussions](https://github.com/jamf/regatta/discussions),
raise an issue, or open a pull request**.

## Development prerequisites

* [Go](https://golang.org/) >= 1.20
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

## Running Documentation Site Locally

This documentation site is powered by [Jekyll](https://jekyllrb.com).
First, install Jekyll and [bundler](https://bundler.io):

```bash
gem install bundler jekyll
```

Then, install the necessary gems:

```bash
cd ./docs
bundle install
```

Last, run Jekyll in the root of the repository:

```bash
make serve-docs
```

## Useful links

* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)
* [Raft algorithm](https://raft.github.io)
* [Pebble](https://github.com/cockroachdb/pebble)

