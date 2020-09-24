# Regatta
![logo](logo.jpg)

Regatta is a distributed key-value store that is deployed in the the core and each EDGE location.

## Development environment prerequisites
* [Go](https://golang.org/) >= 1.15 -- `brew install go`
* Protocol Buffer compiler >= 3 -- `brew install protobuf`
* Go plugin for the protocol compiler -- `GO111MODULE=on go get github.com/golang/protobuf/protoc-gen-go`
* Go plugin for the grpc gateway -- `go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway`
* Go plugin for swagger -- `go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger`
* Docker (optional)

## Other links
* [GRPC in Golang](https://grpc.io/docs/languages/go/)
* [GRPC REST Gateway](https://github.com/grpc-ecosystem/grpc-gateway)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragenboat](https://github.com/lni/dragonboat)


## Run server
```bash
make run
```

# Run client

## grpc
```bash
make run-client
```

## rest
```bash

$ echo -n 'table_1' | base64
dGFibGVfMQ==

$ echo -n 'key_1' | base64
a2V5XzE=

$ curl -d'{"table": "dGFibGVfMQ==", "key": "a2V5XzE="}' -k https://localhost:8443/v1/kv/range
{"kvs":[{"key":"a2V5XzE=","value":"dGFibGVfMXZhbHVlXzE="}],"count":"1"}
$ echo "dGFibGVfMXZhbHVlXzE=" | base64 -d
table_1value_1

$ curl -d'{}' -k https://localhost:8443/v1/maintenance/reset
{}
```