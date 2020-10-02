# Regatta
![logo](logo.jpg)

Regatta is a read-optimised distributed key-value store.

## Development environment prerequisites
* [Go](https://golang.org/) >= 1.15 -- `brew install go`
* Protocol Buffer compiler >= 3 -- `brew install protobuf`
* Go protobuf compiler -- `go install github.com/golang/protobuf/protoc-gen-go`
* Go gRPC gateway compiler plugin -- `go install github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway`
* Go gRPC gateway swagger plugin  -- `go install github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger`
* gRPC curl (optional for testing) -- `brew install grpcurl`
* Docker (optional)

## Other links
* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [gRPC REST Gateway](https://github.com/grpc-ecosystem/grpc-gateway)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)


## Run server
```bash
make run
```

# Run client

## gRPC
```bash
make run-client
```
or
```bash
$ grpcurl -insecure 127.0.0.1:8443 list

$ echo -n 'table_1' | base64
dGFibGVfMQ==

$ echo -n 'key_1' | base64
a2V5XzE=

$ grpcurl -insecure -d='{"table": "dGFibGVfMQ==", "key": "a2V5XzE="}' 127.0.0.1:8443 regatta.v1.KV/Range
{"kvs":[{"key":"a2V5XzE=","value":"dGFibGVfMXZhbHVlXzE="}],"count":"1"}
$ echo "dGFibGVfMXZhbHVlXzE=" | base64 -d
table_1value_1
```

## REST
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