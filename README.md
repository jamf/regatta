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

## Usage
```
  regatta [flags]

Flags:
      --api.address string                  Address the API server should listen on. (default "localhost:8443")
      --api.cert-filename string            Path to the API server certificate. (default "hack/server.crt")
      --api.key-filename string             Path to the API server private key file. (default "hack/server.key")
      --api.reflection-api                  Whether reflection API is provided. Should not be turned on in production.
      --dev-mode                            Dev mode enabled (verbose logging, human-friendly log format).
  -h, --help                                help for regatta
      --kafka.brokers strings               Address of the Kafka broker. (default [localhost:9092])
      --kafka.client-cert-filename string   Kafka client certificate.
      --kafka.client-key-filename string    Kafka client key.
      --kafka.group-id string               Kafka consumer group ID. (default "regatta-local")
      --kafka.server-cert-filename string   Kafka broker CA.
      --kafka.timeout duration              Kafka dialer timeout. (default 10s)
      --kafka.tls                           Enables Kafka broker TLS connection.
      --kafka.topics strings                Kafka topics to read from.
      --log-level string                    Log level: DEBUG/INFO/WARN/ERROR. (default "DEBUG")
      --raft.address string                 RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
                                            This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.
      --raft.cluster-id uint                Raft Cluster ID is the unique value used to identify a Raft cluster. (default 1)
      --raft.listen-address string          ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
                                            When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
                                            When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.
      --raft.node-host-dir string           NodeHostDir raft internal storage (default "/tmp/regatta/raft")
      --raft.node-id uint                   Raft Node ID is a non-zero value used to identify a node within a Raft cluster. (default 1)
      --raft.state-machine-dir string       StateMachineDir persistent storage for the state machine. Applicable only when in-memory-state-machine=false. (default "/tmp/regatta/state-machine")
      --raft.state-machine-wal-dir string   StateMachineWalDir persistent storage for the state machine. If empty all state machine data is stored in state-machine-dir. 
                                            Applicable only when in-memory-state-machine=false.
      --raft.wal-dir string                 WALDir is the directory used for storing the WAL of Raft entries. 
                                            It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
                                            Leave WALDir to have zero value will have everything stored in NodeHostDir.
```

## Run server
```bash
make run
```

## Run client

### gRPC
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

### REST
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