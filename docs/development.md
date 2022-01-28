# Development

## Development environment prerequisites

* [Go](https://golang.org/) >= 1.17 -- `brew install go`
* Protocol Buffer compiler >= 3 -- `brew install protobuf`
* Go protobuf compiler plugin -- `go get -d google.golang.org/protobuf/cmd/protoc-gen-go`
* Go Vitess Protocol Buffers compiler -- `go get -d github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto`
* Go grpc generator protobuf compiler plugin -- `go get -d google.golang.org/grpc/cmd/protoc-gen-go-grpc`
* Go documentation protobuf compiler plugin -- `go get -d github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc`
* gRPC curl (optional for testing) -- `brew install grpcurl`
* [Docker](https://www.docker.com) (optional for testing)
* [kind](https://kind.sigs.k8s.io/) (optional for testing)

## Running regatta locally

### Single replica leader

The following command will run regatta locally in the single replica:

```bash
$ make run
```

### Single replica follower

The following command will run regatta locally in the single replica follower and connect to already running leader:

```bash
$ make run-follower
```

## Testing

> For API usage check [usage docs](usage.md).

### Testing locally with Kind

Following command will initiate [kind](https://kind.sigs.k8s.io/) cluster with 3 regatta replicas running on it.

```bash
$ make kind
```

### Testing locally with kafka

Run kafka locally using docker-compose:

```bash
$ cd hack/local-kafka
$ docker-compose up
Creating network "local-kafka_default" with the default driver
Creating volume "local-kafka_zookeeper_data" with local driver
Creating volume "local-kafka_kafka_data" with local driver
Creating local-kafka_zookeeper_1 ... done
Creating local-kafka_kafka_1     ... done
Attaching to local-kafka_zookeeper_1, local-kafka_kafka_1
zookeeper_1  | zookeeper 15:00:32.90 
zookeeper_1  | zookeeper 15:00:32.96 Welcome to the Bitnami zookeeper container
zookeeper_1  | zookeeper 15:00:32.96 Subscribe to project updates by watching https://github.com/bitnami/bitnami-docker-zookeeper
zookeeper_1  | zookeeper 15:00:32.96 Submit issues and feature requests at https://github.com/bitnami/bitnami-docker-zookeeper/issues
zookeeper_1  | zookeeper 15:00:32.96 
(...)
```

Create `regatta-test` kafka topic:

```bash
$ docker run --network host --rm bitnami/kafka  /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic regatta-test --bootstrap-server 127.0.0.1:9092
 15:00:53.73 
 15:00:53.74 Welcome to the Bitnami kafka container
 15:00:53.74 Subscribe to project updates by watching https://github.com/bitnami/bitnami-docker-kafka
 15:00:53.74 Submit issues and feature requests at https://github.com/bitnami/bitnami-docker-kafka/issues
 15:00:53.75 

Created topic regatta-test.
```

Run regatta connected to Kafka:

```bash
./regatta --dev-mode --api.reflection-api --raft.address=127.0.0.1:5012 --raft.initial-members='1=127.0.0.1:5012' --kafka.group-id=regatta-test-local --kafka.topics=regatta-test --tables.names=regatta-test
```

Produce message to kafka:

```bash
echo "key_1:val_1" | docker run --rm -i --network host edenhill/kafkacat:1.6.0 -K: -b 127.0.0.1:9092 -t regatta-test
```

Read out from regatta:

```bash
$ grpcurl -insecure "-d={\"table\": \"$(echo -n "regatta-test" | base64)\", \"key\": \"$(echo -n "key_1" | base64)\"}" 127.0.0.1:8443 regatta.v1.KV/Range
{
  "kvs": [
    {
      "key": "a2V5XzE=",
      "value": "dmFsXzE="
    }
  ],
  "count": "1"
}

$ echo -n "dmFsXzE=" | base64 --decode   
val_1
```

## Links

* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)
