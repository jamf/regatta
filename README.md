# Regatta

![logo](logo.jpg)

Regatta is a read-optimised distributed key-value store.

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

## Other links

* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)

## Usage

* For CLI usage check [cli docs](./docs/cli/regatta.md).
* For API documentation check [api docs](./docs/api.md).
* For detailed API usage check [usage docs](./docs/usage.md).

## Running regatta locally

### Single replica

The following command will run regatta locally in the single replica:

```bash
$ make run
```

### Kind

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

Run regatta:

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

## Reset cluster data

1. Downscale the regatta statefulset to 0 replicas manually\
   `$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 0 regatta`
2. Delete persistent volume claims in the cluster:\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-0`\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-1`\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-2`
3. Reset kafka offset for given consumer group ID (The consumer group ID is defined in `charts-<env>.yaml` file
   in `.Values.kafka.brokers`.)
    1. SSH to kafka node (Kafka broker configuration is defined in `charts-<env>.yaml` file in `.Values.kafka.groupID`.)
    2. List the topics to which the group is subscribed (use noSSL port 8091)\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
    3. Reset all topics the consumer group subsribes to (run without `--execute` flag for dry run)\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --reset-offsets --all-topics --to-earliest --execute`
    4. Verify the offset was reset\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
4. Upscale the regatta statefulset to 3 replicas manually\
   `$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 3 regatta`
