# Regatta
![logo](logo.jpg)

Regatta is a read-optimised distributed key-value store.

## Limits
* Key limit - 1KB
* Value limit - 2MB
* Single gRPC message limit - 4MB

## Development environment prerequisites
* [Go](https://golang.org/) >= 1.15 -- `brew install go`
* Protocol Buffer compiler >= 3 -- `brew install protobuf`
* Go protobuf compiler plugin -- `go get google.golang.org/protobuf/cmd/protoc-gen-go`
* Go grpc generator protobuf compiler plugin -- `go get google.golang.org/grpc/cmd/protoc-gen-go-grpc`
* gRPC curl (optional for testing) -- `brew install grpcurl`
* [Docker](https://www.docker.com) (optional for testing)
* [kind](https://kind.sigs.k8s.io/) (optional for testing)

## Other links
* [gRPC in Golang](https://grpc.io/docs/languages/go/)
* [Protobuffers in JSON](https://developers.google.com/protocol-buffers/docs/proto3#json)
* [Dragonboat](https://github.com/lni/dragonboat)

## Usage
For CLI usage check [docs](./docs) folder.

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

### Prefix Search

To query key-value pairs with a shared prefix,
supply the `key` and `range_end` fields,
where `key` is the prefix and `range_end` == `key` + 1.
For example, to find all key-value pairs prefixed with the timestamp `1626783802`,
set `key` to `1626783802` and `range_end` to `1626783803`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\",
    \"range_end\": \"$(echo -n "1626783803" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Range Search

To query key-value pairs in a given range, supply the `key` and `range_end` fields.
All pairs whose keys belong to the right-open interval `[key, range_end)` will be returned.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

`range_end` set to `\0` lists every key-value pair with a key greater than or equal to the provided `key`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

`key` set to `\0` lists every key-value pair with a key smaller than the provided `range_end`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "key_1" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### List All Pairs

When `key` and `range_end` are both set to `\0`, then all key-value pairs are returned.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

> [Known issue](https://snappli.atlassian.net/browse/WND-38353):
Regatta may return a gRCP error since the response might be larger than the 4MB gRPC size limit.

### Get Keys Only

List keys in the range `[key, range_end)` with the `keys_only` option.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\",
    \"keys_only\": true}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Get Count Only

Get only the number of keys in the range `[key, range_end)` with the `count_only` option.
This can also be used without the `range_end` field to test the existence of `key`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\",
    \"count_only\": true}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

## Communicating with regatta
### API
Regatta has gRPC API defined in `proto/regatta.proto` and `proto/mvcc.proto`. For the detailed description of individual
methods see the mentioned proto files. Some parts of the API are not implemented yet:
- `KV.Range`
    - not implemented features:
        - paging
        - KV versioning  
    - not implemented fields:
        - `min_mod_revision`, `max_mod_revision`, `min_create_revision`, `max_create_revision`
- `KV.Put`
    - not implemented features:
        - return previous KV
    - not implemented fields:
        - `prev_kv`
- `KV.DeleteRange`
    - not implemented features:
        - range search - regatta can delete only single KV pair
        - return previous KV
    - not implemented fields:
        - `range_end`, `prev_kv`

### Example client binary
The example client is located in `client/main.go`. You can run it by invoking the following command:
```bash
$ make run-client
```

### grpcurl
[gRPCurl](https://github.com/fullstorydev/grpcurl) is universal gRPC client that can be used for testing. 
It has similar interface to cURL. Few examples how to use it with regatta:
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

## Reset cluster data
1. Downscale the regatta statefulset to 0 replicas manually\
`$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 0 regatta`
2. Delete persistent volume claims in the cluster:\
`$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-0`\
`$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-1`\
`$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-2`
3. Reset kafka offset for given consumer group ID (The consumer group ID is defined in `charts-<env>.yaml` file in `.Values.kafka.brokers`.)
   1. SSH to kafka node (Kafka broker configuration is defined in `charts-<env>.yaml` file in `.Values.kafka.groupID`.)
   2. List the topics to which the group is subscribed (use noSSL port 8091)\
    `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
   3. Reset all topics the consumer group subsribes to (run without `--execute` flag for dry run)\
    `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --reset-offsets --all-topics --to-earliest --execute`
   4. Verify the offset was reset\
    `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
4. Upscale the regatta statefulset to 3 replicas manually\
`$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 3 regatta`
