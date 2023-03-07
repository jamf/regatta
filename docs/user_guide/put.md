---
title: Updating Records
layout: default
parent: User Guide
nav_order: 2
---

# Updating Records

See [Put Request API](../api/#regatta-v1-PutRequest) and [Put Response API](../api#regatta-v1-PutResponse)
for the complete gRPC API documentation for inserting and updating records from Regatta.

Put API serves for inserting and modifying records in Regatta.

To insert a record into a table, a `key`, a `value`, and a `table` fields must be set.

```bash
grpcurl -insecure "-d={
        \"table\": \"$(echo -n "regatta-test" | base64)\",
        \"key\": \"$(echo -n "key_1" | base64)\",
        \"value\": \"$(echo -n "value" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/Put
```

Expected response:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "3",
    "raftTerm": "2",
    "raftLeaderId": "1"
  }
}
```

Additionaly, the field `prev_kv` set to `true` can be supplied. Regatta then responds with
the previous key-value pair upserted with the request, if any exists.

```bash
grpcurl -insecure "-d={
        \"table\": \"$(echo -n "regatta-test" | base64)\",
        \"key\": \"$(echo -n "key_1" | base64)\",
        \"value\": \"$(echo -n "value" | base64)\",
        \"prev_kv\": true
    }" 127.0.0.1:8443 regatta.v1.KV/Put
```

Expected response if the record with key `key_1` previously existed
and `prev_kv` was set to true:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "4",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "prevKv": {
    "key": "a2V5XzE=",
    "value": "dmFsdWU="
  }
}
```
