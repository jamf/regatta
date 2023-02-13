---
title: Deleting Records
layout: default
parent: User Guide
nav_order: 3
---

# Deleting Records

See [Delete Range Request API](/api/#regatta-v1-DeleteRangeRequest) and [Delete Range Response API](/api/#regatta-v1-DeleteRangeResponse)
for the complete gRPC API documentation for deleting records in Regatta.

Delete Range API serves for deleting records in Regatta, offering exact key-value pair deletion,
deleting pairs with shared prefix, and range deletion.

## Delete single key

To delete a single key-value pair in a table, set the fields `key` and `table`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete keys by prefix

To delete key-value pairs with a shared prefix, supply the `key` and `range_end` fields, where `key` is the prefix
and `range_end = key + 1`. For example, to delete all key-value pairs prefixed with the timestamp `1626783802`,
set `key` to `1626783802` and `range_end` to `1626783803`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\",
    \"range_end\": \"$(echo -n "1626783803" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete keys range

To delete key-value pairs in a given range, supply the `key` and `range_end` fields. All pairs whose keys belong to the
right-open interval `[key, range_end)` will be deleted.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

`range_end` set to `\0` deletes every key-value pair with a key greater than or equal to the provided `key`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

`key` set to `\0` deletes every key-value pair with a key smaller than the provided `range_end`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "key_1" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete all keys

When `key` and `range_end` are both set to `\0`, then all key-value pairs are deleted.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

Additionaly, the field `prev_kv` set to `true` can be supplied.
Regatta then responds with the deleted key-value pairs, if any exists.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\",
    \"prev_kv\": true
    }" \ 127.0.0.1:8443 regatta.v1.KV/DeleteRange
```
