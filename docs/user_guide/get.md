---
title: Retrieving Records
layout: default
parent: User Guide
nav_order: 1
---

See [Range Request API](/api/#regatta-v1-RangeRequest) and [Range Response API](/api#regatta-v1-RangeResponse)
for the complete gRPC API documentation for retrieving records from Regatta.

## Range API

Range API serves for the retrieval of data, offering exact key-value pair lookups,
prefix searches and range searches.

### Exact key-value pair lookup

To query a single key-value pair, just set the `table` and `key` fields in the payload.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Prefix search

To query key-value pairs with a shared prefix, supply the `key` and `range_end` fields,
where `key` is the prefix and `range_end = key + 1`.
For example, to find all key-value pairs prefixed with the timestamp `1626783802`,
set `key` to `1626783802` and `range_end` to `1626783803`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\",
    \"range_end\": \"$(echo -n "1626783803" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Range search

To query key-value pairs in a given range, supply the `key` and `range_end` fields.
All pairs whose keys belong to the right-open interval `[key, range_end)` will be returned.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

`range_end` set to `\0` lists every key-value pair with a key greater than or equal to the provided `key`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

`key` set to `\0` lists every key-value pair with a key smaller than the provided `range_end`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "key_1" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### List all key-value pairs

When `key` and `range_end` are both set to `\0`, then all key-value pairs are returned.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Get keys only

List keys in the range `[key, range_end)` with the `keys_only` option.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\",
    \"keys_only\": true}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

### Get key count only

Get only the number of keys in the range `[key, range_end)` with the `count_only` option.
This can also be used without the `range_end` field to test the existence of `key`.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\",
    \"count_only\": true}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```
