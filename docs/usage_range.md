# Range API

Serves for retrieval of data.

## Key Lookup

To query a single key set just `table` and `key` field of the message.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

## Prefix Search

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

## Range Search

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

## List All Pairs

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

## Get Keys Only

List keys in the range `[key, range_end)` with the `keys_only` option.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\",
    \"keys_only\": true}" \
    127.0.0.1:8443 regatta.v1.KV/Range
```

## Get Count Only

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
