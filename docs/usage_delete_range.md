# DeleteRange API

Delete records from Regatta.

## Delete Single Key

To delete a single key-value pair supply just a `key` argument.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete Keys By Prefix

To delete key-value pairs with a shared prefix, supply the `key` and `range_end` fields, where `key` is the prefix
and `range_end` == `key` + 1. For example, to find all key-value pairs prefixed with the timestamp `1626783802`,
set `key` to `1626783802` and `range_end` to `1626783803`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "1626783802" | base64)\",
    \"range_end\": \"$(echo -n "1626783803" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete Keys Range

To delete key-value pairs in a given range, supply the `key` and `range_end` fields. All pairs whose keys belong to the
right-open interval `[key, range_end)` will be deleted.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "key_20" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

`range_end` set to `\0` deletes every key-value pair with a key greater than or equal to the provided `key`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "key_1" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

`key` set to `\0` deletes every key-value pair with a key smaller than the provided `range_end`.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "key_1" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

## Delete All Keys

When `key` and `range_end` are both set to `\0`, then all key-value pairs are deleted.

```bash
$ grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"key\": \"$(echo -n "\0" | base64)\",
    \"range_end\": \"$(echo -n "\0" | base64)\"}" \
    127.0.0.1:8443 regatta.v1.KV/DeleteRange
```

Additionaly, we can supply `"prev_kvs": "true"` to the request,
which would return the previous key-value pair we upserted with
the request, if any exists.
