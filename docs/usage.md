# Usage

## API

Regatta has gRPC API defined in `proto/regatta.proto` and `proto/mvcc.proto`. For the detailed description of individual
methods see the mentioned proto files or [api docs](api.md). Some parts of the API are not
implemented [yet](#unimplemented).

### Limits

* Key size limit - 1KiB
* Value size limit - 2MiB
* Single gRPC message size limit - 4MiB

### grpcurl

[grpcurl](https://github.com/fullstorydev/grpcurl) is universal gRPC client that can be used for testing. It has similar
interface to cURL. Few examples how to use it with regatta:

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

## KV API

<a name="unimplemented"></a>

### Unimplemented

> Several API fields are not yet implemented.

- `KV.Range`
    - not implemented features:
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
        - return previous KV
    - not implemented fields:
        - `prev_kv`

### Range

Serves for retrieval of data.

- [API](api.md#RangeRequest)
- [Detailed usage](usage_range.md)

### Put

Serves for update or create of a single KV pair. page.

- [API](api.md#PutRequest)
- [Detailed usage](usage_put.md)

### DeleteRange

Serves for delete of a single or multiple KV pair(s).

- [API](api.md#DeleteRangeRequest)
- [Detailed usage](usage_delete_range.md)

### Txn

Combines all previous commands into single atomic unit guarded by the condition, could be used for `get and set` or
`set multiple` type of operations.

- [API](api.md#TxnRequest)
- [Detailed usage](usage_txn.md)
