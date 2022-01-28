# Put API

Insert and modify records in Regatta.

To insert a record into a table, key, value, and the table
name must be provided. Additionaly, we can supply `"prev_kvs": "true"`
to the request, which would return the previous key-value
pair we upserted with the request, if any exists.

```bash
grpcurl \
    -insecure \
    "-d={
        \"table\": \"$(echo -n "regatta-test" | base64)\",
        \"key\": \"$(echo -n "key_1" | base64)\",
        \"value\": \"$(echo -n "value" | base64)\"
    }" 127.0.0.1:8443 regatta.v1.KV/Put
```

Expected response:

```json
{

}
```

Expected response if the record with key `key_1` previously existed
and `prev_kv` was set to true:

```json
{
  "prevKv": {
    "key": "a2V5XzE=",
    "value": "dGhlX2Nha2VfaXNfYV9saWU="
  }
}
```
