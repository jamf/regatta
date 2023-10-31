---
title: Transactions
layout: default
parent: User Guide
nav_order: 4
---

# Transactions

See [Txn Request API](../api.md#regatta-v1-TxnRequest) and [Txn Response API](../api.md#regatta-v1-TxnResponse)
for the complete gRPC API documentation for retrieving records from Regatta.

A Regatta transaction is an atomic *if/then/else* construct over the key-value store.
It provides a primitive grouping of requests whose execution is guarded,
**supporting the protection of data from concurrent modification**.

Transactions consist of two parts - `RequestOp` operations and `Compare`
predicates. Conditional execution of transactions is
also supported.

To gain a better understanding of how to use the transaction API, see [examples](#examples).

## Retrieving or Modifying Data With Transactions

Transactions can be invoked by sending `TxnRequest` message via the
[`Txn` remote procedure call](../api.md#regatta-v1-KV), `regatta.v1.KV/Txn`, which returns `TxnResponse` message.

`TxnRequest` consists of `Compare` predicates, `RequestOp` operations to be executed
depending on the evaluation of the predicates, and the name of a table in which the transaction
will be executed.

`TxnResponse` consists of a `ResponseHeader`, `ResponseOp` responses, and a `succeeded`
field denoting whether the `Compare` predicates evaluated to true
(i.e. `TxnResponse.succeeded == true`) or not.

`RequestOp` messages are the basic building blocks of transactions.
These are the operations used to retrieve data from the data store or to
modify it. A `RequestOp` is one of `Range`, `Put`, or `DeleteRange` messages.
They can be guarded with predicates, as described in
[the next section](#conditional-execution).
More detailed description and their features of the individual operations can be
found in the [API documentation](../api.md#requestop).

`RequestOp` messages are used in the `success` and `failure` repeated fields in
the `Txn` messages. This is the "execution body" of the if/then/else construct of
conditional execution described [below](#conditional-execution). If a conditional
execution of transactions is not desired, supplement the `RequestOp` operations
only in the `success` field and leave the rest empty.

`ResponseOp` messages are the results of the operations in a given transaction.
A `ResponseOp` is one of `Range`, `Put`, or `DeleteRange` messages, depending on the
type of the corresponding `RequestOp` operation provided in the transaction.
An *n*-th `ResponseOp` message maps to an *n*-th `RequestOp` message in the transaction.
See the [API documentation](../api.md#mvcc-v1-ResponseOp) for more details.

## Conditional Execution

Operations in transactions can be executed conditionally, after supplying a list of
predicates representing a logical conjunction of terms to be evaluated on the data
in Regatta. This is the repeated `compare` field in the `Txn` protobuf message.
Depending on the result of the conjunction of the terms, either `success`
or `failure` operations are executed. If all of the `compare` terms evaluate
to true, then the `success` operations are executed. Otherwise, `failure`
operations are executed.

**The predicates and the operations themselves form a single, non-divisible transaction.**

This is the current Protobuf definition of the `Compare` message
(see the [API documentation of `Compare`](../api.md#mvcc-v1-Compare)),
representing a single term in a given conjunction:

```protobuf
message Compare {
  enum CompareResult {
    EQUAL = 0;
    GREATER = 1;
    LESS = 2;
    NOT_EQUAL = 3;
  }
  enum CompareTarget {
    VALUE = 0;
  }
  
  // result is logical comparison operation for this comparison.
  CompareResult result = 1;
  // target is the key-value field to inspect for the comparison.
  CompareTarget target = 2;
  // key is the subject key for the comparison operation.
  bytes key = 3;
  oneof target_union {
    // value is the value of the given key, in bytes.
    bytes value = 4;
  }

  // range_end compares the given target to all keys in the range [key, range_end).
  // See RangeRequest for more details on key ranges.
  bytes range_end = 64;
}
```

> In future, `CompareTarget` will support comparing against version,
> create revision, modification revision, and lease ID of a given record.

* `CompareResult` - logical operation to be performed on the `CompareTarget`.
    It must be one of `EQUAL`, `GREATER`, `LESS`, or `NOT_EQUAL`. Testing for existence of a
    given key is described in [Testing Existence of Key](#testing-existence-of-key) and testing
    for existence of a key within range is described in
    [Testing Existence of Key Within Range](#testing-existence-of-key-within-range).
* `CompareTarget` - domain on which the `CompareResult` is performed. Only `VALUE` is currently
   supported.

### Testing Existence of Key

A predicate testing for the existence of a given key can also be created.
To do so, supply only the `key` in the `Compare` message. An example of a such
predicate can be found [here](#predicate-testing-existence-of-key).

### Testing Existence of Key Within Range

To test the existence of some keys within a given range, supply only the
`key` and `range_end` in the `Compare` message. An example of a such predicate
can be found [here](#predicate-testing-existence-of-key-within-range).
Note that the predicate evaluates to false if and only if no key exists
in the provided range. Also, `key` and `range_end` form a right-open interval `[key, range_end)`.

## Examples

Transactions are executed via the `regatta.v1.KV/Txn` remote procedure call.

### Transaction With No Predicates

Suppose we wish to atomically insert multiple records into table `regatta-test`,
and list them back. We could achieve this by defining multiple `PUT` operations,
one for each record, and a `RANGE` operation, listing the inserted records,
all in the `success` branch.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"success\": [{
      \"request_put\": {
        \"key\": \"$(echo -n "brba:walter" | base64)\",
        \"value\": \"$(echo -n "white" | base64)\"
      }
    }, {
      \"request_put\": {
        \"key\": \"$(echo -n "brba:jessie" | base64)\",
        \"value\": \"$(echo -n "pinkman" | base64)\"
      }
    }, {
      \"request_put\": {
        \"key\": \"$(echo -n "brba:hank" | base64)\",
        \"value\": \"$(echo -n "schrader" | base64)\"
      }
    }, {
      \"request_range\": {
        \"key\": \"$(echo -n "brba:" | base64)\",
        \"range_end\": \"$(echo -n "brbb:" | base64)\",
        \"count_only\": \"true\"
      }
    }]
}" localhost:8443 regatta.v1.KV/Txn
```

This would be the expected response:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "8",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "succeeded": true,
  "responses": [
    {
      "responsePut": {

      }
    },
    {
      "responsePut": {

      }
    },
    {
      "responsePut": {

      }
    },
    {
      "responseRange": {
        "count": "3"
      }
    }
  ]
}
```

### Transaction With Predicates

The following transaction checks whether there's a key-value pair
`john:doe` in table `regatta-test`. If such a key-value pair
exists, a new record `jane:doe` is upserted in a compare-swap fashion,
as defined in the `success` branch. We also wish to retrieve the previous
key-value of the newly upserted record, as stated in `success[0].request_put.prev_kv = true`.
Mind the upper case `EQUAL` and `VALUE` special values for `compare[0].result` and
`compare[0].target`, respectively. The two records are then listed.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"compare\": [{
      \"result\": \"EQUAL\",
      \"target\": \"VALUE\",
      \"key\": \"$(echo -n "john" | base64)\",
      \"value\": \"$(echo -n "doe" | base64)\"
    }],
    \"success\": [{
      \"request_put\": {
        \"key\": \"$(echo -n "jane" | base64)\",
        \"value\": \"$(echo -n "doe" | base64)\",
        \"prev_kv\": \"true\"
      }
    }, {
      \"request_range\": {
        \"key\": \"$(echo -n "jane" | base64)\"
      }
    }, {
      \"request_range\": {
        \"key\": \"$(echo -n "john" | base64)\"
      }
    }]
}" localhost:8443 regatta.v1.KV/Txn
```

This would be one of the possible responses if the record `john:doe` existed when the transaction was issued:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "12",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "succeeded": true,
  "responses": [
    {
      "responsePut": {

      }
    },
    {
      "responseRange": {
        "kvs": [
          {
            "key": "amFuZQ==",
            "value": "ZG9l"
          }
        ],
        "count": "1"
      }
    },
    {
      "responseRange": {
        "kvs": [
          {
            "key": "am9obg==",
            "value": "ZG9l"
          }
        ],
        "count": "1"
      }
    }
  ]
}
```

Response if the record `john:doe` did not exist:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "11",
    "raftTerm": "2",
    "raftLeaderId": "1"
  }
}
```

Additional operations can be provided in the `failure` branch,
which will execute when any of the predicates in `compare` evaluate
to false. Let's extend the previous example by inserting a different
record if the record `john:doe` is not found.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"compare\": [{
      \"result\": \"EQUAL\",
      \"target\": \"VALUE\",
      \"key\": \"$(echo -n "john" | base64)\",
      \"value\": \"$(echo -n "doe" | base64)\"
    }],
    \"success\": [{
      \"request_put\": {
        \"key\": \"$(echo -n "jane" | base64)\",
        \"value\": \"$(echo -n "doe" | base64)\",
        \"prev_kv\": \"true\"
      }
    }, {
      \"request_range\": {
        \"key\": \"$(echo -n "jane" | base64)\"
      }
    }, {
      \"request_range\": {
        \"key\": \"$(echo -n "john" | base64)\"
      }
    }],
    \"failure\": [{
      \"request_put\": {
        \"key\": \"$(echo -n "foo" | base64)\",
        \"value\": \"$(echo -n "bar" | base64)\"
      }
    }]
}" localhost:8443 regatta.v1.KV/Txn
```

Before executing this transaction, delete the `john:doe`
record in the database to enforce execution of the `failure` branch.
This would then be the expected response:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "14",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "responses": [
    {
      "responsePut": {

      }
    }
  ]
}
```

### Predicate Testing Existence of Key

The following predicate evaluates to true if a key-value pair with the key
`john` exists. If so, the success operations are then executed.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"compare\": [{
      \"key\": \"$(echo -n "john" | base64)\"
    }],
    \"success\": [{
      \"request_put\": {
        \"key\": \"$(echo -n "jane" | base64)\",
        \"value\": \"$(echo -n "doe" | base64)\"
      }
    }]
}" localhost:8443 regatta.v1.KV/Txn
```

Suppose a key-value pair with the key `john` exists,
this would be the expected response:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "revision": "17",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "succeeded": true,
  "responses": [
    {
      "responsePut": {

      }
    }
  ]
}
```

Otherwise, such response would be returned:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "raftTerm": "2",
    "raftLeaderId": "1"
  }
}
```

### Predicate Testing Existence of Key Within Range

To test if there is any key-value pair between the keys `jack`
and `john`, excluding the key-value pair with the key `john`,
we supply `jack` and `john` as the `key` and `range_end` in
the predicate. If such pair exists, a count of such key-value
pairs is retrieved.

```bash
grpcurl -insecure "-d={
    \"table\": \"$(echo -n "regatta-test" | base64)\",
    \"compare\": [{
      \"key\": \"$(echo -n "jack" | base64)\",
      \"range_end\": \"$(echo -n "john" | base64)\"
    }],
    \"success\": [{
      \"request_range\": {
        \"key\": \"$(echo -n "jack" | base64)\",
        \"range_end\": \"$(echo -n "john" | base64)\",
        \"count_only\": "true"
      }
    }]
}" localhost:8443 regatta.v1.KV/Txn
```

Suppose records with the keys `alex`, `jack`, `jim`, `john`, and `pete` exist.
This would be the expected response:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "raftTerm": "2",
    "raftLeaderId": "1"
  },
  "succeeded": true,
  "responses": [
    {
      "responseRange": {
        "count": "2"
      }
    }
  ]
}
```

> Note that `key` and `range_end` form a right-open interval (`[key, range_end)`), hence the response
> does not contain the pair with `john` as a key.

If only keys `alex`, `john`, and `pete` exist, such response would be returned:

```json
{
  "header": {
    "shardId": "10001",
    "replicaId": "1",
    "raftTerm": "2",
    "raftLeaderId": "1"
  }
}
```
