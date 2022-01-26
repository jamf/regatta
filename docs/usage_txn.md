# Transaction API

A transaction is an atomic if/then/else construct over the key-value store.
It provides primitive grouping of requests whose execution is guarded,
supporting the protection of data from concurrent modification.

Transactions consist of two parts - `RequestOp` operations and `Compare`
predicates. Conditional execution of transactions is
also supported.

Protobuf definitions of the discussed API can be found in [`mvcc.proto`](../proto/mvcc.proto).

To gain better understanding of how to use the transaction API, see [examples](#examples).

## Retrieving or Modifying Data With Transactions

Transactions can be invoked by sending `TxnRequest` message via the [`Txn`
remote procedure call](api.md#kv), `regatta.v1.KV/Txn`, which returns `TxnResponse` message.

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
[the next section](#predicates-conditional-execution-of-transactions).
More detailed description and their features of the individual operations can be
found in the [protobuf definition file](../proto/mvcc.proto).

`RequestOp` messages are used in the `success` and `failure` repeated fields in
the `Txn` messages. This is the "execution body" of the if/then/else construct of
conditional execution described [below](#conditional-execution). If a conditional
execution of transactions is not desired, supplement the `RequestOp` operations
only in the `success` field and leave the rest empty.

`ResponseOp` messages are the results of the operations in a given transaction.
A `ResponseOp` is one of `Range`, `Put`, or `DeleteRange` messages, depending on the
type of the corresponding `RequestOp` operation provided in the transaction.
An *n*-th `ResponseOp` message maps to an *n*-th `RequestOp` message in the transaction.

## Conditional Execution

Operations in transactions can be executed conditionally, after supplying a list of
predicates representing a conjunction of terms to be evaluated on the data
in Regatta. This is the repeated `compare` field in the `Txn` protobuf message.
Depending on the result of the conjuction of the terms, either `success`
or `failure` operations are executed. If all of the `compare` terms evaluate
to true, then the `success` operations are executed. Otherwise, `failure`
operations are executed.

The predicates (`compare`) and the operations themselves (either
`success` or `failure`) form a single, non-divisible transaction.

This is the current Protobuf definition of the `Compare` message
(see the [API documentation of `Compare`](api.md#mvcc.v1.Compare)),
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

* `CompareResult` - logical operation to be performed on the [`CompareTarget`](#comparetarget).
    It must be one of `EQUAL`, `GREATER`, `LESS`, or `NOT_EQUAL`.
* `CompareTarget` - domain on which the `CompareResult` is performed. Only `VALUE` is currently
   supported.

## Examples

Transactions are executed via the `regatta.v1.KV/Txn` remote procedure call.

### Transaction With No Predicates

### Transaction With Predicates
