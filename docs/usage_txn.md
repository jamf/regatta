# Transaction API

A transaction is an atomic if/then/else construct over the key-value store.
It provides primitive grouping of requests whose execution is guarded,
supporting the protection of data from concurrent modification.

Transactions consist of two parts - `RequestOp` operations and `Compare`
predicates. Transactions can be recursivelly chained - a transaction may
execute another transaction. Conditional execution of transactions is
also supported.

Protobuf definitions of the discussed API can be found in [`mvcc.proto`](../proto/mvcc.proto).

## Retrieving or Modifying Data With Transactions

`RequestOp` messages are the basic building blocks of transactions.
These are the operations used to retrieve data from the data store or to
modify it. A `RequestOp` can be one of `Range`, `Put`, or `DeleteRange` messages.
They can be guarded with predicates, as described in
[the next section](#predicates-conditional-execution-of-transactions).
More detailed description and their features of the individual operations can be
found in the [protobuf definition file](../proto/mvcc.proto).

`RequestOp` messages are used in the `success` and `failure` repeated fields in
the `Txn` messages. If conditional execution of transactions is not desired,
us the `success` field and leave the rest empty.
For conditional execution, read the next section.

## Conditional Execution of Transactions

Transactions can be executed conditionally, after supplying a list of
predicates representing a conjunction of terms to be evaluated on the data
in Regatta. This is the repeated `compare` field in the `Txn` protobuf message.
Depending on the result of the conjuction of the terms, either `success`
or `failure` operations is executed. If all of the `compare` terms evaluate
to true, then the `success` operations are executed. Otherwise, `failure`
operations are executed.

The comparisons (`compare`) and the operations itself (either
`success` or `failure`) form a single, non-divisible transaction.

This is the current Protobuf definition of the `Compare` message,
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

## Examples (TBD)

Executed via the `regatta.v1.KV/Txn` API.

### Transaction With No Predicates

### Transaction With Predicates

### Recursive Transaction
