---
title: "002: Cursor API"
layout: default
parent: Proposals
nav_order: 2
---

Proposal status: Implemented
{: .label .label-green }

## Summary

Add additional RPC to `regatta.v1.KV` API that would provide a way to query the large dataset from the server.
Currently, the client has to rely on `Range` RPC paging but that is not providing consistent view in time as well as is
rather unwieldy.

## Motivation

* To provide point in time view over the large dataset.

## Design

* Introduce new RPC to `regatta.proto`

```protobuf
service KV {
  // Cursor gets the keys in the range from the key-value store.
  rpc Cursor(RangeRequest) returns (stream RangeResponse);
}
```

* On client call server creates iterator according to `RangeRequest`
* Server batches responses in returned stream from the iterator until iterator exhaustion.
* Batch size should be "reasonable" (needs to be clarified).
* `RangeResponse` should set `more` to `true` unless last (or only) message in the stream.

## Alternatives

* Use of regular Range queries in series - Unfeasible due to consistency reasons, every query would be executed on
  potentially different state of the database.

## Unresolved Questions

* Name of the RPC, the proposal is `Cursor` other alternatives might be `RangeStream`, `Stream`, `Query` etc.
* Default batch (stream response message) size, should be probably lower than max response size `4MB`, prosed value
  is `512KB` as tradeoff between latency and throughput.
