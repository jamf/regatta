---
title: "001: Range query consistency"
layout: default
parent: Proposals
nav_order: 1
---

Proposal status: Implemented
{: .label .label-green }

## Summary

Change current semantics of `regatta.v1.KV/Range` in a way that when no results are found the response returned will be
consistently empty `RangeResponse`.
Currently, when `RangeRequest` without `range_end` is sent the API responds with GRPC `NOT_FOUND` error.

## Motivation

* The behaviour is inconsistent with other (Put/DeleteRange) APIs.
* With current design the response does not include raft header that could be eventually used by the client to determine
  the table leader.
* It encourages "exception driven" code in clients.

## Design

* When 0 results are found in the underlying storage simple empty response (including raft header) should be returned.
* `NOT_FOUND` should still be returned when the table requested does not exist. (no change)
* The same behaviour should be mimicked also in `regatta.v1.KV/Txn` API queries. (which should fix #132)

## Alternatives

* No change - inconsistent API stays.
* API evolution (introducing V2 or new API method) - complexity for clients, in this early phase we could allow
  ourselves a leniency of a breaking change.

## Unresolved Questions

N/A
