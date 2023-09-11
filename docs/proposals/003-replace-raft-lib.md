---
title: "003: Replace Raft library"
layout: default
parent: Proposals
nav_order: 3
---

## Summary
Replace the heart of Regatta the `gihub.com/lni/dragonboat` library with `github.com/hashicorp/raft` library.

## Motivation
As the Regatta evolved we run into problems with `dragonboat` library.
* Library is maintained by just a single person.
* It is a closed design library (most stuff implemented in `internal` packages).
* Has inadequate API, hides many details and takes the control away from the programmer.
    * Snapshots
    * Logging
    * Configuration
    * Transport
    * Node discovery
    * ID assignment
* Even though `dragonboat` has a lot of stars it does not seem to be used much (just tens of imports), in contrast `hashicorp/raft` has over 1k imports.
* Library is not really modular, replacing Transport is almost impossible, the same with LogDB and Snapshot store.

## Design

### Challenges
* `hashicorp/raft` is not multi-group raft implementation
  * that could be mitigated by multiplexing over custom transport (see. [raft-grpc-transport-mux](https://github.com/coufalja/raft-grpc-transport-mux))
  * the advantage is that we could pick the group label of our liking (like table name)
* `hashicorp/raft` does not have support for `on-disk` statemachine impl OOTB
  * every start of FSM is accompanied by applying the most recent snapshot
  * in the case of our table FSM that would lead to large compute and space overhead
  * mitigation lies in implementing own SnapshotManager that would serve lightweight snapshot out of the persisted data
  * inspiration could be found in [Vault](https://github.com/hashicorp/vault/blob/main/physical/raft/snapshot.go)
* `hashicorp/raft` does not automatically forward proposals to the leader node
  * that could be implemented using the same layer as the Raft multiplexing
  * inspiration could be drawn from [Consul](https://github.com/hashicorp/consul/blob/main/internal/storage/raft/forwarding.go)

## Alternatives

* Replace `dragonboat` with `github.io/etcd/raft`. ETCD raft is a base of Dragonboat library (dragonboat is to some degree a wrapper/fork of it) and as such does not make matters simpler, 
on the flip side though it is more powerful than `hashicorp/raft`.
* Fork `drgaonboat`, the attempt was made in `github.com/coufalja/tugboat` but it was soon discovered that the major overhaul would be needed nevertheless.

## Unresolved Questions


