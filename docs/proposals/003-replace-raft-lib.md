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

## Alternatives

* Replace `dragonboat` with `github.io/etcd/raft`. ETCD raft is a base of Dragonboat library (dragonboat is to some degree a wrapper/fork of it) and as such does not make matters simpler, 
on the flip side though it is more powerful than `hashicorp/raft`.
* Fork `drgaonboat`, the attempt was made in `github.com/coufalja/tugboat` but it was soon discovered that the major overhaul would be needed nevertheless.

## Unresolved Questions


