---
title: CHANGELOG
layout: default
nav_order: 999
---

# Changelog

## v0.2.0 (Unreleased)
Release is mostly focused on tidying up the docs and code base and on resource consumption improvements.
- New way of producing table snapshots is introduced that should lead to quicker and more efficient process of recovering shards in a Raft cluster.
- Experimental Tan logdb feature was promoted to a regular and made a default choice.
- Gossip based cluster registry was added, the necessary migration is handled automatically by the new version of the `jamf/regatta-helm` helm chart. If you want to know more refer to the documentation of `memberlist` flags.

### Breaking changes
* Removal of `--experimental.tanlogdb` flag.
* Addition of gossip based cluster registry.

### Features
* New `--raft.logdb` flag (defaults to Tan).
* Added `--memberlist.address`, `--memberlist.advertise-address` and  `--memberlist.members` flags to configure cluster registry.
* Added a raft cluster snapshot mode option `--raft.snapshot-recovery-type` which defaults to new `checkpoint` mode.
* Added a flag for configuring shared table cache size `--storage.table-cache-size` which defaults to `1024`.

### Improvements
* Compressed Log replication messages to lower API bandwidth usage.
* Bump to Go 1.20.
* Pebble instances table cache is now shared.
* Single key lookups now utilise bloom filters optimizing the "key not present" case.
* Added compaction metrics of pebble instances.

### Bugfixes
* Removing kafka package leftovers.
* Improving logreader tests.
* Fixed raft header returned from non-leader cluster nodes.

## v0.1.0

Initial Regatta v0.1.0 Release

This release was made possible thanks to the contributions of @jizi @martinvizvary @jojand @mrackoa and @juldou :rocket::tada:

Special shout-out to @coufalja for his efforts!
