---
title: CHANGELOG
layout: default
nav_order: 999
---

# Changelog
## v0.5.0 (unreleased)

### Highlights
#### Dynamic tables management
* Tables now could be managed dynamically during the runtime of the server using newly provided `regatta.v1.Tables` API.
* Tables API could be secured by an API token using `tables.token` configuration value.

### Deprecations
* `tables.names` and `tables.delete` configuration values were deprecated and will be removed in future releases.

### Bugfixes
* Proper authentication of `maintenance.v1.Backup/Restore` API endpoint.
* When table is deleted in the leader cluster the followers will gracefully handle the situation by deleting the table locally and stopping the replication.
---
## v0.4.1

### Improvements
* Server now reports transient errors for requests that could be potentially retried as `codes.Unavailable`.

### Bugfixes
* Fixed the default timeout of `KV/IterateRange` operation.

---
## v0.4.0

### Breaking changes
* Remove maintenance server port, the API is now available on standard API port. It could still though be secured by a separate API token.
* Replaced `clusterID` with `shard_id` in metric label names.

### Features
* Added new `regatta.v1.KV/IterateRange` API.
* Added option to get the server configuration via the `regatta.v1.Cluster/Status` API.
* Added `memberlist.cluster-name` and `memberlist.node-name` options.
  * Both help identifying cluster and nodes in CLuster API responses.
  * Cluster name setting prevents accidental pairing of multiple clusters running in the same network. (with different cluster name setting)
* Added `api.advertise-address` option to split address advertised to clients from the listen address.
* Removed `maintenance.address` config option.
* Removed `maintenance.cert-filename` config option.
* Removed `maintenance.key-filename` config option.

### Bugfixes
* Fix `KV/DeleteRange` `Count` erroneously returning `1` if no key was deleted.
* Fix `KV/DeleteRange` `PrevKV` panicking when previous key did not exist.

---
## v0.3.2
A bugfix release, fixes critical issue in replication server TLS handling.

### Bugfixes
* Fix replication server TLS.

---
## v0.3.1
A bugfix release, with small improvements.

### Improvements
* Improve startup logging and failure reporting.

### Bugfixes
* Fix `rest.address` handling.
* Fix race-conditions in Raft events handling.

---
## v0.3.0
Release brings a brand-new Cluster API as well as ability to listen over plain or unix socket connection.

### Breaking changes
* Semantics of `api.address` `replication.address` and `maintenance.address` changed. The protocol is now mandatory could be one of `http|https|unix|unixs`.
* Removed `api.reflection-api` configuration.

### Features
* Regatta could now serve its api on unix sockets.
* Support for serving over plain (non-TLS) gRPC added.
* Added `regatta.v1.Cluster` api.

### Improvements
* Improve `version` command output.
* Follower can connect to leader over plain connection or via unix socket.

### Bugfixes
* Fix cluster member node registration.

---
## v0.2.3
Release with profile guided optimization.

### Improvements
* Removal of few obsolete dependencies.
* Set toolchain in go.mod file.
* Enable profile guided optimizations.

---
## v0.2.2
Minor release with just updated dependencies.

---
## v0.2.1
Release focuses on stability and performance of the leader-follower replication.

### Breaking changes
* Leader `replication.logCacheSize` now defaults to 0.

### Features
* Add `replication.keepalive-time` config option for follower. Sets how often the keepalive should be sent.
* Add `replication.keepalive-timeout` config option for follower. Sets how long to wait for an ack of the keepalive message.

### Improvements
* Improve follower replication throughput and latency.
* Snapshot recovery type could be switched on a running cluster safely.
* Bump to Go 1.21.
* Keepalive for replication connection to tackle misbehaving LBs.

### Bugfixes
* Fix release image cosign.
* Fix off by one replication error.

---
## v0.2.0
Release is mostly focused on tidying up the docs and code base and on resource consumption improvements.
- New way of producing table snapshots is introduced that should lead to quicker and more efficient process of recovering shards in a Raft cluster.
- Experimental Tan logdb feature was promoted to a regular and made a default choice.
- Gossip based cluster registry was added, the necessary migration is handled automatically by the new version of the `jamf/regatta-helm` helm chart. If you want to know more refer to the documentation of `memberlist` flags.

### Breaking changes
* Removal of `--experimental.tanlogdb` flag.
* Addition of gossip based cluster registry.
* `regatta.v1.KV/Range` now returns empty response instead of `NOT_FOUND` error if result of the query is an empty range. Implements proposal #001.

### Features
* New `--raft.logdb` flag (defaults to Tan).
* Added `--memberlist.address`, `--memberlist.advertise-address` and  `--memberlist.members` flags to configure cluster registry.
* Added a raft cluster snapshot mode option `--raft.snapshot-recovery-type` which defaults to new `checkpoint` mode.
* Added a flag for configuring shared table cache size `--storage.table-cache-size` which defaults to `1024`.
* Added support for `gzip`, and `snappy` compression for API calls.

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
* Fixed value of `Deleted` field of the `DeleteRange` response.
* Fixed potential DOS by limiting the storage iterator with max RPC size.

---
## v0.1.0

Initial Regatta v0.1.0 Release

This release was made possible thanks to the contributions of @jizi @martinvizvary @jojand @mrackoa and @juldou :rocket::tada:

Special shout-out to @coufalja for his efforts!
