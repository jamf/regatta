---
title: Architecture
layout: default
nav_order: 4
---

Regatta is designed as a "geographically distributed etcd", providing etcd-like gRPC API in every location
while maintaining consistent data set. See [API](/api) for the complete documentation of the gRPC API.

## Topology

The Regatta is designed as hub-and-spoke or consistent-core system. There is always a single statically
defined leader cluster in the topology. Having a statically defined leader cluster reduces the operational
costs and greatly simplifies the system due to less moving parts.

Regatta topology is designed as a multi-group Raft cluster within each data center with asynchronous
pull-based replication across locations. There are two types of clusters within Regatta multi-location deployment:
**Regatta leader cluster** and **Regatta follower cluster**.

* Regatta leader refers to cluster accepting and confirming write proposals, sometimes referred to as a core cluster.
* Regatta follower refers to a cluster connected to the leader cluster asynchronously replicating its state locally,
  sometimes referred to as an edge cluster.

Thanks to this topology, the user is able to dynamically add additional follower clusters.

> TODO: Image.

## Raft

Regatta uses Raft protocol to ensure consistent data within the boundaries of a single cluster. Raft is not used
across the locations due to its synchronous nature. That way Regatta can grant high write throughput within the
leader cluster without adding cross-location latency to each request.

The consensus algorithm provides fault-tolerance by allowing the system to operate as long as the majority of members
are available. This is not only useful for disaster scenarios but also enables the easy rolling update of the cluster.

> TODO: Image.

## Tables

Regatta supports the notion of tables throughout its API. The tables could be imagined as sort of keyspaces or schemas.
Each table is its own Raft group replicating within a single location, while also being a single replication unit for
cross-location replication. That said, all the API guarantees regarding consistency are always scoped to a single table.
There is no guarantee of data consistency within multiple tables.
