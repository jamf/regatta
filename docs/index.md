---
title: Home
layout: home
nav_order: 1
---

# Regatta

**Regatta** is a distributed, eventually consistent key-value store built for Kubernetes.
It is designed to distribute data globally in a *hub-and-spoke model* with an emphasis on *high read throughput*.
It is *fault-tolerant* and able to handle network partitions and node outages gracefully.

## Built for Kubernetes

Regatta is Kubernetes native. You can manage and monitor Regatta as any
other Kubernetes deployment. Check out the
[official Regatta Helm Chart](https://github.com/jamf/regatta-helm) for more information.

## Distribute data globally in a hub-and-spoke model

Regatta is designed to efficiently distribute data from a single core cluster
to multiple edge clusters around the world. See the [Architecture](/architecture#Topology)
page for more information.

## Emphasis on high read throughput

Regatta is built to handle read-heavy workloads and to serve sub-millisecond reads.

## Fault-tolerance and data availability

Thanks to the Raft algorithm and data redundancy, Regatta can serve reads even in the event of
network partition or node outage.

## Data persistance

Regatta is more than just an in-memory cache -- data persistence is built-in. Regatta
can be backed up and restored from backups.

## Dynamic scaling of edge clusters

Regatta makes it really easy to add new edge clusters. When adding a new edge cluster,
no other clusters are affected and the data is automatically replicated from the core cluster to the edge cluster.

## What is Regatta good for?

### You need a distributed key-value store to allow local and quick access to data in edge locations

Regatta will provide a read-only copy of the data in edge locations. Regatta will take care of the data replication,
data availability, and resilience in case of failure of the core cluster.

### You need a local, persistent, cache within a data center and reads heavily outnumber writes

Regatta writes are expensive in comparison with Redis for example.
Reads are usually served from memory, resulting in sub-millisecond reads.

### You need a pseudo-document store

You can define secondary indexes or additional columns/tables within a single Regatta table.
The data consistency is granted within a single table.
There are compare-and-switch and multi-key atomic operations available.

{: .important }
Regatta is under heavy development. This means the API may change
and there are a lot of things to be implemented. If you would like to
help, do not hesitate and check the [Contributing](contributing) page!
