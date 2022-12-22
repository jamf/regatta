---
title: Home
layout: home
nav_order: 1
---

**Regatta** is a distributed, eventually consistent key-value store built for Kubernetes.
It is designed to distribute data globally in a *hub-and-spoke model* with emphasis on *high read throughput*.
It is *fault-tolerant*, able to handle network partitions and node outages gracefully.

### Built for Kubernetes

Regatta is completely Kubernetes native. You can manage and monitor Regatta as any
other Kubernetes deployment.

### Distribute data globally in a hub-and-spoke model

Regatta is designed to efficiently distribute data from a single core clusters
to multiple edge cluster around the world.

### Emphasis on high read throughput

Regatta is built to handle read-heavy workloads and to serve sub-millisecond reads.

### Fault-tolerance and data availability

Thanks to the Raft algorithm and data redundancy, Regatta is able to serve reads even in the event of
network partition or node outage.

### Data persistance

Regatta is more than just an in-memory cache -- data persistence is built-in. Regatta
can be backed up and restored from backups.

{: .important }
Regatta is under heavy development. This means that the API may change
and there are a lot of things to be implemented. If you would like to
help, do not hesitate and check the [Contributing](contributing) page!

