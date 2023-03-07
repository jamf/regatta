---
title: Metrics and observability
layout: default
parent: Operations Guide
nav_order: 4
---

## Metrics

Regatta exposes metrics for Prometheus, available via the `/metrics` endpoint in the REST API (default port 8079).
Go runtime statistics, gRPC statistics, and [Dragonboat](https://github.com/lni/dragonboat) statistics,
which is the underlying framework used for Raft clusters, are exposed. Some exposed metrics:

* `dragonboat_raftnode_has_leader{shardid="1000",replicaid="1"}` --
  Raft leader for the table with shard ID `1000` is instance `1`.
* `grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="regatta.v1.KV",grpc_type="unary"}` --
  total number of served `Range` requests.
* `regatta_table_storage_cache_hits{clusterID="10001",table="regatta-test",type="block"}` --
  Regatta table storage block cache hits
* `regatta_table_storage_cache_misses{clusterID="10001",table="regatta-test",type="block"}` --
  Regatta table storage block cache misses
* `regatta_table_storage_read_amp{clusterID="10001",table="regatta-test"}` -- Regatta table storage read amplification

## Alerts

Prometheus alerting rules can be found in the
[Helm Chart](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L467).

## Debugging

Regatta also exposes the `/debug` endpoint in the REST API for runtime profiling via
[pprof](https://github.com/google/pprof).

