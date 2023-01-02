---
title: Metrics and observability
layout: default
parent: Operations Guide
nav_order: 4
---

## Metrics

Metrics are available via the `/metrics` endpoint in the REST API (default port 8079),
exposing Go runtime statistics, gRPC statistics, and [Dragonboat](https://github.com/lni/dragonboat) statistics,
which is the underlying framework used for Raft clusters.

## Alerts

Prometheus alerting rules can be found in the
[Helm Chart](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L467).

## Debugging

Regatta also exposes the `/debug` endpoint in the REST API for runtime profiling via
[pprof](https://github.com/google/pprof).

