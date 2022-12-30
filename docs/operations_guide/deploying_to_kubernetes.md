---
title: Deploying to Kubernetes
layout: default
parent: Operations Guide
nav_order: 1
---

{: .important }
Regatta currently supports only static cluster members, therefore the number
of members cannot be changed once the cluster is running. To change the number of members in a cluster
when creating the cluster, see the
[Helm Chart](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L21).

# Deploying Regatta to Kubernetes

## Deploying Regatta cluster

In order to run Regatta cluster, TLS certificates and keys must be generated for the gRPC APIs.

See
[Regatta](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L114),
[Replication](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L197), and
[Maintenance API](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L285)
definitions in the Helm Chart or [`regatta leader`](/operations_guid/cli/regatta_leader) command for reference.

> TODO: Show certificate setup

> TODO: Add `helm install` of the leader cluster (test it in kind)

## Connecting follower cluster

In order to connect follower cluster to the leader cluster, leader's address must be provided during follower's
startup. See the field `replication.leaderAddress` in the [Helm Chart](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L239)

> TODO: Add `helm install` of the follower cluster cluster (test it in kind). Show writing in leader and reading in follower.

