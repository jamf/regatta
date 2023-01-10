---
title: Deploying to Kubernetes
layout: default
parent: Operations Guide
nav_order: 1
---

# Deploying Regatta to Kubernetes

In order to run Regatta cluster, TLS certificates and keys must be generated for the gRPC APIs.

See
[Regatta](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L114),
[Replication](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L197), and
[Maintenance API](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L285)
definitions in the Helm Chart or [`regatta leader`](/operations_guid/cli/regatta_leader)
and [`regatta follower`](/operations_guid/cli/regatta_follower) commands for reference.

## Deploying Regatta leader cluster

To deploy Regatta leader cluster, let's specify the following values:

```yaml
# values-leader.yaml

# Create Regatta leader cluster with 3 instances.
mode: leader
replicas: 3

# Specify the tables.
tables: testing-table1,testing-table2

replication:
  # Regatta leader cluster's replication gRPC API for the follower clusters to connect to.
  externalDomain: "leader.regatta.example.com"
  port: 8444

  tls:
    cert: "<PLAINTEXT-CERT>"
    ca: "<PLAINTEXT-CA>"
    key: "<PLAINTEXT-KEY>"

api:
  tls:
    cert: "<PLAINTEXT-CERT>"
    key: "<PLAINTEXT-KEY>"

maintenance:
  server:
    tls:
      cert: "<PLAINTEXT-CERT>"
      key: "<PLAINTEXT-KEY>"
```

Then run the following commands in the core cluster where the Regatta leader cluster should reside:

```bash
helm repo add regatta https://jamf.github.io/regatta-helm
helm repo update
helm install regatta-leader regatta/regatta -f values-leader.yaml
```

This will create a Regatta leader cluster with 3 instances with Replication API for the follower clusters
exposed on `leader.regatta.example.com:8444`.

## Connecting follower cluster

Regatta follower cluster can be dynamically added to replicate data from the leader cluster without any
intervention done in the leader cluster. Just make sure the leader's Replication gRPC API
is reachable from the edge clusters where the Regatta follower clusters will be deployed.

To deploy Regatta follower cluster and connect it to the leader cluster, let's specify the following values:

```yaml
# values-follower.yaml
# Values for the Regatta follower cluster.

# Create Regatta follower cluster with 3 instances.
mode: follower
replicas: 3

# Specify the tables.
tables: testing-table1,testing-table2

replication:
  # Disable the Replication gRPC API for follower clusters.
  server:
    enabled: false

  # Specify the address of the Regatta leader cluster to asynchronously replicate data from.
  leaderAddress: "leader.regatta.example.com:8444"

  tls:
    cert: "<PLAINTEXT-CERT>"
    ca: "<PLAINTEXT-CA>"
    key: "<PLAINTEXT-KEY>"

api:
  tls:
    cert: "<PLAINTEXT-CERT>"
    key: "<PLAINTEXT-KEY>"

maintenance:
  server:
    # Optionally, disable maintenance API for the follower cluster.
    enabled: false
```

Then run the following commands in all the edge clusters where the Regatta follower clusters should reside:

```bash
helm repo add regatta https://jamf.github.io/regatta-helm
helm repo update
helm install regatta-follower regatta/regatta -f values-follower.yaml
```

This will create a Regatta follower cluster with 3 instances asynchronously replicating data
from the Regatta leader cluster `leader.regatta.example.com:8444`. Once the Regatta follower cluster is up
and running, it will immediately pull the data from the leader cluster without any manual intervention needed.

{: .important }
Regatta currently supports only static cluster members, therefore the number
of members cannot be changed once the cluster is running. To change the number of members in a cluster
when creating the cluster, see the
[Helm Chart](https://github.com/jamf/regatta-helm/blob/3dc1954d2a08c4a983c7cef0c2e853bfa5ef65aa/charts/regatta/values.yaml#L21).

