---
title: Quickstart
layout: default
nav_order: 2
---

# Running Regatta

There are multiple possibilities for how to run Regatta -- building Regatta from source and executing the binary,
using the official Regatta Docker image, or deploying Regatta to Kubernetes via Helm Charts, which is the
recommended way.

## Build binary from source

To build and run Regatta locally, see the [Contribution page](/contributing) for all
the required dependencies. Then just run

```bash
git clone git@github.com:jamf/regatta.git && cd regatta
make run
```

## Pull and run official Docker image

Official Regatta images are present in
[`jamf/regatta` GitHub Container Registry](https://github.com/jamf/regatta/pkgs/container/regatta).
Just execute `Docker run` with the following arguments:

```bash
docker run \
    --mount type=bind,source=$(pwd)/hack,target=/hack \
    ghcr.io/jamf/regatta:latest leader \
    --dev-mode \
    --api.reflection-api \
    --raft.address=127.0.0.1:5012 \
    --raft.initial-members='1=127.0.0.1:5012' \
    --tables.names=regatta-test
```

{: .note }
In order to run Regatta, TLS certificates and keys must be provided. For testing purposes,
[certificate and key present in the repository](https://github.com/jamf/regatta/tree/cfc58f0205484b0c8a24c7cbcc0be8563b7cf6a5/hack)
can be used.

## Deploying to Kubernetes from Helm Chart

To easily deploy Regatta to Kubernetes, official [Regatta Helm Chart](https://github.com/jamf/regatta-helm) can be used.

```bash
# values.yaml
api:
  tls:
    cert: "<PLAINTEXT-CERT>"
    key: "<PLAINTEXT-KEY>"

replication:
  tls:
    cert: "<PLAINTEXT-CERT>"
    ca: "<PLAINTEXT-CA>"
    key: "<PLAINTEXT-KEY>"

maintenance:
  server:
    tls:
      cert: "<PLAINTEXT-CERT>"
      key: "<PLAINTEXT-KEY>"
```

```bash
helm repo add regatta https://jamf.github.io/regatta-helm
helm repo update
helm install my-regatta regatta/regatta -f values.yaml
```

This will deploy Regatta leader cluster with one replica. See page
[Deploying to Kubernets](/operations_guide/deploying_to_kubernetes/) for the more advanced deployment
of Regatta and how to connect follower clusters to the leader cluster.

## Interacting with Regatta

Once Regatta is up and running, check the [User Guide page](/user_guide) to see how
to interact with Regatta.
