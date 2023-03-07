---
title: Quickstart
layout: default
nav_order: 3
---

# Running Regatta

There are multiple possibilities for how to run Regatta -- building Regatta from source and executing the binary,
using the official Regatta Docker image, or deploying Regatta to Kubernetes via Helm Charts, which is the
recommended way.

{: .important }
In order to run Regatta, TLS certificates and keys must be provided. For testing purposes,
[certificate and key present in the repository](https://github.com/jamf/regatta/tree/cfc58f0205484b0c8a24c7cbcc0be8563b7cf6a5/hack)
can be used.

## Run binary

### Build from source

To build and run Regatta locally, see the [Contribution page](/contributing) for all
the required dependencies. Then just run

```bash
git clone git@github.com:jamf/regatta.git && cd regatta
make run
```

This command will start a Regatta leader cluster with a single instance locally.

### Download released binary

You can also download binary from the [Releases GitHub Page](https://github.com/jamf/regatta/releases).
After downloading the binary for the given platform, unzip the archive and run the following command:

```bash
tar -xf regatta-darwin-amd64.tar
./regatta leader \
    --dev-mode \
    --api.reflection-api \
    --api.cert-filename=/path/to/server.crt \
    --api.key-filename=/path/to/server.key \
    --maintenance.cert-filename=/path/to/server.crt \
    --maintenance.key-filename=/path/to/server.key \
    --replication.ca-filename=/path/to/server.crt \
    --replication.cert-filename=/path/to/server.crt \
    --raft.address=127.0.0.1:5012 \
    --raft.initial-members='1=127.0.0.1:5012' \
    --tables.names=regatta-test
```

This command will start a Regatta leader cluster with a single instance locally.

{: .note }
Mind the flags providing certificates and keys for the APIs. For testing purposes,
[certificate and key present in the repository](https://github.com/jamf/regatta/tree/cfc58f0205484b0c8a24c7cbcc0be8563b7cf6a5/hack)
can be used.

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

This command will start a Regatta leader cluster with a single instance in a Docker container.

{: .note }
Mind the `--mount` argument mounting the `/hack` directory to the container. This is the default location
where Regatta looks for certificates and keys for APIs. For testing purposes,
[certificate and key present in the repository](https://github.com/jamf/regatta/tree/cfc58f0205484b0c8a24c7cbcc0be8563b7cf6a5/hack)
can be used.

## Deploy to Kubernetes from Helm Chart

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
[Deploying to Kubernets](operations_guide/deploying_to_kubernetes/) for the more advanced deployment
of Regatta and how to connect follower clusters to the leader cluster.

## Interact with Regatta

Once Regatta is up and running, check the [User Guide page](user_guide) to see how
to interact with Regatta.
