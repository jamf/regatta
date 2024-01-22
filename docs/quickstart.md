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

To build and run Regatta locally, see the [Contribution page](contributing.md) for all
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
    --raft.address=127.0.0.1:5012 \
    --raft.initial-members='1=127.0.0.1:5012'
```
This command will start a Regatta leader cluster with a single instance locally.

Create the `regatta-test` table using the API.
```bash
grpcurl -plaintext -d "{\"name\": \"regatta-test\"}" 127.0.0.1:8443 regatta.v1.Tables/Create
```

## Pull and run official Docker image

Official Regatta images are present in
[`jamf/regatta` GitHub Container Registry](https://github.com/jamf/regatta/pkgs/container/regatta).
Just execute `docker run` with the following arguments:

```bash
docker run \
    -p 8443:8443 \
    ghcr.io/jamf/regatta:latest \
    leader \
    --dev-mode \
    --raft.address=127.0.0.1:5012 \
    --raft.initial-members='1=127.0.0.1:5012'
```

This command will start a Regatta leader cluster with a single instance in a Docker container.

Create the `regatta-test` table using the API.
```bash
grpcurl -plaintext -d "{\"name\": \"regatta-test\"}" 127.0.0.1:8443 regatta.v1.Tables/Create
```

## Deploy to Kubernetes from Helm Chart

To easily deploy Regatta to Kubernetes, official [Regatta Helm Chart](https://github.com/jamf/regatta-helm) can be used.

```bash
helm repo add regatta https://jamf.github.io/regatta-helm
helm repo update
helm install regatta regatta/regatta
```

This will deploy Regatta leader cluster with one replica. See page
[Deploying to Kubernetes](operations_guide/deploying_to_kubernetes.md) for the more advanced deployment
of Regatta and how to connect follower clusters to the leader cluster.

## Interact with Regatta

Once Regatta is up and running, check the [User Guide page](user_guide/index.md) to see how
to interact with Regatta.
