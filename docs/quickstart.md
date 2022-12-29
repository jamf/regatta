---
title: Quickstart
layout: default
nav_order: 2
---

## Running Regatta

There are multiple possibilities how to run Regatta -- the simplest being
building Regatta from source and executing the binary or just pulling
the official Regatta Docker image.

### Build binary from source

To build and run Regatta locally, see the [Contribution page](/contributing) for all
the required dependencies. Then just run

```bash
git clone git@github.com:jamf/regatta.git && cd regatta
make run
```

### Pull and run official Docker image

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

## Interacting with Regatta

Once Regatta is up and running, check the [User Guide page](/user_guide) to see how
to interact with Regatta.
