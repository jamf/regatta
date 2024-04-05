# Regatta

[![tag](https://img.shields.io/github/tag/jamf/regatta.svg)](https://github.com/jamf/regatta/releases)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/jamf/regatta)
![Build Status](https://github.com/jamf/regatta/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/jamf/regatta/badge.svg)](https://coveralls.io/github/jamf/regatta)
[![Go report](https://goreportcard.com/badge/github.com/jamf/regatta)](https://goreportcard.com/report/github.com/jamf/regatta)
[![Contributors](https://img.shields.io/github/contributors/jamf/regatta)](https://github.com/jamf/regatta/graphs/contributors)
[![License](https://img.shields.io/github/license/jamf/regatta)](LICENSE)

![Regatta logo](./docs/static/regatta.png "Regatta")

**Regatta** is a distributed ETCD inspired key-value store. Regatta is designed to operate eiter as a standalone node,
standalone cluster or in Leader - Follower mode suited for distributing data in distant locations. e.g. in different
cloud regions.
While Regatta maintains many of ETCD features there are some notable differences:

* Regatta is designed to store much larger (tens of GB) datasets and also provide iterator-like API to query large
  datasets.
* Regatta prioritize speed and performance over some more advanced ETCD features like Watch API, or Leases.
* Regatta support multiple separate keyspaces called tables which operate individually.

## Production readiness

* Even though Regatta has not yet reached the 1.0 milestone it is ready for a production use.
* There might be backward incompatible changes introduced until version 1.0, those will always be flagged in the release
  notes.
* Builds for tagged versions are provided in form of binaries in GH release, and Docker images.
* Tagged releases are suggested for production use, mainline builds should be used only for testing purposes.

## Why you should consider using Regatta?

* You need to distribute data from a single cluster to multiple follower clusters in edge locations.
* You need a local, persistent, cache within a data center and reads heavily outnumber writes.
* You need a pseudo-document store.

## Documentation

For guidance on installation, deployment, and administration,
see the [documentation page](https://engineering.jamf.com/regatta).

## Contributing

Regatta is in active development and contributors are welcome! For guidance on development, see the page
[Contributing](https://engineering.jamf.com/regatta/contributing).
Feel free to ask questions and engage in [GitHub Discussions](https://github.com/jamf/regatta/discussions)!

