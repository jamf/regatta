# Regatta

**Regatta** is a distributed, eventually consistent key-value store built for Kubernetes.
It is designed to distribute data globally in a *hub-and-spoke model* with emphasis on *high read throughput*.
It is *fault-tolerant*, able to handle network partitions and node outages gracefully.

## Why Regatta?

* You need to distribute data from a single cluster to multiple follower clusters in edge locations.
* You need a local, persistent, cache within a data center and reads heavily outnumber writes.
* You need a pseudo-document store.

## Documentation

For guidance on installation, deployment, and administration,
see the [documentation page](https://shiny-invention-a2acc4a1.pages.github.io).

## Contributing

Regatta is in active development and contributors are welcome! For guidance on development, see the page
[Contributing](https://shiny-invention-a2acc4a1.pages.github.io/contributing).
Feel free to ask questions and engage in [GitHub Discussions](https://github.com/jamf/regatta/discussions)!

