# Regatta

![logo](logo.jpg)

---

## What is Regatta?

Regatta is a distributed key-value store. Regatta is designed as easy to deploy, kubernetes friendly with emphasis on
high read throughput and low operational cost.

## Documentation

> For guidance on installation, development, deployment, and administration, see our [docs](docs/index.md) folder.

---

## What is it good for?

Regatta is not a general purpose database and as such not be used like that. Regatta should be used in a case of need to
distribute larger-than-memory dataset across distances to ensure data co-location to support services running there.

### Some example uses cases

* You need a distributed KV database to allow local, quick access to the data in Edge locations.
    * Regatta will provide read-only copy of the data in Edge location.
    * Regatta will take care of the data-replication, data availability and resilience in case of Core/leader
      failure.
* You need a persistent cache locally within a data center, in the case when reads heavily outnumber writes.
    * Regatta writes are expensive in comparison with e.g. Redis.
    * Regatta usually reads most of the dataset in-use from memory serving sub-ms reads.
* You need a pseudo Document store
    * You can define secondary indexes or additional columns/tables within a single regatta Table.
    * The data consistency is granted within a single table.
    * There are compare-and-switch and multi-key atomic operations available.

---

## Development

> For guidance on development, see our [development](docs/development.md) document.

---

## Client usage

> For guidance on use of Regatta from client perspective, see our [usage](docs/usage.md) document.
