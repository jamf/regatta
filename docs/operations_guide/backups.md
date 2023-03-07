---
title: Backups
layout: default
parent: Operations Guide
nav_order: 3
---

# Backups

Regatta supports creating backups and restoring from them through the [Maintenance gRPC API](../api.md#maintenance-proto)
and built-in [`backup`](cli/regatta_backup.md) and
[`restore`](cli/regatta_restore.md) commands.


{: .important }
Backing up and restoring can be done only in a leader cluster.

To interact with Regatta's Maintenance API, use the `regatta` binary, which can be
downloaded from the [Releases GitHub page](https://github.com/jamf/regatta/releases)
or use the [Docker Image](https://github.com/orgs/jamf/packages?repo_name=regatta).

## Create backup

To create backups, Maintenance API must be enabled during Regatta startup.
See the [Helm Chart](https://github.com/jamf/regatta-helm/blob/master/charts/regatta/values.yaml)
or the [CLI documentation](cli/regatta_leader.md) for reference.

Additionaly, a *token* must be provided during Regatta startup which
is then provided to the `regatta backup` command:

```bash
regatta backup \
      --address=127.0.0.1:8445 \
      --token=$(BACKUP_TOKEN) \
      --ca=ca.crt \
      --dir=/backup \
      --json=true
```

The command then creates binary file for each table and a human-readable JSON manifest
from Regatta leader cluster running on `127.0.0.1:8445`.

### Periodically backing up to S3 Bucket

Regatta Helm Chart also offers a [CronJob](https://github.com/jamf/regatta-helm/blob/master/charts/regatta/values.yaml#L322)
to periodically create backup and push it to an S3 Bucket.

## Restore from backup

{: .warning }
Restoring from backups is a destructive operation and should be used only as a part of a break-glass procedure.

To restore from backups, Maintenance API must be enabled during Regatta startup and a *token* and a directory
containing binary backups and the JSON manifest must be provided to the `regatta restore` command.
All tables present in the manifest are then restored.

```bash
regatta restore \
      --address=127.0.0.1:8445 \
      --token=$(BACKUP_TOKEN) \
      --ca=ca.crt \
      --dir=./backup \
      --json=true
```

This command overwrites all the tables specified in the `backup` directory in a Regatta leader cluster
runnin on `127.0.0.1:8445`.

## Resetting a follower cluster

Data in the follower cluster can also be wiped completely, forcing the follower to reload all the data directly from
the leader. See the [Reset method in the Maintenance gRPC API documentation](../api.md#maintenance) for more information.

