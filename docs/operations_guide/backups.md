---
title: Backups
layout: default
parent: Operations Guide
nav_order: 3
---

# Backups

Regatta supports creating backups and restoring from them through the [Maintenance gRPC API](/api/#maintenance-prot)
and built-in [`backup`](/operations_guide/cli/regatta_backup) and
[`restore`](/operations_guide/cli/regatta_restore) commands.


{: .important }
Backing up and restoring can be done only in a leader cluster.

## Create backup

To create backups, Maintenance API must be enabled during Regatta startup.
See the [Helm Chart](https://github.com/jamf/regatta-helm/blob/master/charts/regatta/values.yaml)
or the [CLI documentation](/operations_guide/cli/regatta_leader) for reference.

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

The command then creates binary file for each table and a human-readable JSON manifest.

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
      --dir=/backup \
      --json=true
```

## Resetting a follower cluster

Data in the follower cluster can also be wiped completely, forcing the follower to reload all the data directly from
the leader. See the [Reset method in the Maintenance gRPC API documentation](/api/#maintenance) for more information.

