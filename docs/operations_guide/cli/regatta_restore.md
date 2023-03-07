---
title: regatta restore
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta restore

Restore Regatta from local files.

### Synopsis

WARNING: Restoring from backup is a destructive operation and should be used only as part of break glass procedure.

Restore Regatta cluster from a directory of choice. All tables present in the manifest.json will be restored.
Restoring is done sequentially, for the fine-grained control of what to restore use backup manifest file.
It is almost certain that after restore the cold-start of all the followers watching the restored leader cluster is going to be necessary.

```
regatta restore [flags]
```

### Options

```
      --address string   Maintenance API address. (default "127.0.0.1:8445")
      --ca string        Path to the client CA cert file.
      --dir string       Directory containing the backups (current directory if empty)
  -h, --help             help for restore
      --json             Enables JSON logging.
      --token string     The access token to use for the authentication.
```

### SEE ALSO

* [regatta](regatta.md)	 - Regatta is a read-optimized distributed key-value store.

