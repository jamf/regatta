---
title: regatta backup
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta backup

Backup regatta to local files

### Synopsis

Command backs up regatta into a directory of choice, it currently backs up all the tables present in the target server.
Backup consist of file per a table in binary compressed form + human-readable manifest file. Use restore command to load backup into the server.

```
regatta backup [flags]
```

### Options

```
      --address string   Regatta maintenance API address (default "127.0.0.1:8445")
      --ca string        Path to the client CA cert file.
      --dir string       Target dir (current directory if empty)
  -h, --help             help for backup
      --json             Enables JSON logging.
      --token string     The access token to use for the authentication.
```

### SEE ALSO

* [regatta](/operations_guide/cli/regatta)	 - Regatta is read-optimized distributed key-value store.

