---
title: regatta backup
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta backup

Backup Regatta to local files.

### Synopsis

Command backs up Regatta into a directory of choice. All tables present in the target server are backed up.
Backup consists of file per a table in a binary compressed form and a human-readable manifest file. Use restore command to load backup into the server.

```
regatta backup [flags]
```

### Options

```
      --address string   Regatta maintenance API address. (default "127.0.0.1:8445")
      --ca string        Path to the client CA certificate.
      --dir string       Target directory (current directory if empty).
  -h, --help             help for backup
      --json             Enables JSON logging.
      --token string     The access token to use for the authentication.
```

### SEE ALSO

* [regatta](regatta)	 - Regatta is a read-optimized distributed key-value store.

