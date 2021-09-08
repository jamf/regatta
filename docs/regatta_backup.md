## regatta backup

Backup regatta to local files

### Synopsis

Command backs up regatta into a directory of choice, it currently back up all the tables present in the target server.
Backup consist of file per a table in binary compressed form + human-readable manifest file. Use restore command to load backup into the server.

```
regatta backup [flags]
```

### Options

```
      --address string   Leader API address (default "127.0.0.1:8444")
      --ca string        Path to the client CA cert file. (default "hack/replication/ca.crt")
      --cert string      Path to the client certificate. (default "hack/replication/client.crt")
      --dir string       Target dir (current directory if empty)
  -h, --help             help for backup
      --key string       Path to the client private key file. (default "hack/replication/client.key")
```

### SEE ALSO

* [regatta](regatta.md)	 - Regatta is read-optimized distributed key-value store.

