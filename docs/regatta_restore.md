## regatta restore

Restore regatta from local files

### Synopsis

WARNING: The restore is a destructive operation and should be used only as part of break glass procedure.
Command restore regatta cluster from a directory of choice, it will restore all the tables present in the manifest.json.
The restore will be done sequentially, for the fine-grained control of what to restore use backup manifest file.
It is almost certain that after restore the cold-start of all the followers watching the cluster restored is going to be necessary.

```
regatta restore [flags]
```

### Options

```
      --address string   Regatta maintenance API address (default "127.0.0.1:8445")
      --ca string        Path to the client CA cert file.
      --dir string       Target dir (current directory if empty)
  -h, --help             help for restore
      --token string     The access token to use for the authentication.
```

### SEE ALSO

* [regatta](regatta.md)	 - Regatta is read-optimized distributed key-value store.

