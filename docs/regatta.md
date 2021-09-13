## regatta

Regatta is read-optimized distributed key-value store.

### Synopsis

Regatta can be run in two modes Leader and Follower, in the Leader mode write API is enabled 
and the node (or cluster of leader nodes) acts as a source of truth for the Follower nodes/clusters. In the Follower mode 
write API is disabled and the node or cluster of nodes replicates the writes done to the Leader cluster to which the
Follower one is connected to.

### Options

```
  -h, --help   help for regatta
```

### SEE ALSO

* [regatta backup](regatta_backup.md)	 - Backup regatta to local files
* [regatta follower](regatta_follower.md)	 - Start Regatta in follower mode
* [regatta leader](regatta_leader.md)	 - Start Regatta in leader mode
* [regatta restore](regatta_restore.md)	 - Restore regatta from local files

