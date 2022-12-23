---
title: regatta
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
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

* [regatta backup](/operations/cli/regatta_backup)	 - Backup regatta to local files
* [regatta follower](/operations/cli/regatta_follower)	 - Start Regatta in follower mode
* [regatta leader](/operations/cli/regatta_leader)	 - Start Regatta in leader mode
* [regatta restore](/operations/cli/regatta_restore)	 - Restore regatta from local files
* [regatta version](/operations/cli/regatta_version)	 - Print current version.

