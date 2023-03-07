---
title: regatta
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta

Regatta is a read-optimized distributed key-value store.

### Synopsis

Regatta can be run in two modes -- leader and follower. Write API is enabled in the leader mode
and the node (or cluster of leader nodes) acts as a source of truth for the follower nodes/clusters.
Write API is disabled in the follower mode and the follower node or cluster of follower nodes replicate the writes
done to the leader cluster to which the follower is connected to.

### Options

```
  -h, --help   help for regatta
```

### SEE ALSO

* [regatta backup](regatta_backup)	 - Backup Regatta to local files.
* [regatta follower](regatta_follower)	 - Start Regatta in follower mode.
* [regatta leader](regatta_leader)	 - Start Regatta in leader mode.
* [regatta restore](regatta_restore)	 - Restore Regatta from local files.
* [regatta version](regatta_version)	 - Print current version.

