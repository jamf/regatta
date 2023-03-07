---
title: regatta leader
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta leader

Start Regatta in leader mode.

```
regatta leader [flags]
```

### Options

```
      --api.address string                             API server address. (default ":8443")
      --api.cert-filename string                       Path to the API server certificate. (default "hack/server.crt")
      --api.key-filename string                        Path to the API server private key file. (default "hack/server.key")
      --api.reflection-api                             Whether reflection API is enabled. Should be disabled in production.
      --dev-mode                                       Development mode enabled (verbose logging, human-friendly log format).
      --experimental.tanlogdb                          Whether experimental LogDB implementation Tan is used in-place of Pebble based one.
  -h, --help                                           help for leader
      --log-level string                               Log level: DEBUG/INFO/WARN/ERROR. (default "INFO")
      --maintenance.address string                     Replication API server address. (default ":8445")
      --maintenance.cert-filename string               Path to the API server certificate. (default "hack/replication/server.crt")
      --maintenance.enabled                            Whether maintenance API is enabled. (default true)
      --maintenance.key-filename string                Path to the API server private key file. (default "hack/replication/server.key")
      --maintenance.token string                       Token to check for maintenance API access, if left empty (default) no token is checked.
      --raft.address string                            RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
                                                       This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.
      --raft.compaction-overhead uint                  CompactionOverhead defines the number of most recent entries to keep after each Raft log compaction.
                                                       Raft log compaction is performed automatically every time when a snapshot is created. (default 5000)
      --raft.election-rtt int                          ElectionRTT is the minimum number of message RTT between elections. Message RTT is defined by NodeHostConfig.RTTMillisecond. 
                                                       The Raft paper suggests it to be a magnitude greater than HeartbeatRTT, which is the interval between two heartbeats. In Raft, the actual interval between elections is randomized to be between ElectionRTT and 2 * ElectionRTT.
                                                       As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the election interval to be 1 second, then ElectionRTT should be set to 10.
                                                       When CheckQuorum is enabled, ElectionRTT also defines the interval for checking leader quorum. (default 20)
      --raft.heartbeat-rtt int                         HeartbeatRTT is the number of message RTT between heartbeats. Message RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the heartbeat interval to be close to the average RTT between nodes.
                                                       As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the heartbeat interval to be every 200 milliseconds, then HeartbeatRTT should be set to 2. (default 1)
      --raft.initial-members stringToString            Raft cluster initial members defines a mapping of node IDs to their respective raft address.
                                                       The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014". (default [])
      --raft.listen-address string                     ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
                                                       When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
                                                       When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.
      --raft.max-in-mem-log-size uint                  MaxInMemLogSize is the target size in bytes allowed for storing in memory Raft logs on each Raft node.
                                                       In memory Raft logs are the ones that have not been applied yet. (default 6291456)
      --raft.max-recv-queue-size uint                  MaxReceiveQueueSize is the maximum size in bytes of each receive queue. Once the maximum size is reached, further replication messages will be
                                                       dropped to restrict memory usage. When set to 0, it means the queue size is unlimited.
      --raft.max-send-queue-size uint                  MaxSendQueueSize is the maximum size in bytes of each send queue. Once the maximum size is reached, further replication messages will be
                                                       dropped to restrict memory usage. When set to 0, it means the send queue size is unlimited.
      --raft.node-host-dir string                      NodeHostDir raft internal storage (default "/tmp/regatta/raft")
      --raft.node-id uint                              Raft Node ID is a non-zero value used to identify a node within a Raft cluster. (default 1)
      --raft.rtt duration                              RTTMillisecond defines the average Round Trip Time (RTT) between two NodeHost instances.
                                                       Such a RTT interval is internally used as a logical clock tick, Raft heartbeat and election intervals are both defined in term of how many such RTT intervals.
                                                       Note that RTTMillisecond is the combined delays between two NodeHost instances including all delays caused by network transmission, delays caused by NodeHost queuing and processing. (default 50ms)
      --raft.snapshot-entries uint                     SnapshotEntries defines how often the state machine should be snapshotted automatically.
                                                       It is defined in terms of the number of applied Raft log entries.
                                                       SnapshotEntries can be set to 0 to disable such automatic snapshotting. (default 10000)
      --raft.snapshot-recovery-type string             Specifies the way how the snapshots should be shared between nodes within the cluster. Options: snapshot, checkpoint, default: checkpoint for non Windows systems. 
                                                       Type 'snapshot' uses in-memory snapshot of DB to send over wire to the peer. Type 'checkpoint'' uses hardlinks on FS a sends DB in tarball over wire. Checkpoint is thus much more memory and compute efficient at the potential expense of disk space, it is not advisable to use on OS/FS which does not support hardlinks.
      --raft.state-machine-dir string                  StateMachineDir persistent storage for the state machine. (default "/tmp/regatta/state-machine")
      --raft.wal-dir string                            WALDir is the directory used for storing the WAL of Raft entries. 
                                                       It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
                                                       Leave WALDir to have zero value will have everything stored in NodeHostDir.
      --replication.address string                     Replication API server address. (default ":8444")
      --replication.ca-filename string                 Path to the API server CA cert file. (default "hack/replication/ca.crt")
      --replication.cert-filename string               Path to the API server certificate. (default "hack/replication/server.crt")
      --replication.enabled                            Whether replication API is enabled. (default true)
      --replication.key-filename string                Path to the API server private key file. (default "hack/replication/server.key")
      --replication.log-cache-size int                 Size of the replication cache. (default 1024)
      --replication.max-send-message-size-bytes uint   The target maximum size of single replication message allowed to send.
                                                       Under some circumstances, a larger message could be sent. Followers should be able to accept slightly larger messages. (default 4194304)
      --rest.address string                            REST API server address. (default ":8079")
      --rest.read-timeout duration                     Maximum duration for reading the entire request. (default 5s)
      --storage.block-cache-size int                   Shared block cache size in bytes, the cache is used to hold uncompressed blocks of data in memory. (default 16777216)
      --tables.delete strings                          Delete Regatta tables with given names.
      --tables.names strings                           Create Regatta tables with given names.
```

### SEE ALSO

* [regatta](regatta)	 - Regatta is a read-optimized distributed key-value store.

