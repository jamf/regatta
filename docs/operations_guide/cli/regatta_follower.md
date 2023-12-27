---
title: regatta follower
layout: default
parent: CLI Documentation
grand_parent: Operations Guide
---
## regatta follower

Start Regatta in follower mode.

```
regatta follower [flags]
```

### Options

```
      --api.address string                                    API server address. The address the server listens on. (default "http://0.0.0.0:8443")
      --api.advertise-address string                          Advertise API server address, used for NAT traversal. (default "http://127.0.0.1:8443")
      --api.cert-filename string                              Path to the API server certificate.
      --api.key-filename string                               Path to the API server private key file.
      --dev-mode                                              Development mode enabled (verbose logging, human-friendly log format).
  -h, --help                                                  help for follower
      --log-level string                                      Log level: DEBUG/INFO/WARN/ERROR. (default "INFO")
      --maintenance.address string                            Replication API server address. (default "http://127.0.0.1:8445")
      --maintenance.cert-filename string                      Path to the API server certificate.
      --maintenance.enabled                                   Whether maintenance API is enabled. (default true)
      --maintenance.key-filename string                       Path to the API server private key file.
      --maintenance.token string                              Token to check for maintenance API access, if left empty (default) no token is checked.
      --memberlist.address string                             Address is the address for the gossip service to bind to and listen on. Both UDP and TCP ports are used by the gossip service.
                                                              The local gossip service should be able to receive gossip service related messages by binding to and listening on this address. BindAddress is usually in the format of IP:Port, Hostname:Port or DNS Name:Port. (default "0.0.0.0:7432")
      --memberlist.advertise-address string                   AdvertiseAddress is the address to advertise to other Regatta instances used for NAT traversal.
                                                              Gossip services running on remote Regatta instances will use AdvertiseAddress to exchange gossip service related messages. AdvertiseAddress is in the format of IP:Port, Hostname:Port or DNS Name:Port.
      --memberlist.members strings                            Seed is a list of AdvertiseAddress of remote Regatta instances. Local Regatta instance will try to contact all of them to bootstrap the gossip service. 
                                                              At least one reachable Regatta instance is required to successfully bootstrap the gossip service. Each seed address is in the format of IP:Port, Hostname:Port or DNS Name:Port.
      --raft.address string                                   RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
                                                              This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.
      --raft.compaction-overhead uint                         CompactionOverhead defines the number of most recent entries to keep after each Raft log compaction.
                                                              Raft log compaction is performed automatically every time when a snapshot is created. (default 5000)
      --raft.election-rtt int                                 ElectionRTT is the minimum number of message RTT between elections. Message RTT is defined by NodeHostConfig.RTTMillisecond. 
                                                              The Raft paper suggests it to be a magnitude greater than HeartbeatRTT, which is the interval between two heartbeats. In Raft, the actual interval between elections is randomized to be between ElectionRTT and 2 * ElectionRTT.
                                                              As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the election interval to be 1 second, then ElectionRTT should be set to 10.
                                                              When CheckQuorum is enabled, ElectionRTT also defines the interval for checking leader quorum. (default 20)
      --raft.heartbeat-rtt int                                HeartbeatRTT is the number of message RTT between heartbeats. Message RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the heartbeat interval to be close to the average RTT between nodes.
                                                              As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the heartbeat interval to be every 200 milliseconds, then HeartbeatRTT should be set to 2. (default 1)
      --raft.initial-members stringToString                   Raft cluster initial members defines a mapping of node IDs to their respective raft address.
                                                              The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014". (default [])
      --raft.listen-address string                            ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
                                                              When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
                                                              When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.
      --raft.logdb string                                     Log DB implementation to use for storage of Raft log. 
                                                              Due to higher performance and lower resource consumption Tan should be preferred, use Pebble only for backward compatibility. (options: pebble, tan) (default "tan")
      --raft.max-in-mem-log-size uint                         MaxInMemLogSize is the target size in bytes allowed for storing in memory Raft logs on each Raft node.
                                                              In memory Raft logs are the ones that have not been applied yet. (default 6291456)
      --raft.max-recv-queue-size uint                         MaxReceiveQueueSize is the maximum size in bytes of each receive queue. Once the maximum size is reached, further replication messages will be
                                                              dropped to restrict memory usage. When set to 0, it means the queue size is unlimited.
      --raft.max-send-queue-size uint                         MaxSendQueueSize is the maximum size in bytes of each send queue. Once the maximum size is reached, further replication messages will be
                                                              dropped to restrict memory usage. When set to 0, it means the send queue size is unlimited.
      --raft.node-host-dir string                             NodeHostDir raft internal storage (default "/tmp/regatta/raft")
      --raft.node-id uint                                     Raft Node ID is a non-zero value used to identify a node within a Raft cluster. (default 1)
      --raft.rtt duration                                     RTTMillisecond defines the average Round Trip Time (RTT) between two NodeHost instances.
                                                              Such a RTT interval is internally used as a logical clock tick, Raft heartbeat and election intervals are both defined in term of how many such RTT intervals.
                                                              Note that RTTMillisecond is the combined delays between two NodeHost instances including all delays caused by network transmission, delays caused by NodeHost queuing and processing. (default 50ms)
      --raft.snapshot-entries uint                            SnapshotEntries defines how often the state machine should be snapshot automatically.
                                                              It is defined in terms of the number of applied Raft log entries.
                                                              SnapshotEntries can be set to 0 to disable such automatic snapshotting. (default 10000)
      --raft.snapshot-recovery-type string                    Specifies the way how the snapshots should be shared between nodes within the cluster. Options: snapshot, checkpoint, default: checkpoint for non Windows systems. 
                                                              Type 'snapshot' uses in-memory snapshot of DB to send over wire to the peer. Type 'checkpoint'' uses hardlinks on FS a sends DB in tarball over wire. Checkpoint is thus much more memory and compute efficient at the potential expense of disk space, it is not advisable to use on OS/FS which does not support hardlinks.
      --raft.state-machine-dir string                         StateMachineDir persistent storage for the state machine. (default "/tmp/regatta/state-machine")
      --raft.wal-dir string                                   WALDir is the directory used for storing the WAL of Raft entries. 
                                                              It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
                                                              Leave WALDir to have zero value will have everything stored in NodeHostDir.
      --replication.ca-filename string                        Path to the client CA cert file. (default "hack/replication/ca.crt")
      --replication.cert-filename string                      Path to the client certificate. (default "hack/replication/client.crt")
      --replication.keepalive-time duration                   After a duration of this time if the replication client doesn't see any activity it pings the server to see if the transport is still alive. If set below 10s, a minimum value of 10s will be used instead. (default 1m0s)
      --replication.keepalive-timeout duration                After having pinged for keepalive check, the replication client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. (default 10s)
      --replication.key-filename string                       Path to the client private key file. (default "hack/replication/client.key")
      --replication.leader-address string                     Address of the leader replication API to connect to. (default "localhost:8444")
      --replication.lease-interval duration                   Interval in which the workers re-new their table leases. (default 15s)
      --replication.log-rpc-timeout duration                  The log RPC timeout. (default 1m0s)
      --replication.max-recovery-in-flight uint               The maximum number of recovery goroutines allowed to run in this instance. (default 1)
      --replication.max-recv-message-size-bytes uint          The maximum size of single replication message allowed to receive. (default 8388608)
      --replication.max-snapshot-recv-bytes-per-second uint   Maximum bytes per second received by the snapshot API client, default value 0 means unlimited.
      --replication.poll-interval duration                    Replication interval in seconds, the leader poll time. (default 1s)
      --replication.reconcile-interval duration               Replication interval of tables reconciliation (workers startup/shutdown). (default 30s)
      --replication.snapshot-rpc-timeout duration             The snapshot RPC timeout. (default 1h0m0s)
      --rest.address string                                   REST API server address. (default "http://127.0.0.1:8079")
      --rest.read-timeout duration                            Maximum duration for reading the entire request. (default 5s)
      --storage.block-cache-size int                          Shared block cache size in bytes, the cache is used to hold uncompressed blocks of data in memory. (default 16777216)
      --storage.table-cache-size int                          Shared table cache size, the cache is used to hold handles to open SSTs. (default 1024)
```

### SEE ALSO

* [regatta](regatta.md)	 - Regatta is a read-optimized distributed key-value store.

