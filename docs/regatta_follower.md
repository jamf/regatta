## regatta follower

Start Regatta in follower mode

```
regatta follower [flags]
```

### Options

```
      --api.address string                    Address the API server should listen on. (default ":8443")
      --api.cert-filename string              Path to the API server certificate. (default "hack/server.crt")
      --api.key-filename string               Path to the API server private key file. (default "hack/server.key")
      --api.reflection-api                    Whether reflection API is provided. Should not be turned on in production.
      --dev-mode                              Dev mode enabled (verbose logging, human-friendly log format).
  -h, --help                                  help for follower
      --kafka.brokers strings                 Address of the Kafka broker. (default [127.0.0.1:9092])
      --kafka.check-topics                    Enables checking if all "--kafka.topics" exist before kafka client connection attempt.
      --kafka.client-cert-filename string     Kafka client certificate.
      --kafka.client-key-filename string      Kafka client key.
      --kafka.debug-logs                      Enables kafka client debug logs. You need to set "--log-level" to "DEBUG", too.
      --kafka.group-id string                 Kafka consumer group ID. (default "regatta-local")
      --kafka.server-cert-filename string     Kafka broker CA.
      --kafka.timeout duration                Kafka dialer timeout. (default 10s)
      --kafka.tls                             Enables Kafka broker TLS connection.
      --kafka.topics strings                  Kafka topics to read from.
      --log-level string                      Log level: DEBUG/INFO/WARN/ERROR. (default "INFO")
      --raft.address string                   RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
                                              This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.
      --raft.compaction-overhead uint         CompactionOverhead defines the number of most recent entries to keep after each Raft log compaction.
                                              Raft log compaction is performed automatically every time when a snapshot is created. (default 5000)
      --raft.election-rtt int                 ElectionRTT is the minimum number of message RTT between elections. Message RTT is defined by NodeHostConfig.RTTMillisecond. 
                                              The Raft paper suggests it to be a magnitude greater than HeartbeatRTT, which is the interval between two heartbeats. In Raft, the actual interval between elections is randomized to be between ElectionRTT and 2 * ElectionRTT.
                                              As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the election interval to be 1 second, then ElectionRTT should be set to 10.
                                              When CheckQuorum is enabled, ElectionRTT also defines the interval for checking leader quorum. (default 20)
      --raft.heartbeat-rtt int                HeartbeatRTT is the number of message RTT between heartbeats. Message RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the heartbeat interval to be close to the average RTT between nodes.
                                              As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the heartbeat interval to be every 200 milliseconds, then HeartbeatRTT should be set to 2. (default 1)
      --raft.initial-members stringToString   Raft cluster initial members defines a mapping of node IDs to their respective raft address.
                                              The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014". (default [])
      --raft.listen-address string            ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
                                              When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
                                              When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.
      --raft.max-in-mem-log-size uint         MaxInMemLogSize is the target size in bytes allowed for storing in memory Raft logs on each Raft node.
                                              In memory Raft logs are the ones that have not been applied yet. (default 6291456)
      --raft.node-host-dir string             NodeHostDir raft internal storage (default "/tmp/regatta/raft")
      --raft.node-id uint                     Raft Node ID is a non-zero value used to identify a node within a Raft cluster. (default 1)
      --raft.rtt duration                     RTTMillisecond defines the average Round Trip Time (RTT) between two NodeHost instances.
                                              Such a RTT interval is internally used as a logical clock tick, Raft heartbeat and election intervals are both defined in term of how many such RTT intervals.
                                              Note that RTTMillisecond is the combined delays between two NodeHost instances including all delays caused by network transmission, delays caused by NodeHost queuing and processing. (default 50ms)
      --raft.snapshot-entries uint            SnapshotEntries defines how often the state machine should be snapshotted automatically.
                                              It is defined in terms of the number of applied Raft log entries.
                                              SnapshotEntries can be set to 0 to disable such automatic snapshotting. (default 10000)
      --raft.state-machine-dir string         StateMachineDir persistent storage for the state machine. Applicable only when in-memory-state-machine=false. (default "/tmp/regatta/state-machine")
      --raft.state-machine-wal-dir string     StateMachineWalDir persistent storage for the state machine. If empty all state machine data is stored in state-machine-dir. 
                                              Applicable only when in-memory-state-machine=false.
      --raft.wal-dir string                   WALDir is the directory used for storing the WAL of Raft entries. 
                                              It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
                                              Leave WALDir to have zero value will have everything stored in NodeHostDir.
      --replication.ca-filename string        Path to the client CA cert file. (default "hack/replication/ca.crt")
      --replication.cert-filename string      Path to the client certificate. (default "hack/replication/client.crt")
      --replication.key-filename string       Path to the client private key file. (default "hack/replication/client.key")
      --replication.leader-address string     Address of the leader replication API to connect to. (default "localhost:8444")
      --rest.address string                   Address the REST API server should listen on. (default ":8079")
```

### SEE ALSO

* [regatta](regatta.md)	 - Regatta is read-optimized distributed key-value store.
