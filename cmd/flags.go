package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	rootFlagSet  = pflag.NewFlagSet("root", pflag.ContinueOnError)
	apiFlagSet   = pflag.NewFlagSet("api", pflag.ContinueOnError)
	restFlagSet  = pflag.NewFlagSet("rest", pflag.ContinueOnError)
	raftFlagSet  = pflag.NewFlagSet("raft", pflag.ContinueOnError)
	kafkaFlagSet = pflag.NewFlagSet("kafka", pflag.ContinueOnError)
)

func init() {
	// Root flags
	rootFlagSet.Bool("dev-mode", false, "Dev mode enabled (verbose logging, human-friendly log format).")
	rootFlagSet.String("log-level", "INFO", "Log level: DEBUG/INFO/WARN/ERROR.")

	// API flags
	apiFlagSet.String("api.address", "localhost:8443", "Address the API server should listen on.")
	apiFlagSet.String("api.cert-filename", "hack/server.crt", "Path to the API server certificate.")
	apiFlagSet.String("api.key-filename", "hack/server.key", "Path to the API server private key file.")
	apiFlagSet.Bool("api.reflection-api", false, "Whether reflection API is provided. Should not be turned on in production.")

	// REST API flags
	restFlagSet.String("rest.address", "localhost:8079", "Address the REST API server should listen on.")

	// Raft flags
	raftFlagSet.Duration("raft.rtt", 50*time.Millisecond,
		`RTTMillisecond defines the average Round Trip Time (RTT) between two NodeHost instances.
Such a RTT interval is internally used as a logical clock tick, Raft heartbeat and election intervals are both defined in term of how many such RTT intervals.
Note that RTTMillisecond is the combined delays between two NodeHost instances including all delays caused by network transmission, delays caused by NodeHost queuing and processing.`)
	raftFlagSet.Int("raft.heartbeat-rtt", 1,
		`HeartbeatRTT is the number of message RTT between heartbeats. Message RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the heartbeat interval to be close to the average RTT between nodes.
As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the heartbeat interval to be every 200 milliseconds, then HeartbeatRTT should be set to 2.`)
	raftFlagSet.Int("raft.election-rtt", 20,
		`ElectionRTT is the minimum number of message RTT between elections. Message RTT is defined by NodeHostConfig.RTTMillisecond. 
The Raft paper suggests it to be a magnitude greater than HeartbeatRTT, which is the interval between two heartbeats. In Raft, the actual interval between elections is randomized to be between ElectionRTT and 2 * ElectionRTT.
As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the election interval to be 1 second, then ElectionRTT should be set to 10.
When CheckQuorum is enabled, ElectionRTT also defines the interval for checking leader quorum.`)
	raftFlagSet.String("raft.wal-dir", "",
		`WALDir is the directory used for storing the WAL of Raft entries. 
It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
Leave WALDir to have zero value will have everything stored in NodeHostDir.`)
	raftFlagSet.String("raft.node-host-dir", "/tmp/regatta/raft", "NodeHostDir raft internal storage")
	raftFlagSet.String("raft.state-machine-wal-dir", "",
		`StateMachineWalDir persistent storage for the state machine. If empty all state machine data is stored in state-machine-dir. 
Applicable only when in-memory-state-machine=false.`)
	raftFlagSet.String("raft.state-machine-dir", "/tmp/regatta/state-machine",
		"StateMachineDir persistent storage for the state machine. Applicable only when in-memory-state-machine=false.")
	raftFlagSet.String("raft.address", "",
		`RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.`)
	raftFlagSet.String("raft.listen-address", "",
		`ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.`)
	raftFlagSet.Uint64("raft.node-id", 1, "Raft Node ID is a non-zero value used to identify a node within a Raft cluster.")
	raftFlagSet.StringToString("raft.initial-members", map[string]string{}, `Raft cluster initial members defines a mapping of node IDs to their respective raft address.
The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014".`)
	raftFlagSet.Uint64("raft.snapshot-entries", 10000,
		`SnapshotEntries defines how often the state machine should be snapshotted automatically.
It is defined in terms of the number of applied Raft log entries.
SnapshotEntries can be set to 0 to disable such automatic snapshotting.`)
	raftFlagSet.Uint64("raft.compaction-overhead", 5000,
		`CompactionOverhead defines the number of most recent entries to keep after each Raft log compaction.
Raft log compaction is performed automatically every time when a snapshot is created.`)
	raftFlagSet.Uint64("raft.max-in-mem-log-size", 6*1024*1024,
		`MaxInMemLogSize is the target size in bytes allowed for storing in memory Raft logs on each Raft node.
In memory Raft logs are the ones that have not been applied yet.`)

	// Kafka flags
	kafkaFlagSet.StringSlice("kafka.brokers", []string{"127.0.0.1:9092"}, "Address of the Kafka broker.")
	kafkaFlagSet.Duration("kafka.timeout", 10*time.Second, "Kafka dialer timeout.")
	kafkaFlagSet.String("kafka.group-id", "regatta-local", "Kafka consumer group ID.")
	kafkaFlagSet.StringSlice("kafka.topics", nil, "Kafka topics to read from.")
	kafkaFlagSet.Bool("kafka.tls", false, "Enables Kafka broker TLS connection.")
	kafkaFlagSet.String("kafka.server-cert-filename", "", "Kafka broker CA.")
	kafkaFlagSet.String("kafka.client-cert-filename", "", "Kafka client certificate.")
	kafkaFlagSet.String("kafka.client-key-filename", "", "Kafka client key.")
	kafkaFlagSet.Bool("kafka.check-topics", false, `Enables checking if all "--kafka.topics" exist before kafka client connection attempt.`)
	kafkaFlagSet.Bool("kafka.debug-logs", false, `Enables kafka client debug logs. You need to set "--log-level" to "DEBUG", too.`)
}

func initConfig(set *pflag.FlagSet) {
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/regatta/")
	viper.AddConfigPath("/config")
	viper.AddConfigPath("$HOME/.regatta")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	err := viper.BindPFlags(set)
	if err != nil {
		panic(fmt.Errorf("error binding pflags %v", err))
	}

	err = viper.ReadInConfig()
	if err != nil {
		switch t := err.(type) {
		case viper.ConfigFileNotFoundError:
			fmt.Println("No config file found, using flags and defaults")
		default:
			panic(fmt.Errorf("error reading config %v", t))
		}
	}
}