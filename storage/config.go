// Copyright JAMF Software, LLC

package storage

import (
	"github.com/jamf/regatta/storage/tables"
	"go.uber.org/zap"
)

type LogDBImplementation int

const (
	Pebble LogDBImplementation = iota
	Tan
	Default = Pebble
)

type TableConfig tables.TableConfig

type MetaConfig tables.MetaConfig

type Config struct {
	Logger *zap.Logger
	// NodeID is a non-zero value used to identify a node within a Raft cluster.
	NodeID uint64
	// InitialMembers is a map of both meta and table clusters initial members.
	InitialMembers map[uint64]string
	// WALDir is the directory used for storing the WAL of Raft entries. It is
	// recommended to use low latency storage such as NVME SSD with power loss
	// protection to store such WAL data. WAL will be stored in
	// NodeHostDir if it is set to the zero value.
	WALDir string
	// NodeHostDir is where everything else is stored.
	NodeHostDir string
	// RTTMillisecond defines the average Round Trip Time (RTT) in milliseconds
	// between two NodeHost instances. Such a RTT interval is internally used as
	// a logical clock tick. Raft heartbeat and election intervals are both
	// defined in terms of such logical clock ticks (RTT intervals).
	// Note that RTTMillisecond is the combined delays between two NodeHost
	// instances including all delays caused by network transmission, delays
	// caused by NodeHost queuing and processing. As an example, when fully
	// loaded, the average Rround Trip Time between two of our NodeHost instances
	// used for benchmarking purposes is up to 500 microseconds when the ping time
	// between them is 100 microseconds. Set RTTMillisecond to 1 when it is less
	// than 1 million in your environment.
	RTTMillisecond uint64
	// RaftAddress is a DNS name:port or IP:port address used by the transport
	// module for exchanging Raft messages, snapshots and metadata between
	// NodeHost instances. It should be set to the public address that can be
	// accessed from remote NodeHost instances.
	//
	// When the NodeHostConfig.ListenAddress field is empty, NodeHost listens on
	// RaftAddress for incoming Raft messages. When hostname or domain name is
	// used, it will be resolved to IPv4 addresses first and Dragonboat listens
	// to all resolved IPv4 addresses.
	//
	// By default, the RaftAddress value is not allowed to change between NodeHost
	// restarts. AddressByNodeHostID should be set to true when the RaftAddress
	// value might change after restart.
	RaftAddress string
	// ListenAddress is an optional field in the hostname:port or IP:port address
	// form used by the transport module to listen on for Raft message and
	// snapshots. When the ListenAddress field is not set, the transport module
	// listens on RaftAddress. If 0.0.0.0 is specified as the IP of the
	// ListenAddress, Dragonboat listens to the specified port on all network
	// interfaces. When hostname or domain name is used, it will be resolved to
	// IPv4 addresses first and Dragonboat listens to all resolved IPv4 addresses.
	ListenAddress string
	// EnableMetrics determines whether health metrics in Prometheus format should
	// be enabled.
	EnableMetrics bool
	// MaxSendQueueSize is the maximum size in bytes of each send queue.
	// Once the maximum size is reached, further replication messages will be
	// dropped to restrict memory usage. When set to 0, it means the send queue
	// size is unlimited.
	MaxSendQueueSize uint64
	// MaxReceiveQueueSize is the maximum size in bytes of each receive queue.
	// Once the maximum size is reached, further replication messages will be
	// dropped to restrict memory usage. When set to 0, it means the queue size
	// is unlimited.
	MaxReceiveQueueSize uint64
	// NotifyCommit specifies whether clients should be notified when their
	// regular proposals and config change requests are committed. By default,
	// commits are not notified, clients are only notified when their proposals
	// are both committed and applied.
	NotifyCommit bool
	// Table is a configuration for table OnDisk state machines.
	Table TableConfig
	// Meta is a configuration for metadata inmemory state machine.
	Meta MetaConfig
	// LogDBImplementation underlying LogDB implementation Pebble (default) or Tan.
	LogDBImplementation LogDBImplementation
	// LogCacheSize specifies the size of the log cache.
	LogCacheSize int
}
