package cmd

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dragonboatlogger "github.com/lni/dragonboat/v3/logger"
	"github.com/spf13/cobra"
	"github.com/wandera/regatta/kafka"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/raft"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	devMode bool

	addr          string
	certFilename  string
	keyFilename   string
	logLevel      string
	reflectionAPI bool

	walDir             string
	nodeHostDir        string
	stateMachineWalDir string
	stateMachineDir    string
	raftAddress        string
	listenAddress      string
	raftID             uint64
	raftClusterID      uint64

	kafkaAddr string
	groupID   string
)

func init() {
	rootCmd.PersistentFlags().BoolVar(&devMode, "dev-mode", false, "Dev mode enabled (verbose logging, human-friendly log format).")

	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:8443", "Address the API server should listen on.")
	rootCmd.PersistentFlags().StringVar(&certFilename, "cert-filename", "hack/server.crt", "Path to the API server certificate.")
	rootCmd.PersistentFlags().StringVar(&keyFilename, "key-filename", "hack/server.key", "Path to the API server private key file.")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "DEBUG", "Log level: DEBUG/INFO/WARN/ERROR.")
	rootCmd.PersistentFlags().BoolVar(&reflectionAPI, "reflection-api", false, "Whether reflection API is provided. Should not be turned on in production.")

	rootCmd.PersistentFlags().StringVar(&walDir, "wal-dir", "",
		`WALDir is the directory used for storing the WAL of Raft entries. 
It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
Leave WALDir to have zero value will have everything stored in NodeHostDir.`)
	rootCmd.PersistentFlags().StringVar(&nodeHostDir, "node-host-dir", "/tmp/regatta/raft", "NodeHostDir raft internal storage")
	rootCmd.PersistentFlags().StringVar(&stateMachineWalDir, "state-machine-wal-dir", "",
		`StateMachineWalDir persistent storage for the state machine. If empty all state machine data is stored in state-machine-dir. 
Applicable only when in-memory-state-machine=false.`)
	rootCmd.PersistentFlags().StringVar(&stateMachineDir, "state-machine-dir", "/tmp/regatta/state-machine",
		"StateMachineDir persistent storage for the state machine. Applicable only when in-memory-state-machine=false.")
	rootCmd.PersistentFlags().StringVar(&raftAddress, "raft-address", "",
		`RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.`)
	_ = rootCmd.MarkPersistentFlagRequired("raft-address")
	rootCmd.PersistentFlags().StringVar(&listenAddress, "listen-address", "",
		`ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.`)
	rootCmd.PersistentFlags().Uint64Var(&raftID, "node-id", 1, "Raft Node ID is a non-zero value used to identify a node within a Raft cluster.")
	rootCmd.PersistentFlags().Uint64Var(&raftClusterID, "cluster-id", 1, "Raft Cluster ID is the unique value used to identify a Raft cluster.")

	// TODO config properly
	rootCmd.PersistentFlags().StringVar(&kafkaAddr, "kafka-addr", "localhost:9092", "Address of the Kafka broker.")
	rootCmd.PersistentFlags().StringVar(&groupID, "group-id", "regatta-local", "Kafka consumer group ID")
}

var rootCmd = &cobra.Command{
	Use:   "regatta",
	Short: "Regatta is read-optimized distributed key-value store.",
	Run: func(cmd *cobra.Command, args []string) {
		logger := buildLogger()
		defer logger.Sync()
		dragonboatlogger.SetLoggerFactory(raft.NewLogger)
		log := zap.S().Named("root")

		nhc := config.NodeHostConfig{
			WALDir:         walDir,
			NodeHostDir:    nodeHostDir,
			RTTMillisecond: 50,
			RaftAddress:    raftAddress,
			ListenAddress:  listenAddress,
			EnableMetrics:  true,
		}
		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}

		cfg := config.Config{
			NodeID:             raftID,
			ClusterID:          raftClusterID,
			ElectionRTT:        20,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		}

		err = nh.StartOnDiskCluster(map[uint64]string{raftID: raftAddress}, false, func(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
			return raft.NewPebbleStateMachine(clusterID, nodeID, stateMachineDir, stateMachineWalDir, nil)
		}, cfg)

		if err != nil {
			log.Fatalf("failed to start Raft cluster: %v", err)
		}

		// Create storage
		st := &storage.Raft{
			NodeHost: nh,
			Session:  nh.GetNoOPSession(raftClusterID),
		}

		// Create regatta server
		regatta := regattaserver.NewServer(addr, certFilename, keyFilename, reflectionAPI)

		// Create and register grpc/rest endpoints
		kvs := &regattaserver.KVServer{
			Storage: st,
		}
		if err := kvs.Register(regatta); err != nil {
			log.Fatalf("registerKVServer failed: %v", err)
		}
		ms := &regattaserver.MaintenanceServer{
			Storage: st,
		}
		if err := ms.Register(regatta); err != nil {
			log.Fatalf("registerMaintenanceServer failed: %v", err)
		}

		// Start server
		go func() {
			if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatalf("listenAndServe failed: %v", err)
			}
		}()

		// Start Kafka consumer
		// TODO config
		kafkaCfg := kafka.Config{
			Brokers: []string{kafkaAddr},
			TLS:     false,
			Topics: []kafka.TopicConfig{
				{
					Name:    "applicable-cellular-data-policy",
					GroupID: groupID,
					Table:   "applicable-cellular-data-policy",
				},
				{
					Name:    "applicable-wifi-data-policy",
					GroupID: groupID,
					Table:   "applicable-wifi-data-policy",
				},
			},
			DebugLogs: false,
		}

		consumer, err := kafka.NewConsumer(kafkaCfg, onMessage(st))
		if err != nil {
			log.Fatalf("Fail to create consumer: %v", err)
		}

		log.Info("Start consuming...")
		if err := consumer.Start(context.Background()); err != nil {
			log.Fatalf("Fail to start consumer: %v", err)
		}

		// Check signals
		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

		// Cleanup
		<-shutdown
		consumer.Close()
		_ = regatta.Shutdown(context.Background(), 30*time.Second)
		nh.Stop()
	},
}

func buildLogger() *zap.Logger {
	logCfg := zap.NewProductionConfig()
	if devMode {
		logCfg = zap.NewDevelopmentConfig()
	}

	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	var level zapcore.Level
	if err := level.Set(logLevel); err != nil {
		panic(err)
	}
	logCfg.Level.SetLevel(level)
	logger, err := logCfg.Build()
	if err != nil {
		zap.S().Fatal("failed to build logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	return logger
}

func onMessage(st *storage.Raft) kafka.OnMessageFunc {
	return func(ctx context.Context, table, key, value []byte) error {
		if value != nil {
			_, err := st.Put(ctx, &proto.PutRequest{
				Table: table,
				Key:   key,
				Value: value,
			})
			return err
		}
		_, err := st.Delete(ctx, &proto.DeleteRangeRequest{
			Table: table,
			Key:   key,
		})
		return err
	}
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
