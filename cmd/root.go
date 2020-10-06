package cmd

import (
	"net/http"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dragonboatlogger "github.com/lni/dragonboat/v3/logger"
	"github.com/spf13/cobra"
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

	walDir        string
	nodeHostDir   string
	raftAddress   string
	listenAddress string
	raftID        uint64
	raftClusterID uint64
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
	rootCmd.PersistentFlags().StringVar(&nodeHostDir, "node-host-dir", "/tmp/regatta", "NodeHostDir is where everything else is stored")
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
}

var rootCmd = &cobra.Command{
	Use:   "regatta",
	Short: "Regatta is read-optimized distributed key-value store.",
	Run: func(cmd *cobra.Command, args []string) {
		logger := buildLogger()
		defer logger.Sync()
		dragonboatlogger.SetLoggerFactory(raft.NewLogger)

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
		err = nh.StartCluster(map[uint64]string{raftID: raftAddress}, false, raft.NewStateMachine, config.Config{
			NodeID:             raftID,
			ClusterID:          raftClusterID,
			ElectionRTT:        20,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		})
		if err != nil {
			zap.S().Fatal("Failed to start Raft cluster: %v", err)
		}

		// Create storage
		st := &storage.RaftStorage{
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
			zap.S().Fatalf("registerKVServer failed: %v", err)
		}
		ms := &regattaserver.MaintenanceServer{
			Storage: st,
		}
		if err := ms.Register(regatta); err != nil {
			zap.S().Fatalf("registerMaintenanceServer failed: %v", err)
		}

		// Start serving
		if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
			zap.S().Fatalf("ListenAndServe failed: %v", err)
		}
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
		zap.S().Fatal("Failed to build logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	return logger
}

// Execute cobra command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
