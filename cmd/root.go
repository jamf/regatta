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

	walDir      string
	nodeHostDir string
	raftAddress string
)

func init() {
	rootCmd.PersistentFlags().BoolVar(&devMode, "dev-mode", false, "Dev mode enabled (verbose logging, human-friendly log format)")

	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:8443", "addr to listen on")
	rootCmd.PersistentFlags().StringVar(&certFilename, "cert-filename", "hack/server.crt", "path to the certificate")
	rootCmd.PersistentFlags().StringVar(&keyFilename, "key-filename", "hack/server.key", "path to the certificate key file")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "DEBUG", "log level: DEBUG/INFO/WARN/ERROR")
	rootCmd.PersistentFlags().BoolVar(&reflectionAPI, "reflection-api", false, "whether reflection API is provided. Should not be turned on in production.")

	rootCmd.PersistentFlags().StringVar(&walDir, "wal-dir", "", "WALDir is the directory used for storing the WAL of Raft entries. It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. Leave WALDir to have zero value will have everything stored in NodeHostDir.")
	rootCmd.PersistentFlags().StringVar(&nodeHostDir, "node-host-dir", "/tmp/regatta", "NodeHostDir is where everything else is stored")
	rootCmd.PersistentFlags().StringVar(&raftAddress, "raft-address", "", "RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots. This is also the identifier for a NodeHost instance. RaftAddress should be set to the public address that can be accessed from remote NodeHost instances.")
	_ = rootCmd.MarkPersistentFlagRequired("raft-address")
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
			RTTMillisecond: 200,
			RaftAddress:    raftAddress,
			EnableMetrics:  true,
		}
		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}
		err = nh.StartCluster(map[uint64]string{1: "127.0.0.1:5012"}, false, raft.NewStateMachine, config.Config{
			NodeID:             1,
			ClusterID:          100,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		})
		if err != nil {
			zap.S().Fatal("Failed to start Raft cluster: %v", err)
		}

		// Create storage
		var storage storage.SimpleStorage
		storage.Reset()
		storage.PutDummyData()

		// Create regatta server
		regatta := regattaserver.NewServer(addr, certFilename, keyFilename, reflectionAPI)

		// Create and register grpc/rest endpoints
		kvs := &regattaserver.KVServer{
			Storage: &storage,
		}
		if err := kvs.Register(regatta); err != nil {
			zap.S().Fatalf("registerKVServer failed: %v", err)
		}
		ms := &regattaserver.MaintenanceServer{
			Storage: &storage,
		}
		if err := ms.Register(regatta); err != nil {
			zap.S().Fatalf("registerMaintenanceServer failed: %v", err)
		}

		// Start serving
		if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
			zap.S().Fatalf("ListenAndServe failed: %v", err)
		}
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
