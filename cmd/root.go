package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/spf13/viper"

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

func init() {
	// Root flags
	rootCmd.PersistentFlags().Bool("dev-mode", false, "Dev mode enabled (verbose logging, human-friendly log format).")
	rootCmd.PersistentFlags().String("log-level", "DEBUG", "Log level: DEBUG/INFO/WARN/ERROR.")

	// API flags
	rootCmd.PersistentFlags().String("api.address", "localhost:8443", "Address the API server should listen on.")
	rootCmd.PersistentFlags().String("api.cert-filename", "hack/server.crt", "Path to the API server certificate.")
	rootCmd.PersistentFlags().String("api.key-filename", "hack/server.key", "Path to the API server private key file.")
	rootCmd.PersistentFlags().Bool("api.reflection-api", false, "Whether reflection API is provided. Should not be turned on in production.")

	// Raft flags
	rootCmd.PersistentFlags().String("raft.wal-dir", "",
		`WALDir is the directory used for storing the WAL of Raft entries. 
It is recommended to use low latency storage such as NVME SSD with power loss protection to store such WAL data. 
Leave WALDir to have zero value will have everything stored in NodeHostDir.`)
	rootCmd.PersistentFlags().String("raft.node-host-dir", "/tmp/regatta/raft", "NodeHostDir raft internal storage")
	rootCmd.PersistentFlags().String("raft.state-machine-wal-dir", "",
		`StateMachineWalDir persistent storage for the state machine. If empty all state machine data is stored in state-machine-dir. 
Applicable only when in-memory-state-machine=false.`)
	rootCmd.PersistentFlags().String("raft.state-machine-dir", "/tmp/regatta/state-machine",
		"StateMachineDir persistent storage for the state machine. Applicable only when in-memory-state-machine=false.")
	rootCmd.PersistentFlags().String("raft.address", "",
		`RaftAddress is a hostname:port or IP:port address used by the Raft RPC module for exchanging Raft messages and snapshots.
This is also the identifier for a Storage instance. RaftAddress should be set to the public address that can be accessed from remote Storage instances.`)
	rootCmd.PersistentFlags().String("raft.listen-address", "",
		`ListenAddress is a hostname:port or IP:port address used by the Raft RPC module to listen on for Raft message and snapshots.
When the ListenAddress field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0 is specified as the IP of the ListenAddress, Regatta listens to the specified port on all interfaces.
When hostname or domain name is specified, it is locally resolved to IP addresses first and Regatta listens to all resolved IP addresses.`)
	rootCmd.PersistentFlags().Uint64("raft.node-id", 1, "Raft Node ID is a non-zero value used to identify a node within a Raft cluster.")
	rootCmd.PersistentFlags().Uint64("raft.cluster-id", 1, "Raft Cluster ID is the unique value used to identify a Raft cluster.")
	rootCmd.PersistentFlags().StringToString("raft.initial-members", map[string]string{}, `Raft cluster initial members defines a mapping of node IDs to their respective raft address.
The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014".`)

	// Kafka flags
	rootCmd.PersistentFlags().StringSlice("kafka.brokers", []string{"localhost:9092"}, "Address of the Kafka broker.")
	rootCmd.PersistentFlags().Duration("kafka.timeout", 10*time.Second, "Kafka dialer timeout.")
	rootCmd.PersistentFlags().String("kafka.group-id", "regatta-local", "Kafka consumer group ID.")
	rootCmd.PersistentFlags().StringSlice("kafka.topics", nil, "Kafka topics to read from.")
	rootCmd.PersistentFlags().Bool("kafka.tls", false, "Enables Kafka broker TLS connection.")
	rootCmd.PersistentFlags().String("kafka.server-cert-filename", "", "Kafka broker CA.")
	rootCmd.PersistentFlags().String("kafka.client-cert-filename", "", "Kafka client certificate.")
	rootCmd.PersistentFlags().String("kafka.client-key-filename", "", "Kafka client key.")

	cobra.OnInitialize(initConfig)
}

var rootCmd = &cobra.Command{
	Use:     "regatta",
	Short:   "Regatta is read-optimized distributed key-value store.",
	Run:     root,
	PreRunE: validateConfig,
}

func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/regatta/")
	viper.AddConfigPath("/config")
	viper.AddConfigPath("$HOME/.regatta")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	err := viper.BindPFlags(rootCmd.PersistentFlags())
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

func validateConfig(_ *cobra.Command, _ []string) error {
	if !viper.IsSet("raft.address") {
		return errors.New("raft address must be set")
	}
	return nil
}

func initialMembers(log *zap.SugaredLogger) map[uint64]string {
	initialMembers := make(map[uint64]string)
	for kStr, v := range viper.GetStringMapString("raft.initial-members") {
		kUint, err := strconv.ParseUint(kStr, 10, 64)
		if err != nil {
			log.Panicf("cluster node ID in \"raft.initial-members\" must be integer: %v", err)
		}
		initialMembers[kUint] = v
	}
	return initialMembers
}

func root(_ *cobra.Command, _ []string) {
	logger := buildLogger()
	defer logger.Sync()
	dragonboatlogger.SetLoggerFactory(raft.NewLogger)
	log := zap.S().Named("root")

	metadata := &raft.Metadata{}
	nhc := config.NodeHostConfig{
		WALDir:            viper.GetString("raft.wal-dir"),
		NodeHostDir:       viper.GetString("raft.node-host-dir"),
		RTTMillisecond:    50,
		RaftAddress:       viper.GetString("raft.address"),
		ListenAddress:     viper.GetString("raft.listen-address"),
		EnableMetrics:     true,
		RaftEventListener: metadata,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Panic(err)
	}

	cfg := config.Config{
		NodeID:                  viper.GetUint64("raft.node-id"),
		ClusterID:               viper.GetUint64("raft.cluster-id"),
		CheckQuorum:             true,
		ElectionRTT:             20,
		HeartbeatRTT:            1,
		SnapshotEntries:         100000,
		CompactionOverhead:      50000,
		SnapshotCompressionType: config.Snappy,
		MaxInMemLogSize:         64 * 1024 * 1024,
	}

	err = nh.StartOnDiskCluster(
		initialMembers(log),
		false,
		func(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
			return raft.NewPebbleStateMachine(
				clusterID,
				nodeID,
				viper.GetString("raft.state-machine-dir"),
				viper.GetString("raft.state-machine-wal"),
				nil,
			)
		},
		cfg,
	)

	if err != nil {
		log.Panicf("failed to start Raft cluster: %v", err)
	}

	// Create storage
	st := &storage.Raft{
		NodeHost: nh,
		Metadata: metadata,
		Session:  nh.GetNoOPSession(viper.GetUint64("raft.cluster-id")),
	}

	// Create regatta server
	regatta := regattaserver.NewServer(
		viper.GetString("api.address"),
		viper.GetString("api.cert-filename"),
		viper.GetString("api.key-filename"),
		viper.GetBool("api.reflection-api"),
	)

	// Create and register grpc/rest endpoints
	mTables := viper.GetStringSlice("kafka.topics")
	kvs := &regattaserver.KVServer{
		Storage:       st,
		ManagedTables: mTables,
	}
	if err := kvs.Register(regatta); err != nil {
		log.Panicf("registerKVServer failed: %v", err)
	}
	ms := &regattaserver.MaintenanceServer{
		Storage: st,
	}
	if err := ms.Register(regatta); err != nil {
		log.Panicf("registerMaintenanceServer failed: %v", err)
	}

	// Start server
	go func() {
		if err := regatta.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("listenAndServe failed: %v", err)
		}
	}()

	var tc []kafka.TopicConfig
	for _, topic := range viper.GetStringSlice("kafka.topics") {
		tc = append(tc, kafka.TopicConfig{
			Name:    topic,
			GroupID: viper.GetString("kafka.group-id"),
			Table:   topic,
		})
	}
	kafkaCfg := kafka.Config{
		Brokers:            viper.GetStringSlice("kafka.brokers"),
		DialerTimeout:      viper.GetDuration("kafka.timeout"),
		TLS:                viper.GetBool("kafka.tls"),
		ServerCertFilename: viper.GetString("kafka.server-cert-filename"),
		ClientCertFilename: viper.GetString("kafka.client-cert-filename"),
		ClientKeyFilename:  viper.GetString("kafka.client-key-filename"),
		Topics:             tc,
		DebugLogs:          false,
	}

	// Start Kafka consumer
	consumer, err := kafka.NewConsumer(kafkaCfg, onMessage(st))
	if err != nil {
		log.Panicf("failed to create consumer: %v", err)
	}

	log.Info("Start consuming...")
	if err := consumer.Start(context.Background()); err != nil {
		log.Panicf("failed to start consumer: %v", err)
	}

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Cleanup
	<-shutdown
	consumer.Close()
	_ = regatta.Shutdown(context.Background(), 30*time.Second)
	nh.Stop()
}

func buildLogger() *zap.Logger {
	logCfg := zap.NewProductionConfig()
	if viper.GetBool("dev-mode") {
		logCfg = zap.NewDevelopmentConfig()
	}

	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	var level zapcore.Level
	if err := level.Set(viper.GetString("log-level")); err != nil {
		panic(err)
	}
	logCfg.Level.SetLevel(level)
	logger, err := logCfg.Build()
	if err != nil {
		zap.S().Panicf("failed to build logger: %v", err)
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
