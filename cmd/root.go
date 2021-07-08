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

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dbl "github.com/lni/dragonboat/v3/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	"github.com/wandera/regatta/kafka"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/raft"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	// Root flags
	rootCmd.PersistentFlags().Bool("dev-mode", false, "Dev mode enabled (verbose logging, human-friendly log format).")
	rootCmd.PersistentFlags().String("log-level", "INFO", "Log level: DEBUG/INFO/WARN/ERROR.")

	// API flags
	rootCmd.PersistentFlags().String("api.address", "localhost:8443", "Address the API server should listen on.")
	rootCmd.PersistentFlags().String("api.cert-filename", "hack/server.crt", "Path to the API server certificate.")
	rootCmd.PersistentFlags().String("api.key-filename", "hack/server.key", "Path to the API server private key file.")
	rootCmd.PersistentFlags().Bool("api.reflection-api", false, "Whether reflection API is provided. Should not be turned on in production.")

	// REST API flags
	rootCmd.PersistentFlags().String("rest.address", "localhost:8079", "Address the REST API server should listen on.")

	// Raft flags
	rootCmd.PersistentFlags().Duration("raft.rtt", 50*time.Millisecond,
		`RTTMillisecond defines the average Round Trip Time (RTT) between two NodeHost instances.
Such a RTT interval is internally used as a logical clock tick, Raft heartbeat and election intervals are both defined in term of how many such RTT intervals.
Note that RTTMillisecond is the combined delays between two NodeHost instances including all delays caused by network transmission, delays caused by NodeHost queuing and processing.`)
	rootCmd.PersistentFlags().Int("raft.heartbeat-rtt", 1,
		`HeartbeatRTT is the number of message RTT between heartbeats. Message RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the heartbeat interval to be close to the average RTT between nodes.
As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the heartbeat interval to be every 200 milliseconds, then HeartbeatRTT should be set to 2.`)
	rootCmd.PersistentFlags().Int("raft.election-rtt", 20,
		`ElectionRTT is the minimum number of message RTT between elections. Message RTT is defined by NodeHostConfig.RTTMillisecond. 
The Raft paper suggests it to be a magnitude greater than HeartbeatRTT, which is the interval between two heartbeats. In Raft, the actual interval between elections is randomized to be between ElectionRTT and 2 * ElectionRTT.
As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond, to set the election interval to be 1 second, then ElectionRTT should be set to 10.
When CheckQuorum is enabled, ElectionRTT also defines the interval for checking leader quorum.`)
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
	rootCmd.PersistentFlags().StringToString("raft.initial-members", map[string]string{}, `Raft cluster initial members defines a mapping of node IDs to their respective raft address.
The node ID must be must be Integer >= 1. Example for the initial 3 node cluster setup on the localhost: "--raft.initial-members=1=127.0.0.1:5012,2=127.0.0.1:5013,3=127.0.0.1:5014".`)
	rootCmd.PersistentFlags().Uint64("raft.snapshot-entries", 10000,
		`SnapshotEntries defines how often the state machine should be snapshotted automatically.
It is defined in terms of the number of applied Raft log entries.
SnapshotEntries can be set to 0 to disable such automatic snapshotting.`)
	rootCmd.PersistentFlags().Uint64("raft.compaction-overhead", 5000,
		`CompactionOverhead defines the number of most recent entries to keep after each Raft log compaction.
Raft log compaction is performed automatically every time when a snapshot is created.`)
	rootCmd.PersistentFlags().Uint64("raft.max-in-mem-log-size", 6*1024*1024,
		`MaxInMemLogSize is the target size in bytes allowed for storing in memory Raft logs on each Raft node.
In memory Raft logs are the ones that have not been applied yet.`)

	// Kafka flags
	rootCmd.PersistentFlags().StringSlice("kafka.brokers", []string{"127.0.0.1:9092"}, "Address of the Kafka broker.")
	rootCmd.PersistentFlags().Duration("kafka.timeout", 10*time.Second, "Kafka dialer timeout.")
	rootCmd.PersistentFlags().String("kafka.group-id", "regatta-local", "Kafka consumer group ID.")
	rootCmd.PersistentFlags().StringSlice("kafka.topics", nil, "Kafka topics to read from.")
	rootCmd.PersistentFlags().Bool("kafka.tls", false, "Enables Kafka broker TLS connection.")
	rootCmd.PersistentFlags().String("kafka.server-cert-filename", "", "Kafka broker CA.")
	rootCmd.PersistentFlags().String("kafka.client-cert-filename", "", "Kafka client certificate.")
	rootCmd.PersistentFlags().String("kafka.client-key-filename", "", "Kafka client key.")
	rootCmd.PersistentFlags().Bool("kafka.check-topics", false, `Enables checking if all "--kafka.topics" exist before kafka client connection attempt.`)
	rootCmd.PersistentFlags().Bool("kafka.debug-logs", false, `Enables kafka client debug logs. You need to set "--log-level" to "DEBUG", too.`)

	// Tables flags
	rootCmd.PersistentFlags().StringSlice("tables.names", nil, "Create Regatta tables with given names")

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

	dbl.SetLoggerFactory(raft.LoggerFactory(logger))
	dbl.GetLogger("raft").SetLevel(dbl.DEBUG)
	dbl.GetLogger("rsm").SetLevel(dbl.DEBUG)
	dbl.GetLogger("transport").SetLevel(dbl.DEBUG)
	dbl.GetLogger("dragonboat").SetLevel(dbl.DEBUG)
	dbl.GetLogger("logdb").SetLevel(dbl.DEBUG)

	log := logger.Sugar().Named("root")

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	nhc := config.NodeHostConfig{
		WALDir:         viper.GetString("raft.wal-dir"),
		NodeHostDir:    viper.GetString("raft.node-host-dir"),
		RTTMillisecond: uint64(viper.GetDuration("raft.rtt").Milliseconds()),
		RaftAddress:    viper.GetString("raft.address"),
		ListenAddress:  viper.GetString("raft.listen-address"),
		EnableMetrics:  true,
	}
	nhc.Expert.LogDB = buildLogDBConfig()

	err := nhc.Prepare()
	if err != nil {
		log.Panic(err)
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Panic(err)
	}
	defer nh.Close()

	tm := tables.NewManager(nh, initialMembers(log),
		tables.Config{
			NodeID: viper.GetUint64("raft.node-id"),
			Table: tables.TableConfig{
				ElectionRTT:        viper.GetUint64("raft.election-rtt"),
				HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
				SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
				CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
				MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
				WALDir:             viper.GetString("raft.state-machine-wal-dir"),
				NodeHostDir:        viper.GetString("raft.state-machine-dir"),
			},
			Meta: tables.MetaConfig{
				ElectionRTT:        viper.GetUint64("raft.election-rtt"),
				HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
				SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
				CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
				MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
			},
		})
	err = tm.Start()
	if err != nil {
		log.Panic(err)
	}
	defer tm.Close()

	go func() {
		if err := tm.WaitUntilReady(); err != nil {
			log.Infof("table manager failed to start: %v", err)
			return
		}
		log.Info("table manager started")
		tNames := viper.GetStringSlice("tables.names")
		for _, table := range tNames {
			log.Debugf("creating table %s", table)
			err := tm.CreateTable(table)
			if err != nil {
				if err == tables.ErrTableExists {
					log.Infof("table %s already exist, skipping creation", table)
				} else {
					log.Errorf("failed to create table %s: %v", table, err)
				}
			}
		}
	}()

	mTables := viper.GetStringSlice("kafka.topics")

	// Create storage
	st := &tables.KVStorageWrapper{Manager: tm}
	if !waitForClusterInit(shutdown, nh) {
		log.Info("Shutting down...")
		return
	}

	// Load API certificate
	watcher := &cert.Watcher{
		CertFile: viper.GetString("api.cert-filename"),
		KeyFile:  viper.GetString("api.key-filename"),
		Log:      logger.Named("cert").Sugar(),
	}
	err = watcher.Watch()
	if err != nil {
		log.Panicf("cannot watch certificate: %v", err)
	}
	defer watcher.Stop()

	// Create regatta server
	regatta := regattaserver.NewServer(
		viper.GetString("api.address"),
		watcher.TLSConfig(),
		viper.GetBool("api.reflection-api"),
	)
	defer regatta.Shutdown()

	// Create and register grpc/rest endpoints
	kvs := &regattaserver.KVServer{
		Storage:       st,
		ManagedTables: mTables,
	}
	proto.RegisterKVServer(regatta, kvs)

	ms := &regattaserver.MaintenanceServer{
		Storage: st,
	}
	proto.RegisterMaintenanceServer(regatta, ms)

	// Start server
	log.Infof("regatta listening at %s", regatta.Addr)
	go func() {
		if err := regatta.ListenAndServe(); err != nil {
			log.Panicf("grpc listenAndServe failed: %v", err)
		}
	}()

	hs := regattaserver.NewRESTServer(viper.GetString("rest.address"))
	go func() {
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("REST listenAndServe failed: %v", err)
		}
	}()
	defer hs.Shutdown()

	var tc []kafka.TopicConfig
	for _, topic := range mTables {
		tc = append(tc, kafka.TopicConfig{
			Name:     topic,
			GroupID:  viper.GetString("kafka.group-id"),
			Table:    topic,
			Listener: onMessage(st),
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
		DebugLogs:          viper.GetBool("kafka.debug-logs"),
	}

	// wait until kafka is ready
	checkTopics := viper.GetBool("kafka.check-topics")
	if checkTopics && !waitForKafkaInit(shutdown, kafkaCfg) {
		log.Info("Shutting down...")
		return
	}

	// Start Kafka consumer
	consumer, err := kafka.NewConsumer(kafkaCfg)
	if err != nil {
		log.Panicf("failed to create consumer: %v", err)
	}
	defer consumer.Close()
	prometheus.MustRegister(consumer)

	log.Info("start consuming...")
	if err := consumer.Start(context.Background()); err != nil {
		log.Panicf("failed to start consumer: %v", err)
	}

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}

// waitForClusterInit checks state of clusters for `nh`. It blocks until no clusters are pending.
// It can be interrupted with signal in `shutdown` channel.
func waitForClusterInit(shutdown chan os.Signal, nh *dragonboat.NodeHost) bool {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		ready := true
		select {
		case <-shutdown:
			return false
		case <-ticker.C:
			info := nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
			for _, ci := range info.ClusterInfoList {
				ready = ready && !ci.Pending
			}
			if ready {
				return true
			}
		}
	}
}

// waitForKafkaInit checks if kafka is ready and has all topics regatta will consume. It blocks until check is successful.
// It can be interrupted with signal in `shutdown` channel.
func waitForKafkaInit(shutdown chan os.Signal, cfg kafka.Config) bool {
	ch := kafka.NewChecker(cfg, 30*time.Second)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdown:
			return false
		case <-ticker.C:
			if ch.Check() {
				return true
			}
		}
	}
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
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	return logger
}

func buildLogDBConfig() config.LogDBConfig {
	cfg := config.GetSmallMemLogDBConfig()
	cfg.KVRecycleLogFileNum = 4
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	return cfg
}

func onMessage(st storage.KVStorage) kafka.OnMessageFunc {
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
