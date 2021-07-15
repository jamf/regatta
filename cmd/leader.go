package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dbl "github.com/lni/dragonboat/v3/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	"github.com/wandera/regatta/kafka"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

func init() {
	// Base flags
	leaderCmd.PersistentFlags().AddFlagSet(rootFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(apiFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(restFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(raftFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(kafkaFlagSet)

	// Tables flags
	leaderCmd.PersistentFlags().StringSlice("tables.names", nil, "Create Regatta tables with given names")

	// Replication flags
	leaderCmd.PersistentFlags().String("replication.address", "localhost:8444", "Address the replication API server should listen on.")
	leaderCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/server.crt", "Path to the API server certificate.")
	leaderCmd.PersistentFlags().String("replication.key-filename", "hack/replication/server.key", "Path to the API server private key file.")
	leaderCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the API server CA cert file.")
}

var leaderCmd = &cobra.Command{
	Use:   "leader",
	Short: "Start Regatta in leader mode",
	Run:   leader,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return validateLeaderConfig()
	},
}

func validateLeaderConfig() error {
	if !viper.IsSet("raft.address") {
		return errors.New("raft address must be set")
	}
	return nil
}

func leader(_ *cobra.Command, _ []string) {
	logger := rl.NewLogger(viper.GetBool("dev-mode"), viper.GetString("log-level"))
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	dbl.SetLoggerFactory(rl.LoggerFactory(logger))
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

	initialMembers, err := parseInitialMembers(viper.GetStringMapString("raft.initial-members"))
	if err != nil {
		log.Panic(err)
	}

	tm := tables.NewManager(nh, initialMembers,
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

	// Create regatta API server
	grpc_prometheus.EnableHandlingTimeHistogram()
	regatta := regattaserver.NewServer(
		viper.GetString("api.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcher.GetCertificate(), nil
			},
		})),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 60 * time.Second,
		}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)

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
	defer regatta.Shutdown()

	// Load replication API certificate
	watcherReplication := &cert.Watcher{
		CertFile: viper.GetString("replication.cert-filename"),
		KeyFile:  viper.GetString("replication.key-filename"),
		Log:      logger.Named("cert").Sugar(),
	}
	err = watcherReplication.Watch()
	if err != nil {
		log.Panicf("cannot watch replication certificate: %v", err)
	}
	defer watcherReplication.Stop()

	caBytes, err := ioutil.ReadFile(viper.GetString("replication.ca-filename"))
	if err != nil {
		log.Panicf("cannot load clients CA: %v", err)
	}
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(caBytes)

	// Create regatta replication server
	replication := regattaserver.NewServer(
		viper.GetString("replication.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  cp,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcherReplication.GetCertificate(), nil
			},
		})),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)

	proto.RegisterMetadataServer(replication, &proto.UnimplementedMetadataServer{})

	// Start server
	log.Infof("regatta replication listening at %s", replication.Addr)
	go func() {
		if err := replication.ListenAndServe(); err != nil {
			log.Panicf("grpc listenAndServe failed: %v", err)
		}
	}()
	defer replication.Shutdown()

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
