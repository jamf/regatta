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

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	"github.com/wandera/regatta/kafka"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/replication"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	// Base flags
	followerCmd.PersistentFlags().AddFlagSet(rootFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(apiFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(restFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(raftFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(kafkaFlagSet)

	// Replication flags
	followerCmd.PersistentFlags().String("replication.leader-address", "localhost:8444", "Address of the leader replication API to connect to.")
	followerCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/client.crt", "Path to the client certificate.")
	followerCmd.PersistentFlags().String("replication.key-filename", "hack/replication/client.key", "Path to the client private key file.")
	followerCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the client CA cert file.")
}

var followerCmd = &cobra.Command{
	Use:   "follower",
	Short: "Start Regatta in follower mode",
	Run:   follower,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return validateFollowerConfig()
	},
}

func validateFollowerConfig() error {
	if !viper.IsSet("replication.leader-address") {
		return errors.New("leader address must be set")
	}
	if !viper.IsSet("raft.address") {
		return errors.New("raft address must be set")
	}
	return nil
}

func follower(_ *cobra.Command, _ []string) {
	logger := rl.NewLogger(viper.GetBool("dev-mode"), viper.GetString("log-level"))
	defer func() {
		_ = logger.Sync()
	}()
	zap.ReplaceGlobals(logger)
	log := logger.Sugar().Named("root")
	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	nh, err := createNodeHost(logger)
	if err != nil {
		log.Panic(err)
	}
	defer nh.Close()

	tm, err := createTableManager(nh)
	if err != nil {
		log.Panic(err)
	}
	err = tm.Start()
	if err != nil {
		log.Panic(err)
	}
	defer tm.Close()

	// Replication
	{
		watcher := &cert.Watcher{
			CertFile: viper.GetString("replication.cert-filename"),
			KeyFile:  viper.GetString("replication.key-filename"),
			Log:      logger.Named("cert").Sugar(),
		}
		err = watcher.Watch()
		if err != nil {
			log.Panicf("cannot watch certificate: %v", err)
		}
		defer watcher.Stop()

		caBytes, err := ioutil.ReadFile(viper.GetString("replication.ca-filename"))
		if err != nil {
			log.Panicf("cannot load server CA: %v", err)
		}
		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(caBytes)

		conn, err := createReplicationConn(cp, watcher)
		defer func() {
			_ = conn.Close()
		}()
		if err != nil {
			log.Panicf("cannot create replication conn: %v", err)
		}

		mc := proto.NewMetadataClient(conn)
		mr := replication.NewMetadata(mc, tm)
		mr.Replicate()
		defer mr.Close()
	}

	// Create storage
	st := &tables.KVStorageWrapper{Manager: tm}
	mTables := viper.GetStringSlice("kafka.topics")

	// Start servers
	{
		grpc_prometheus.EnableHandlingTimeHistogram()
		// Create regatta API server
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
		// Create server
		regatta := createAPIServer(watcher, st, mTables)
		// Start server
		go func() {
			log.Infof("regatta listening at %s", regatta.Addr)
			if err := regatta.ListenAndServe(); err != nil {
				log.Panicf("grpc listenAndServe failed: %v", err)
			}
		}()
		defer regatta.Shutdown()

		// Create REST server
		hs := regattaserver.NewRESTServer(viper.GetString("rest.address"))
		go func() {
			if err := hs.ListenAndServe(); err != http.ErrServerClosed {
				log.Panicf("REST listenAndServe failed: %v", err)
			}
		}()
		defer hs.Shutdown()
	}

	// Start Kafka
	{
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
	}

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}

func createReplicationConn(cp *x509.CertPool, replicationWatcher *cert.Watcher) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{
		RootCAs: cp,
		GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return replicationWatcher.GetCertificate(), nil
		},
	})

	replConn, err := grpc.Dial(viper.GetString("replication.leader-address"), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return replConn, nil
}
