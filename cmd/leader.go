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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	"github.com/wandera/regatta/kafka"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	leaderCmd.PersistentFlags().Bool("replication.enabled", false, "Replication API enabled")
	leaderCmd.PersistentFlags().String("replication.address", ":8444", "Address the replication API server should listen on.")
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

		if viper.GetBool("replication.enabled") {
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

			replication := createReplicationServer(watcherReplication, caBytes, tm)
			// Start server
			go func() {
				log.Infof("regatta replication listening at %s", replication.Addr)
				if err := replication.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer replication.Shutdown()
		}

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

func createReplicationServer(watcherReplication *cert.Watcher, ca []byte, manager *tables.Manager) *regattaserver.RegattaServer {
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(ca)

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

	proto.RegisterMetadataServer(replication, &regattaserver.MetadataServer{Manager: manager})
	return replication
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
