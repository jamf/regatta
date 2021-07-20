package cmd

import (
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
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

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}
