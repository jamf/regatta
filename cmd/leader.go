// Copyright JAMF Software, LLC

package cmd

import (
	"errors"
	"os"
	"os/signal"
	"syscall"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattaserver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	// Base flags
	leaderCmd.PersistentFlags().AddFlagSet(rootFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(apiFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(restFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(raftFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(storageFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(kafkaFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(maintenanceFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(tablesFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(experimentalFlagSet)

	// Replication flags
	leaderCmd.PersistentFlags().Bool("replication.enabled", true, "Replication API enabled")
	leaderCmd.PersistentFlags().Uint64("replication.max-send-message-size-bytes", regattaserver.DefaultMaxGRPCSize, `The target maximum size of single replication message allowed to send.
Still under some circumstances a larger message could be sent. So make sure the followers are able to accept slightly larger messages.`)
	leaderCmd.PersistentFlags().String("replication.address", ":8444", "Address the replication API server should listen on.")
	leaderCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/server.crt", "Path to the API server certificate.")
	leaderCmd.PersistentFlags().String("replication.key-filename", "hack/replication/server.key", "Path to the API server private key file.")
	leaderCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the API server CA cert file.")
	leaderCmd.PersistentFlags().Int("replication.log-cache-size", regattaserver.DefaultCacheSize, "Size of the replication cache.")
}

var leaderCmd = &cobra.Command{
	Use:   "leader",
	Short: "Start Regatta in leader mode",
	Run:   leader,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return validateLeaderConfig()
	},
	DisableAutoGenTag: true,
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

	autoSetMaxprocs(log)

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	engine, closer := startEngine(logger, log)
	defer closer()

	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(histogramBuckets))

	closer = startAPIServer(logger, log, &regattaserver.KVServer{Storage: engine})
	defer closer()

	if viper.GetBool("replication.enabled") {
		closer = startReplicationServer(logger, log, engine)
		defer closer()
	}

	if viper.GetBool("maintenance.enabled") {
		backupSrv := &regattaserver.BackupServer{Tables: engine}
		metadataSrv := &regattaserver.MetadataServer{Tables: engine}
		closer = startMaintenanceServer(logger, log, backupSrv, metadataSrv)
		defer closer()
	}

	closer = startRESTServer(log)
	defer closer()

	server := regattaserver.NewLeaderTableServer(engine.Manager)
	closer = startTableServer(logger, log, server)
	defer closer()

	closer = startKafka(log, engine, shutdown)
	defer closer()

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}
