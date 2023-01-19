// Copyright JAMF Software, LLC

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/replication"
	"github.com/jamf/regatta/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	followerCmd.PersistentFlags().AddFlagSet(storageFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(maintenanceFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(tablesFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(experimentalFlagSet)

	// Replication flags
	followerCmd.PersistentFlags().String("replication.leader-address", "localhost:8444", "Address of the leader replication API to connect to.")
	followerCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/client.crt", "Path to the client certificate.")
	followerCmd.PersistentFlags().String("replication.key-filename", "hack/replication/client.key", "Path to the client private key file.")
	followerCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the client CA cert file.")
	followerCmd.PersistentFlags().Duration("replication.poll-interval", 10*time.Second, "Replication interval in seconds, the leader poll time.")
	followerCmd.PersistentFlags().Duration("replication.reconcile-interval", 30*time.Second, "Replication interval of tables reconciliation (workers startup/shutdown).")
	followerCmd.PersistentFlags().Duration("replication.lease-interval", 15*time.Second, "Interval in which the workers re-new their table leases.")
	followerCmd.PersistentFlags().Duration("replication.log-rpc-timeout", 1*time.Minute, "The log RPC timeout.")
	followerCmd.PersistentFlags().Duration("replication.snapshot-rpc-timeout", 1*time.Hour, "The snapshot RPC timeout.")
	followerCmd.PersistentFlags().Uint64("replication.max-recv-message-size-bytes", 8*1024*1024, "The maximum size of single replication message allowed to receive.")
	followerCmd.PersistentFlags().Uint64("replication.max-recovery-in-flight", 1, "The maximum number of recovery goroutines allowed to run in this instance.")
	followerCmd.PersistentFlags().Uint64("replication.max-snapshot-recv-bytes-per-second", 0, "Max bytes per second received by the snapshot API client, default value 0 means unlimited.")
}

var followerCmd = &cobra.Command{
	Use:   "follower",
	Short: "Start Regatta in follower mode",
	Run:   follower,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig(cmd.PersistentFlags())
		return validateFollowerConfig()
	},
	DisableAutoGenTag: true,
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

	autoSetMaxprocs(log)

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	engine, closer := startEngine(logger, log)
	defer closer()

	closer = startFollowerReplication(logger, log, engine)
	defer closer()

	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(histogramBuckets))

	server := &regattaserver.ReadonlyKVServer{
		KVServer: regattaserver.KVServer{
			Storage: engine,
		},
	}
	closer = startAPIServer(logger, log, server)
	defer closer()

	if viper.GetBool("maintenance.enabled") {
		resetSrv := &regattaserver.ResetServer{Tables: engine}
		closer = startMaintenanceServer(logger, log, resetSrv, nil)
		defer closer()
	}

	closer = startRESTServer(log)
	defer closer()

	tableServer := regattaserver.NewFollowerTableServer(engine.Manager.GetTables)
	closer = startTableServer(logger, log, tableServer)
	defer closer()

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}

func createReplicationConn(cp *x509.CertPool, replicationWatcher *cert.Watcher) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{
		RootCAs:    cp,
		MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return replicationWatcher.GetCertificate(), nil
		},
	})

	replConn, err := grpc.Dial(viper.GetString("replication.leader-address"),
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(viper.GetUint64("replication.max-recv-message-size-bytes")))),
	)
	if err != nil {
		return nil, err
	}
	return replConn, nil
}

func startFollowerReplication(logger *zap.Logger, log *zap.SugaredLogger, engine *storage.Engine) closerFunc {
	watcher := startWatcher(
		viper.GetString("replication.cert-filename"),
		viper.GetString("replication.key-filename"),
		logger.Named("cert").Sugar(),
	)

	caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
	if err != nil {
		watcher.Stop()
		log.Panicf("cannot load server CA: %v", err)
	}
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(caBytes)

	conn, err := createReplicationConn(cp, watcher)
	if err != nil {
		if connErr := conn.Close(); connErr != nil {
			log.Errorf("could not close replication connection: %v", connErr)
		}
		watcher.Stop()
		log.Panicf("cannot create replication conn: %v", err)
	}

	d := replication.NewManager(engine.Manager, engine.NodeHost, conn, replication.Config{
		ReconcileInterval: viper.GetDuration("replication.reconcile-interval"),
		Workers: replication.WorkerConfig{
			PollInterval:        viper.GetDuration("replication.poll-interval"),
			LeaseInterval:       viper.GetDuration("replication.lease-interval"),
			LogRPCTimeout:       viper.GetDuration("replication.log-rpc-timeout"),
			SnapshotRPCTimeout:  viper.GetDuration("replication.snapshot-rpc-timeout"),
			MaxRecoveryInFlight: int64(viper.GetUint64("replication.max-recovery-in-flight")),
			MaxSnapshotRecv:     viper.GetUint64("replication.max-snapshot-recv-bytes-per-second"),
		},
	})
	prometheus.MustRegister(d)
	d.Start()

	return func() {
		conn.Close()
		watcher.Stop()
		d.Close()
	}
}
