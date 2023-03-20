// Copyright JAMF Software, LLC

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/pebble/vfs"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func init() {
	// Base flags
	leaderCmd.PersistentFlags().AddFlagSet(rootFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(apiFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(restFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(raftFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(storageFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(maintenanceFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(experimentalFlagSet)

	// Tables flags
	leaderCmd.PersistentFlags().StringSlice("tables.names", nil, "Create Regatta tables with given names.")
	leaderCmd.PersistentFlags().StringSlice("tables.delete", nil, "Delete Regatta tables with given names.")

	// Replication flags
	leaderCmd.PersistentFlags().Bool("replication.enabled", true, "Whether replication API is enabled.")
	leaderCmd.PersistentFlags().Uint64("replication.max-send-message-size-bytes", regattaserver.DefaultMaxGRPCSize, `The target maximum size of single replication message allowed to send.
Under some circumstances, a larger message could be sent. Followers should be able to accept slightly larger messages.`)
	leaderCmd.PersistentFlags().String("replication.address", ":8444", "Replication API server address.")
	leaderCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/server.crt", "Path to the API server certificate.")
	leaderCmd.PersistentFlags().String("replication.key-filename", "hack/replication/server.key", "Path to the API server private key file.")
	leaderCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the API server CA cert file.")
	leaderCmd.PersistentFlags().Int("replication.log-cache-size", regattaserver.DefaultCacheSize, "Size of the replication cache.")
}

var leaderCmd = &cobra.Command{
	Use:   "leader",
	Short: "Start Regatta in leader mode.",
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

	engine, err := storage.New(storage.Config{
		Logger: logger.Named("engine"),
		NodeID: viper.GetUint64("raft.node-id"),
		InitialMembers: func() map[uint64]string {
			initialMembers, err := parseInitialMembers(viper.GetStringMapString("raft.initial-members"))
			if err != nil {
				log.Panic(err)
			}
			return initialMembers
		}(),
		WALDir:              viper.GetString("raft.wal-dir"),
		NodeHostDir:         viper.GetString("raft.node-host-dir"),
		RTTMillisecond:      uint64(viper.GetDuration("raft.rtt").Milliseconds()),
		RaftAddress:         viper.GetString("raft.address"),
		ListenAddress:       viper.GetString("raft.listen-address"),
		EnableMetrics:       true,
		MaxReceiveQueueSize: viper.GetUint64("raft.max-recv-queue-size"),
		MaxSendQueueSize:    viper.GetUint64("raft.max-send-queue-size"),
		LogCacheSize:        viper.GetInt("replication.log-cache-size"),
		Table: storage.TableConfig{
			FS:                 vfs.Default,
			ElectionRTT:        viper.GetUint64("raft.election-rtt"),
			HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
			SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
			CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
			MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
			DataDir:            viper.GetString("raft.state-machine-dir"),
			RecoveryType:       toRecoveryType(viper.GetString("raft.snapshot-recovery-type")),
			BlockCacheSize:     viper.GetInt64("storage.block-cache-size"),
			TableCacheSize:     viper.GetInt("storage.table-cache-size"),
		},
		Meta: storage.MetaConfig{
			ElectionRTT:        viper.GetUint64("raft.election-rtt"),
			HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
			SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
			CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
			MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
		},
		LogDBImplementation: func() storage.LogDBImplementation {
			switch viper.GetString("raft.logdb") {
			case "pebble":
				return storage.Pebble
			case "tan":
				return storage.Tan
			default:
				log.Panicf("unknown logdb impl: %s", viper.GetString("raft.logdb"))
			}
			return storage.Default
		}(),
	},
	)
	if err != nil {
		log.Panic(err)
	}
	if err := engine.Start(); err != nil {
		log.Panic(err)
	}
	defer engine.Close()

	go func() {
		if err := engine.WaitUntilReady(); err != nil {
			log.Infof("table manager failed to start: %v", err)
			return
		}
		log.Info("table manager started")
		tNames := viper.GetStringSlice("tables.names")
		for _, table := range tNames {
			log.Debugf("creating table %s", table)
			err := engine.CreateTable(table)
			if err != nil {
				if err == serrors.ErrTableExists {
					log.Infof("table %s already exist, skipping creation", table)
				} else {
					log.Errorf("failed to create table %s: %v", table, err)
				}
			}
		}
		dNames := viper.GetStringSlice("tables.delete")
		for _, table := range dNames {
			log.Debugf("deleting table %s", table)
			err := engine.DeleteTable(table)
			if err != nil {
				log.Errorf("failed to delete table %s: %v", table, err)
			}
		}
	}()

	// Start servers
	{
		grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(histogramBuckets))
		// Create regatta API server
		{
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
			regatta := createAPIServer(watcher)
			proto.RegisterKVServer(regatta, &regattaserver.KVServer{
				Storage: engine,
			})
			// Start server
			go func() {
				log.Infof("regatta listening at %s", regatta.Addr)
				if err := regatta.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer regatta.Shutdown()
		}

		if viper.GetBool("replication.enabled") {
			// Load replication API certificate
			watcher := &cert.Watcher{
				CertFile: viper.GetString("replication.cert-filename"),
				KeyFile:  viper.GetString("replication.key-filename"),
				Log:      logger.Named("cert").Sugar(),
			}
			err = watcher.Watch()
			if err != nil {
				log.Panicf("cannot watch replication certificate: %v", err)
			}
			defer watcher.Stop()
			caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
			if err != nil {
				log.Panicf("cannot load clients CA: %v", err)
			}

			replication := createReplicationServer(watcher, caBytes, logger.Named("server.replication"))
			ls := regattaserver.NewLogServer(
				engine.Manager,
				engine.LogReader,
				logger,
				viper.GetUint64("replication.max-send-message-size-bytes"),
			)
			proto.RegisterMetadataServer(replication, &regattaserver.MetadataServer{Tables: engine})
			proto.RegisterSnapshotServer(replication, &regattaserver.SnapshotServer{Tables: engine})
			proto.RegisterLogServer(replication, ls)

			prometheus.MustRegister(ls)
			// Start server
			go func() {
				log.Infof("regatta replication listening at %s", replication.Addr)
				if err := replication.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer replication.Shutdown()
		}

		if viper.GetBool("maintenance.enabled") {
			// Load maintenance API certificate
			watcher := &cert.Watcher{
				CertFile: viper.GetString("maintenance.cert-filename"),
				KeyFile:  viper.GetString("maintenance.key-filename"),
				Log:      logger.Named("cert").Sugar(),
			}
			err = watcher.Watch()
			if err != nil {
				log.Panicf("cannot watch maintenance certificate: %v", err)
			}
			defer watcher.Stop()

			maintenance := createMaintenanceServer(watcher)
			proto.RegisterMetadataServer(maintenance, &regattaserver.MetadataServer{Tables: engine})
			proto.RegisterMaintenanceServer(maintenance, &regattaserver.BackupServer{Tables: engine})
			// Start server
			go func() {
				log.Infof("regatta maintenance listening at %s", maintenance.Addr)
				if err := maintenance.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer maintenance.Shutdown()
		}

		// Create REST server
		hs := regattaserver.NewRESTServer(viper.GetString("rest.address"), viper.GetDuration("rest.read-timeout"))
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

func createReplicationServer(watcher *cert.Watcher, ca []byte, log *zap.Logger) *regattaserver.RegattaServer {
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(ca)

	// Create regatta replication server
	return regattaserver.NewServer(
		viper.GetString("replication.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  cp,
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcher.GetCertificate(), nil
			},
		})),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
			grpc_zap.StreamServerInterceptor(log, grpc_zap.WithDecider(logDeciderFunc)),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_zap.UnaryServerInterceptor(log, grpc_zap.WithDecider(logDeciderFunc)),
		)),
	)
}

func logDeciderFunc(_ string, err error) bool {
	st, _ := status.FromError(err)
	return st != nil
}
