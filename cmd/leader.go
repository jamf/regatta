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
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage"
	serrors "github.com/jamf/regatta/storage/errors"
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
	leaderCmd.PersistentFlags().AddFlagSet(memberlistFlagSet)
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
	leaderCmd.PersistentFlags().String("replication.address", "http://127.0.0.1:8444", "Replication API server address.")
	leaderCmd.PersistentFlags().String("replication.cert-filename", "", "Path to the API server certificate.")
	leaderCmd.PersistentFlags().String("replication.key-filename", "", "Path to the API server private key file.")
	leaderCmd.PersistentFlags().String("replication.ca-filename", "", "Path to the API server CA cert file.")
	leaderCmd.PersistentFlags().Int("replication.log-cache-size", 0, "Size of the replication cache. Size 0 means cache is turned off.")
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
	engineLog := logger.Named("engine")
	setupDragonboatLogger(engineLog)

	autoSetMaxprocs(log)

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	engine, err := storage.New(storage.Config{
		Log:           engineLog.Sugar(),
		ClientAddress: viper.GetString("api.address"),
		NodeID:        viper.GetUint64("raft.node-id"),
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
		Gossip: storage.GossipConfig{
			BindAddress:      viper.GetString("memberlist.address"),
			AdvertiseAddress: viper.GetString("memberlist.advertise-address"),
			InitialMembers:   viper.GetStringSlice("memberlist.members"),
		},
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
			return storage.Pebble
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
			if err := engine.CreateTable(table); err != nil {
				if errors.Is(err, serrors.ErrTableExists) {
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
		// Create regatta API server
		{
			regatta := createAPIServer()
			regattapb.RegisterKVServer(regatta, &regattaserver.KVServer{
				Storage: engine,
			})
			regattapb.RegisterClusterServer(regatta, &regattaserver.ClusterServer{
				Cluster: engine,
			})
			// Start server
			go func() {
				if err := regatta.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer regatta.Shutdown()
		}

		if viper.GetBool("replication.enabled") {
			// Load replication API certificate
			replication := createReplicationServer(logger.Named("server.replication"))
			ls := regattaserver.NewLogServer(
				engine.Manager,
				engine.LogReader,
				logger,
				viper.GetUint64("replication.max-send-message-size-bytes"),
			)
			regattapb.RegisterMetadataServer(replication, &regattaserver.MetadataServer{Tables: engine})
			regattapb.RegisterSnapshotServer(replication, &regattaserver.SnapshotServer{Tables: engine})
			regattapb.RegisterLogServer(replication, ls)
			// Start server
			go func() {
				if err := replication.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer replication.Shutdown()
		}

		if viper.GetBool("maintenance.enabled") {
			maintenance := createMaintenanceServer()
			regattapb.RegisterMetadataServer(maintenance, &regattaserver.MetadataServer{Tables: engine})
			regattapb.RegisterMaintenanceServer(maintenance, &regattaserver.BackupServer{Tables: engine})
			// Start server
			go func() {
				if err := maintenance.ListenAndServe(); err != nil {
					log.Panicf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer maintenance.Shutdown()
		}

		// Create REST server
		addr, _, _ := resolveUrl(viper.GetString("rest.address"))
		hs := regattaserver.NewRESTServer(addr, viper.GetDuration("rest.read-timeout"))
		go func() {
			if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Panicf("REST listenAndServe failed: %v", err)
			}
		}()
		defer hs.Shutdown()
	}

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
}

func createReplicationServer(log *zap.Logger) *regattaserver.RegattaServer {
	addr, secure, net := resolveUrl(viper.GetString("replication.address"))
	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
			grpc_zap.StreamServerInterceptor(log, grpc_zap.WithDecider(logDeciderFunc)),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_zap.UnaryServerInterceptor(log, grpc_zap.WithDecider(logDeciderFunc)),
		)),
	}
	if secure {
		c, err := cert.New(viper.GetString("replication.cert-filename"), viper.GetString("replication.key-filename"))
		if err != nil {
			log.Sugar().Panicf("cannot load replication certificate: %v", err)
		}
		caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
		if err != nil {
			log.Sugar().Panicf("cannot load clients CA: %v", err)
		}
		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(caBytes)
		grpc.Creds(credentials.NewTLS(&tls.Config{
			ClientAuth:     tls.RequireAndVerifyClientCert,
			ClientCAs:      cp,
			MinVersion:     tls.VersionTLS12,
			GetCertificate: c.GetCertificate,
		}))
	}
	// Create regatta replication server
	return regattaserver.NewServer(addr, net, opts...)
}

func logDeciderFunc(_ string, err error) bool {
	st, _ := status.FromError(err)
	return st != nil
}
