// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/security"
	"github.com/jamf/regatta/storage"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	leaderCmd.PersistentFlags().AddFlagSet(tablesFlagSet)
	leaderCmd.PersistentFlags().AddFlagSet(experimentalFlagSet)

	// Tables flags
	leaderCmd.PersistentFlags().StringSlice("tables.names", nil, "Create Regatta tables with given names.")
	leaderCmd.PersistentFlags().StringSlice("tables.delete", nil, "Delete Regatta tables with given names.")
	_ = leaderCmd.PersistentFlags().MarkDeprecated("tables.names", "Use `regatta.v1.Tables/Create` API to create tables instead.")
	_ = leaderCmd.PersistentFlags().MarkDeprecated("tables.delete", "Use `regatta.v1.Tables/Delete` API to delete tables instead.")

	// Replication flags
	leaderCmd.PersistentFlags().Bool("replication.enabled", true, "Whether replication API is enabled.")
	leaderCmd.PersistentFlags().Uint64("replication.max-send-message-size-bytes", regattaserver.DefaultMaxGRPCSize, `The target maximum size of single replication message allowed to send.
Under some circumstances, a larger message could be sent. Followers should be able to accept slightly larger messages.`)
	leaderCmd.PersistentFlags().String("replication.address", "http://0.0.0.0:8444", "Replication API server address. The address the server listens on.")
	leaderCmd.PersistentFlags().String("replication.cert-filename", "", "Path to the API server certificate.")
	leaderCmd.PersistentFlags().String("replication.key-filename", "", "Path to the API server private key file.")
	leaderCmd.PersistentFlags().String("replication.ca-filename", "", "Path to the API server CA cert file.")
	leaderCmd.PersistentFlags().Bool("replication.client-cert-auth", false, "Replication server client certificate auth enabled. If set to true the `replication.ca-filename` should be provided as well.")
	leaderCmd.PersistentFlags().Int("replication.log-cache-size", 0, "Size of the replication cache. Size 0 means cache is turned off.")
}

var leaderCmd = &cobra.Command{
	Use:   "leader",
	Short: "Start Regatta in leader mode.",
	RunE:  leader,
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

func leader(_ *cobra.Command, _ []string) error {
	logger := rl.NewLogger(viper.GetBool("dev-mode"), viper.GetString("log-level"))
	defer func() {
		_ = logger.Sync()
	}()
	zap.ReplaceGlobals(logger)
	log := logger.Sugar().Named("root")
	engineLog := logger.Named("engine")
	setupDragonboatLogger(engineLog)

	if err := autoSetMaxprocs(log); err != nil {
		return err
	}

	// Check signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	initialMembers, err := parseInitialMembers(viper.GetStringMapString("raft.initial-members"))
	if err != nil {
		return fmt.Errorf("failed to parse raft.initial-members: %w", err)
	}

	logDBImpl, err := parseLogDBImplementation(viper.GetString("raft.logdb"))
	if err != nil {
		return fmt.Errorf("failed to parse raft.logdb: %w", err)
	}

	engine, err := storage.New(storage.Config{
		Log:                 engineLog.Sugar(),
		ClientAddress:       viper.GetString("api.advertise-address"),
		NodeID:              viper.GetUint64("raft.node-id"),
		InitialMembers:      initialMembers,
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
			ClusterName:      viper.GetString("memberlist.cluster-name"),
			NodeName:         viper.GetString("memberlist.node-name"),
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
		LogDBImplementation: logDBImpl,
	})
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}
	if err := engine.Start(); err != nil {
		return fmt.Errorf("failed to start engine: %w", err)
	}
	defer engine.Close()

	go func() {
		if err := engine.WaitUntilReady(context.Background()); err != nil {
			log.Infof("engine failed to start: %v", err)
			return
		}
		log.Info("engine started")
		tNames := viper.GetStringSlice("tables.names")
		for _, table := range tNames {
			log.Debugf("creating table %s", table)
			if _, err := engine.CreateTable(table); err != nil {
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
			regatta, err := createAPIServer(logger.Named("server.api"), func(r grpc.ServiceRegistrar) {
				regattapb.RegisterKVServer(r, &regattaserver.KVServer{
					Storage: engine,
				})
				regattapb.RegisterClusterServer(r, &regattaserver.ClusterServer{
					Cluster: engine,
					Config:  viperConfigReader,
				})
				if viper.GetBool("tables.enabled") {
					regattapb.RegisterTablesServer(r, &regattaserver.TablesServer{Tables: engine, AuthFunc: authFunc(viper.GetString("tables.token"))})
				}
				if viper.GetBool("maintenance.enabled") {
					regattapb.RegisterMaintenanceServer(r, &regattaserver.BackupServer{Tables: engine, AuthFunc: authFunc(viper.GetString("maintenance.token"))})
				}
			})
			if err != nil {
				return fmt.Errorf("failed to create API server: %w", err)
			}
			// Start server
			go func() {
				if err := regatta.ListenAndServe(); err != nil {
					log.Errorf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer regatta.Shutdown()
		}

		if viper.GetBool("replication.enabled") {
			// Load replication API certificate
			replication, err := createReplicationServer(logger.Named("server.replication"), func(r grpc.ServiceRegistrar) {
				ls := regattaserver.NewLogServer(
					engine,
					engine.LogReader,
					logger,
					viper.GetUint64("replication.max-send-message-size-bytes"),
				)
				regattapb.RegisterMetadataServer(r, &regattaserver.MetadataServer{Tables: engine})
				regattapb.RegisterSnapshotServer(r, &regattaserver.SnapshotServer{Tables: engine})
				regattapb.RegisterKVServer(r, &regattaserver.KVServer{Storage: engine})
				regattapb.RegisterLogServer(r, ls)
			})
			if err != nil {
				return fmt.Errorf("failed to create Replication server: %w", err)
			}

			// Start server
			go func() {
				if err := replication.ListenAndServe(); err != nil {
					log.Errorf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer replication.Shutdown()
		}

		// Create REST server
		addr, _, _ := resolveURL(viper.GetString("rest.address"))
		hs := regattaserver.NewRESTServer(addr, viper.GetDuration("rest.read-timeout"))
		go func() {
			if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Errorf("REST listenAndServe failed: %v", err)
			}
		}()
		defer hs.Shutdown()
	}

	// Cleanup
	<-shutdown
	log.Info("shutting down...")
	return nil
}

func createReplicationServer(log *zap.Logger, reg func(r grpc.ServiceRegistrar)) (*regattaserver.RegattaServer, error) {
	addr, secure, net := resolveURL(viper.GetString("replication.address"))
	lopts := []logging.Option{logging.WithLogOnEvents(logging.FinishCall), logging.WithLevels(codeToLevel)}
	opts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			grpcmetrics.StreamServerInterceptor(),
			logging.StreamServerInterceptor(interceptorLogger(log), lopts...),
		),
		grpc.ChainUnaryInterceptor(
			grpcmetrics.UnaryServerInterceptor(),
			logging.UnaryServerInterceptor(interceptorLogger(log), lopts...),
		),
	}
	if secure {
		ti := security.TLSInfo{
			CertFile:        viper.GetString("replication.cert-filename"),
			KeyFile:         viper.GetString("replication.key-filename"),
			TrustedCAFile:   viper.GetString("replication.ca-filename"),
			ClientCertAuth:  viper.GetBool("replication.client-cert-auth"),
			AllowedCN:       viper.GetString("replication.allowed-cn"),
			AllowedHostname: viper.GetString("replication.allowed-hostname"),
			Logger:          log.Named("cert").Sugar(),
		}
		cfg, err := ti.ServerConfig()
		if err != nil {
			return nil, fmt.Errorf("cannot build tls config: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	// Create regatta replication server
	server := regattaserver.NewServer(addr, net, log.Sugar(), opts...)
	reg(server)
	grpcmetrics.InitializeMetrics(server.Server)
	return server, nil
}

func codeToLevel(code codes.Code) logging.Level {
	switch code {
	case codes.OK, codes.NotFound, codes.Canceled, codes.AlreadyExists, codes.InvalidArgument, codes.Unauthenticated:
		return logging.LevelDebug
	case codes.DeadlineExceeded, codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.Unavailable:
		return logging.LevelWarn
	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return logging.LevelError
	default:
		return logging.LevelError
	}
}

func interceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i]
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key.(string), v))
			case int:
				f = append(f, zap.Int(key.(string), v))
			case bool:
				f = append(f, zap.Bool(key.(string), v))
			default:
				f = append(f, zap.Any(key.(string), v))
			}
		}

		logger := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}
