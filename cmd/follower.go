// Copyright JAMF Software, LLC

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/replication"
	"github.com/jamf/regatta/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func init() {
	// Base flags
	followerCmd.PersistentFlags().AddFlagSet(rootFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(apiFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(restFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(raftFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(memberlistFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(storageFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(maintenanceFlagSet)
	followerCmd.PersistentFlags().AddFlagSet(experimentalFlagSet)

	// Replication flags
	followerCmd.PersistentFlags().String("replication.leader-address", "localhost:8444", "Address of the leader replication API to connect to.")
	followerCmd.PersistentFlags().Duration("replication.keepalive-time", 1*time.Minute, "After a duration of this time if the replication client doesn't see any activity it pings the server to see if the transport is still alive. If set below 10s, a minimum value of 10s will be used instead.")
	followerCmd.PersistentFlags().Duration("replication.keepalive-timeout", 10*time.Second, "After having pinged for keepalive check, the replication client waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
	followerCmd.PersistentFlags().String("replication.cert-filename", "hack/replication/client.crt", "Path to the client certificate.")
	followerCmd.PersistentFlags().String("replication.key-filename", "hack/replication/client.key", "Path to the client private key file.")
	followerCmd.PersistentFlags().String("replication.ca-filename", "hack/replication/ca.crt", "Path to the client CA cert file.")
	followerCmd.PersistentFlags().Duration("replication.poll-interval", 1*time.Second, "Replication interval in seconds, the leader poll time.")
	followerCmd.PersistentFlags().Duration("replication.reconcile-interval", 30*time.Second, "Replication interval of tables reconciliation (workers startup/shutdown).")
	followerCmd.PersistentFlags().Duration("replication.lease-interval", 15*time.Second, "Interval in which the workers re-new their table leases.")
	followerCmd.PersistentFlags().Duration("replication.log-rpc-timeout", 1*time.Minute, "The log RPC timeout.")
	followerCmd.PersistentFlags().Duration("replication.snapshot-rpc-timeout", 1*time.Hour, "The snapshot RPC timeout.")
	followerCmd.PersistentFlags().Uint64("replication.max-recv-message-size-bytes", 8*1024*1024, "The maximum size of single replication message allowed to receive.")
	followerCmd.PersistentFlags().Uint64("replication.max-recovery-in-flight", 1, "The maximum number of recovery goroutines allowed to run in this instance.")
	followerCmd.PersistentFlags().Uint64("replication.max-snapshot-recv-bytes-per-second", 0, "Maximum bytes per second received by the snapshot API client, default value 0 means unlimited.")
}

var followerCmd = &cobra.Command{
	Use:   "follower",
	Short: "Start Regatta in follower mode.",
	RunE:  follower,
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

func follower(_ *cobra.Command, _ []string) error {
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
		LogDBImplementation: logDBImpl,
	})
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}
	if err := engine.Start(); err != nil {
		return fmt.Errorf("failed to start engine: %w", err)
	}
	defer engine.Close()

	// Replication
	{
		conn, err := createReplicationConn()
		defer func() {
			_ = conn.Close()
		}()
		if err != nil {
			return fmt.Errorf("cannot create replication conn: %w", err)
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
		defer d.Close()
	}

	// Start servers
	{
		{
			// Create regatta API server
			// Create server
			regatta, err := createAPIServer()
			if err != nil {
				return fmt.Errorf("failed to create API server: %w", err)
			}
			regattapb.RegisterKVServer(regatta, &regattaserver.ReadonlyKVServer{
				KVServer: regattaserver.KVServer{
					Storage: engine,
				},
			})
			regattapb.RegisterClusterServer(regatta, &regattaserver.ClusterServer{
				Cluster: engine,
			})
			if viper.GetBool("maintenance.enabled") {
				regattapb.RegisterMaintenanceServer(regatta, &regattaserver.ResetServer{Tables: engine, AuthFunc: authFunc(viper.GetString("maintenance.token"))})
			}
			// Start server
			go func() {
				if err := regatta.ListenAndServe(); err != nil {
					log.Errorf("grpc listenAndServe failed: %v", err)
				}
			}()
			defer regatta.Shutdown()
		}

		// Create REST server
		addr, _, _ := resolveUrl(viper.GetString("rest.address"))
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

func createReplicationConn() (*grpc.ClientConn, error) {
	addr, secure, net := resolveUrl(viper.GetString("replication.leader-address"))
	var creds grpc.DialOption
	if secure {
		c, err := cert.New(viper.GetString("replication.cert-filename"), viper.GetString("replication.key-filename"))
		if err != nil {
			return nil, fmt.Errorf("cannot load certificate: %w", err)
		}

		caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
		if err != nil {
			return nil, fmt.Errorf("cannot load server CA: %w", err)
		}
		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(caBytes)
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:              cp,
			MinVersion:           tls.VersionTLS12,
			GetClientCertificate: c.GetClientCertificate,
		}))
	} else {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	switch net {
	case "unix", "unixs":
		addr = fmt.Sprintf("unix://%s", addr)
	default:
		addr = fmt.Sprintf("dns:%s", addr)
	}

	replConn, err := grpc.Dial(addr, creds,
		grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                viper.GetDuration("replication.keepalive-time"),
			Timeout:             viper.GetDuration("replication.keepalive-timeout"),
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(viper.GetUint64("replication.max-recv-message-size-bytes")))),
	)
	if err != nil {
		return nil, err
	}
	return replConn, nil
}
