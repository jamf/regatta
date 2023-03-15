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
	"time"

	"github.com/cockroachdb/pebble/vfs"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/proto"
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
	followerCmd.PersistentFlags().Uint64("replication.max-snapshot-recv-bytes-per-second", 0, "Maximum bytes per second received by the snapshot API client, default value 0 means unlimited.")
}

var followerCmd = &cobra.Command{
	Use:   "follower",
	Short: "Start Regatta in follower mode.",
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
		},
		Meta: storage.MetaConfig{
			ElectionRTT:        viper.GetUint64("raft.election-rtt"),
			HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
			SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
			CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
			MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
		},
		LogDBImplementation: func() storage.LogDBImplementation {
			if viper.GetBool("experimental.tanlogdb") {
				return storage.Tan
			}
			return storage.Default
		}(),
	})
	if err != nil {
		log.Panic(err)
	}
	if err := engine.Start(); err != nil {
		log.Panic(err)
	}
	defer engine.Close()

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

		caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
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
			grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(histogramBuckets))
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
			regatta := createAPIServer(watcher)
			proto.RegisterKVServer(regatta, &regattaserver.ReadonlyKVServer{
				KVServer: regattaserver.KVServer{
					Storage: engine,
				},
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
			proto.RegisterMaintenanceServer(maintenance, &regattaserver.ResetServer{Tables: engine})
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
