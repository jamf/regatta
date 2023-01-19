// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	"github.com/jamf/regatta/kafka"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var histogramBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

func createTableServer(watcher *cert.Watcher) *regattaserver.RegattaServer {
	return regattaserver.NewServer(
		viper.GetString("tables.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcher.GetCertificate(), nil
			},
		})),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 60 * time.Second,
		}),
		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor, grpc_auth.StreamServerInterceptor(authFunc(viper.GetString("tables.token")))),
		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor, grpc_auth.UnaryServerInterceptor(authFunc(viper.GetString("tables.token")))),
	)
}

func createAPIServer(watcher *cert.Watcher) *regattaserver.RegattaServer {
	return regattaserver.NewServer(
		viper.GetString("api.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcher.GetCertificate(), nil
			},
		})),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 60 * time.Second,
		}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
}

func createMaintenanceServer(watcher *cert.Watcher) *regattaserver.RegattaServer {
	// Create regatta maintenance server
	return regattaserver.NewServer(
		viper.GetString("maintenance.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcher.GetCertificate(), nil
			},
		})),
		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor, grpc_auth.StreamServerInterceptor(authFunc(viper.GetString("maintenance.token")))),
		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor, grpc_auth.UnaryServerInterceptor(authFunc(viper.GetString("maintenance.token")))),
	)
}

func authFunc(token string) func(ctx context.Context) (context.Context, error) {
	if token == "" {
		return func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}
	}
	return func(ctx context.Context) (context.Context, error) {
		t, err := grpc_auth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return ctx, err
		}
		if token != t {
			return ctx, status.Errorf(codes.Unauthenticated, "Invalid token")
		}
		return ctx, nil
	}
}

type tokenCredentials string

func (t tokenCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if t != "" {
		return map[string]string{"authorization": "Bearer " + string(t)}, nil
	}
	return nil, nil
}

func (tokenCredentials) RequireTransportSecurity() bool {
	return true
}

func parseInitialMembers(members map[string]string) (map[uint64]string, error) {
	initialMembers := make(map[uint64]string)
	for kStr, v := range members {
		kUint, err := strconv.ParseUint(kStr, 10, 64)
		if err != nil {
			return nil, err
		}
		initialMembers[kUint] = v
	}
	return initialMembers, nil
}

// closerFunc to be called to shutdown the resource.
type closerFunc func()

func startAPIServer(logger *zap.Logger, log *zap.SugaredLogger, server proto.KVServer) closerFunc {
	watcher := startWatcher(
		viper.GetString("api.cert-filename"),
		viper.GetString("api.key-filename"),
		logger.Named("cert-api").Sugar(),
	)

	// Create server
	regatta := createAPIServer(watcher)
	proto.RegisterKVServer(regatta, server)

	// Start server
	go func() {
		log.Infof("regatta listening at %s", regatta.Addr)
		if err := regatta.ListenAndServe(); err != nil {
			watcher.Stop()
			log.Panicf("grpc listenAndServe failed: %v", err)
		}
	}()

	return func() {
		watcher.Stop()
		regatta.Shutdown()
	}
}

func startReplicationServer(logger *zap.Logger, log *zap.SugaredLogger, engine *storage.Engine) closerFunc {
	watcher := startWatcher(
		viper.GetString("replication.cert-filename"),
		viper.GetString("replication.key-filename"),
		logger.Named("cert-replication").Sugar(),
	)

	caBytes, err := os.ReadFile(viper.GetString("replication.ca-filename"))
	if err != nil {
		log.Panicf("cannot load clients CA: %v", err)
	}

	replication := createLeaderReplicationServer(watcher, caBytes, logger.Named("server.replication"))
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
			watcher.Stop()
			log.Panicf("grpc listenAndServe failed: %v", err)
		}
	}()

	return func() {
		watcher.Stop()
		replication.Shutdown()
	}
}

func startMaintenanceServer(logger *zap.Logger, log *zap.SugaredLogger, maintenanceSrv proto.MaintenanceServer, metadataSrv *regattaserver.MetadataServer) closerFunc {
	watcher := startWatcher(
		viper.GetString("maintenance.cert-filename"),
		viper.GetString("maintenance.key-filename"),
		logger.Named("cert-maintenance").Sugar(),
	)

	maintenance := createMaintenanceServer(watcher)
	proto.RegisterMaintenanceServer(maintenance, maintenanceSrv)
	// &regattaserver.BackupServer{Tables: engine}
	if metadataSrv != nil {
		proto.RegisterMetadataServer(maintenance, metadataSrv)
		// &regattaserver.MetadataServer{Tables: engine}
	}

	// Start server
	go func() {
		log.Infof("regatta maintenance listening at %s", maintenance.Addr)
		if err := maintenance.ListenAndServe(); err != nil {
			watcher.Stop()
			log.Panicf("grpc listenAndServe failed: %v", err)
		}
	}()

	return func() {
		watcher.Stop()
		maintenance.Shutdown()
	}
}

func startRESTServer(log *zap.SugaredLogger) closerFunc {
	hs := regattaserver.NewRESTServer(viper.GetString("rest.address"), viper.GetDuration("rest.read-timeout"))
	go func() {
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("REST listenAndServe failed: %v", err)
		}
	}()
	return hs.Shutdown
}

func startTableServer(logger *zap.Logger, log *zap.SugaredLogger, tableSrv proto.TablesServer) closerFunc {
	watcher := startWatcher(
		viper.GetString("tables.cert-filename"),
		viper.GetString("tables.key-filename"),
		logger.Named("cert-tables").Sugar(),
	)

	ts := createTableServer(watcher)
	proto.RegisterTablesServer(ts, tableSrv)

	go func() {
		log.Infof("regatta table server listening at %s", ts.Addr)
		if err := ts.ListenAndServe(); err != nil {
			watcher.Stop()
			log.Panicf("grpc listenAndServe for table server failed: %v", err)
		}
	}()

	return func() {
		watcher.Stop()
		ts.Shutdown()
	}
}

func startWatcher(certFile, keyFile string, log *zap.SugaredLogger) *cert.Watcher {
	watcher := &cert.Watcher{
		CertFile: certFile,
		KeyFile:  keyFile,
		Log:      log,
	}

	if err := watcher.Watch(); err != nil {
		log.Panicf("cannot watch certificate: %v", err)
	}

	return watcher
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

func onMessage(st regattaserver.KVService) kafka.OnMessageFunc {
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

func startKafka(log *zap.SugaredLogger, engine *storage.Engine, shutdown chan os.Signal) closerFunc {
	topics := viper.GetStringSlice("kafka.topics")
	var tc []kafka.TopicConfig
	for _, topic := range topics {
		tc = append(tc, kafka.TopicConfig{
			Name:     topic,
			GroupID:  viper.GetString("kafka.group-id"),
			Table:    topic,
			Listener: onMessage(engine),
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
		return func() {}
	}

	// Start Kafka consumer
	consumer, err := kafka.NewConsumer(kafkaCfg)
	if err != nil {
		log.Panicf("failed to create consumer: %v", err)
	}
	prometheus.MustRegister(consumer)

	log.Info("start consuming...")
	if err := consumer.Start(context.Background()); err != nil {
		consumer.Close()
		log.Panicf("failed to start consumer: %v", err)
	}

	return consumer.Close
}

func logDeciderFunc(_ string, err error) bool {
	st, _ := status.FromError(err)
	return st != nil
}

func createLeaderReplicationServer(watcher *cert.Watcher, ca []byte, log *zap.Logger) *regattaserver.RegattaServer {
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

func startEngine(logger *zap.Logger, log *zap.SugaredLogger) (*storage.Engine, closerFunc) {
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
			WALDir:             viper.GetString("raft.state-machine-wal-dir"),
			NodeHostDir:        viper.GetString("raft.state-machine-dir"),
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
	},
	)
	if err != nil {
		log.Panic("could not create engine: %v", err)
	}
	if err := engine.Start(); err != nil {
		log.Panic("could not start engine: %v", err)
	}

	go func() {
		if err := engine.WaitUntilReady(); err != nil {
			log.Panicf("table manager failed to start: %v", err)
		}
		log.Info("table manager started")
	}()

	return engine, func() {
		if err := engine.Close(); err != nil {
			log.Errorf("could not close engine: %v", err)
		} else {
			log.Infof("closed engine")
		}
	}
}
