package cmd

import (
	"context"
	"crypto/tls"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dbl "github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/plugin/tan"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var histogramBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

func createTableManager(nh *dragonboat.NodeHost) (*tables.Manager, error) {
	initialMembers, err := parseInitialMembers(viper.GetStringMapString("raft.initial-members"))
	if err != nil {
		return nil, err
	}

	tm := tables.NewManager(nh, initialMembers,
		tables.Config{
			NodeID: viper.GetUint64("raft.node-id"),
			Table: tables.TableConfig{
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
			Meta: tables.MetaConfig{
				ElectionRTT:        viper.GetUint64("raft.election-rtt"),
				HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
				SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
				CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
				MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
			},
		})
	return tm, nil
}

func createAPIServer(watcher *cert.Watcher) *regattaserver.RegattaServer {
	return regattaserver.NewServer(
		viper.GetString("api.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
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

func createNodeHost(logger *zap.Logger) (*dragonboat.NodeHost, error) {
	dbl.SetLoggerFactory(rl.LoggerFactory(logger))
	dbl.GetLogger("raft").SetLevel(dbl.DEBUG)
	dbl.GetLogger("rsm").SetLevel(dbl.DEBUG)
	dbl.GetLogger("transport").SetLevel(dbl.DEBUG)
	dbl.GetLogger("dragonboat").SetLevel(dbl.DEBUG)
	dbl.GetLogger("logdb").SetLevel(dbl.DEBUG)
	dbl.GetLogger("settings").SetLevel(dbl.DEBUG)

	nhc := config.NodeHostConfig{
		WALDir:                        viper.GetString("raft.wal-dir"),
		NodeHostDir:                   viper.GetString("raft.node-host-dir"),
		RTTMillisecond:                uint64(viper.GetDuration("raft.rtt").Milliseconds()),
		RaftAddress:                   viper.GetString("raft.address"),
		ListenAddress:                 viper.GetString("raft.listen-address"),
		EnableMetrics:                 true,
		MaxSnapshotRecvBytesPerSecond: viper.GetUint64("raft.max-snapshot-recv-bytes-per-second"),
		MaxSnapshotSendBytesPerSecond: viper.GetUint64("raft.max-snapshot-send-bytes-per-second"),
		MaxReceiveQueueSize:           viper.GetUint64("raft.max-recv-queue-size"),
		MaxSendQueueSize:              viper.GetUint64("raft.max-send-queue-size"),
	}

	if viper.GetBool("experimental.tanlogdb") {
		nhc.Expert.LogDBFactory = tan.Factory
	} else {
		nhc.Expert.LogDB = buildLogDBConfig()
	}

	err := nhc.Prepare()
	if err != nil {
		return nil, err
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	return nh, nil
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

func buildLogDBConfig() config.LogDBConfig {
	cfg := config.GetSmallMemLogDBConfig()
	cfg.KVRecycleLogFileNum = 4
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	return cfg
}

type token string

func (t token) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if t != "" {
		return map[string]string{"authorization": "Bearer " + string(t)}, nil
	}
	return nil, nil
}

func (token) RequireTransportSecurity() bool {
	return true
}
