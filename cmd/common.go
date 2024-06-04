// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	rl "github.com/jamf/regatta/log"
	dbl "github.com/jamf/regatta/raft/logger"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/security"
	"github.com/jamf/regatta/storage"
	"github.com/jamf/regatta/storage/table"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var (
	apiBuckets  = []float64{.0001, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 30}
	grpcmetrics = grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram(grpcprom.WithHistogramBuckets(apiBuckets)))
)

func init() {
	prometheus.DefaultRegisterer.MustRegister(grpcmetrics)
}

func createAPIServer(log *zap.Logger, reg func(grpc.ServiceRegistrar)) (*regattaserver.RegattaServer, error) {
	addr, secure, nw := resolveURL(viper.GetString("api.address"))
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 60 * time.Second}),
		grpc.ChainStreamInterceptor(
			auth.StreamServerInterceptor(defaultAuthFunc),
			grpcmetrics.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			auth.UnaryServerInterceptor(defaultAuthFunc),
			grpcmetrics.UnaryServerInterceptor(),
		),
		grpc.MaxConcurrentStreams(viper.GetUint32("api.max-concurrent-streams")),
	}
	workers := viper.GetInt("api.stream-workers")
	if workers > 0 {
		opts = append(opts, grpc.NumStreamWorkers(uint32(workers)))
	} else if workers == 0 {
		opts = append(opts, grpc.NumStreamWorkers(uint32(runtime.NumCPU()+1)))
	}
	if secure {
		ti := security.TLSInfo{
			CertFile:        viper.GetString("api.cert-filename"),
			KeyFile:         viper.GetString("api.key-filename"),
			TrustedCAFile:   viper.GetString("api.ca-filename"),
			ClientCertAuth:  viper.GetBool("api.client-cert-auth"),
			AllowedCN:       viper.GetString("api.allowed-cn"),
			AllowedHostname: viper.GetString("api.allowed-hostname"),
			Logger:          log.Named("cert").Sugar(),
		}
		cfg, err := ti.ServerConfig()
		if err != nil {
			return nil, fmt.Errorf("cannot build tls config: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	l, err := net.Listen(nw, addr)
	if err != nil {
		return nil, err
	}
	if limit := viper.GetUint32("api.max-concurrent-connections"); limit > 0 {
		l = netutil.LimitListener(l, int(limit))
	}
	server := regattaserver.NewServer(l, log.Sugar(), opts...)
	reg(server)
	grpcmetrics.InitializeMetrics(server.Server)
	return server, nil
}

func resolveURL(urlStr string) (addr string, secure bool, network string) {
	u, err := url.Parse(urlStr)
	if err != nil {
		log.Panicf("cannot parse address: %v", err)
	}
	addr = u.Host
	network = "tcp"
	if u.Scheme == "unix" || u.Scheme == "unixs" {
		addr = u.Host + u.Path
		network = "unix"
	}
	secure = u.Scheme == "https" || u.Scheme == "unixs"
	return addr, secure, network
}

func toRecoveryType(str string) table.SnapshotRecoveryType {
	switch str {
	case "snapshot":
		return table.RecoveryTypeSnapshot
	case "checkpoint":
		return table.RecoveryTypeCheckpoint
	default:
		if runtime.GOOS == "windows" {
			return table.RecoveryTypeSnapshot
		}
		return table.RecoveryTypeCheckpoint
	}
}

func authFunc(token string) func(ctx context.Context) (context.Context, error) {
	if token == "" {
		return func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}
	}
	return func(ctx context.Context) (context.Context, error) {
		t, err := auth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return ctx, err
		}
		if token != t {
			return ctx, status.Errorf(codes.Unauthenticated, "Invalid token")
		}
		return ctx, nil
	}
}

var defaultAuthFunc = authFunc("")

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

func parseLogDBImplementation(impl string) (storage.LogDBImplementation, error) {
	switch impl {
	case "pebble":
		return storage.Pebble, nil
	case "tan":
		return storage.Tan, nil
	default:
		return storage.Pebble, fmt.Errorf("unknown logdb impl: %s", impl)
	}
}

var dbLoggerOnce sync.Once

func setupDragonboatLogger(logger *zap.Logger) {
	dbLoggerOnce.Do(func() {
		dbl.SetLoggerFactory(rl.LoggerFactory(logger))
		dbl.GetLogger("raft").SetLevel(dbl.WARNING)
		dbl.GetLogger("rsm").SetLevel(dbl.WARNING)
		dbl.GetLogger("transport").SetLevel(dbl.ERROR)
		dbl.GetLogger("dragonboat").SetLevel(dbl.WARNING)
		dbl.GetLogger("logdb").SetLevel(dbl.INFO)
		dbl.GetLogger("tan").SetLevel(dbl.INFO)
		dbl.GetLogger("settings").SetLevel(dbl.INFO)
	})
}

var secretConfigs = []string{
	"maintenance.token",
	"tables.token",
}

func viperConfigReader() map[string]any {
	res := make(map[string]any)
	for _, key := range viper.AllKeys() {
		if slices.Contains(secretConfigs, key) {
			res[key] = "**********"
		} else {
			res[key] = viper.Get(key)
		}
	}
	return res
}
