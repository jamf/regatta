// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/storage"
	"github.com/jamf/regatta/storage/table"
	dbl "github.com/lni/dragonboat/v4/logger"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var histogramBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

func init() {
	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(histogramBuckets))
}

func createAPIServer() (*regattaserver.RegattaServer, error) {
	addr, secure, net := resolveUrl(viper.GetString("api.address"))
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 60 * time.Second}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	}
	if secure {
		c, err := cert.New(viper.GetString("api.cert-filename"), viper.GetString("api.key-filename"))
		if err != nil {
			return nil, fmt.Errorf("cannot load certificate: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: c.GetCertificate,
		})))
	}
	return regattaserver.NewServer(addr, net, opts...), nil
}

func resolveUrl(urlStr string) (addr string, secure bool, network string) {
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
