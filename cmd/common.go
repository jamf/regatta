// Copyright JAMF Software, LLC

package cmd

import (
	"context"
	"crypto/tls"
	"runtime"
	"strconv"
	"sync"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jamf/regatta/cert"
	rl "github.com/jamf/regatta/log"
	"github.com/jamf/regatta/regattaserver"
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

func createAPIServer(cert *cert.Reloadable) *regattaserver.RegattaServer {
	return regattaserver.NewServer(
		viper.GetString("api.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: cert.GetCertificate,
		})),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 60 * time.Second,
		}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
}

func createMaintenanceServer(cert *cert.Reloadable) *regattaserver.RegattaServer {
	// Create regatta maintenance server
	return regattaserver.NewServer(
		viper.GetString("maintenance.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: cert.GetCertificate,
		})),
		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor, grpc_auth.StreamServerInterceptor(authFunc(viper.GetString("maintenance.token")))),
		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor, grpc_auth.UnaryServerInterceptor(authFunc(viper.GetString("maintenance.token")))),
	)
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
