package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strconv"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	dbl "github.com/lni/dragonboat/v3/logger"
	"github.com/spf13/viper"
	"github.com/wandera/regatta/cert"
	"github.com/wandera/regatta/kafka"
	rl "github.com/wandera/regatta/log"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

func createTableManager(nh *dragonboat.NodeHost) (*tables.Manager, error) {
	initialMembers, err := parseInitialMembers(viper.GetStringMapString("raft.initial-members"))
	if err != nil {
		return nil, err
	}

	tm := tables.NewManager(nh, initialMembers,
		tables.Config{
			NodeID: viper.GetUint64("raft.node-id"),
			Table: tables.TableConfig{
				ElectionRTT:        viper.GetUint64("raft.election-rtt"),
				HeartbeatRTT:       viper.GetUint64("raft.heartbeat-rtt"),
				SnapshotEntries:    viper.GetUint64("raft.snapshot-entries"),
				CompactionOverhead: viper.GetUint64("raft.compaction-overhead"),
				MaxInMemLogSize:    viper.GetUint64("raft.max-in-mem-log-size"),
				WALDir:             viper.GetString("raft.state-machine-wal-dir"),
				NodeHostDir:        viper.GetString("raft.state-machine-dir"),
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

func createAPIServer(watcher *cert.Watcher, st *tables.KVStorageWrapper, mTables []string) *regattaserver.RegattaServer {
	regatta := regattaserver.NewServer(
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

	// Create and register grpc/rest endpoints
	kvs := &regattaserver.KVServer{
		Storage:       st,
		ManagedTables: mTables,
	}
	proto.RegisterKVServer(regatta, kvs)

	ms := &regattaserver.MaintenanceServer{
		Storage: st,
	}
	proto.RegisterMaintenanceServer(regatta, ms)
	return regatta
}

func createReplicationServer(watcherReplication *cert.Watcher, ca []byte, manager *tables.Manager) *regattaserver.RegattaServer {
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(ca)

	// Create regatta replication server
	replication := regattaserver.NewServer(
		viper.GetString("replication.address"),
		viper.GetBool("api.reflection-api"),
		grpc.Creds(credentials.NewTLS(&tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  cp,
			GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return watcherReplication.GetCertificate(), nil
			},
		})),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)

	proto.RegisterMetadataServer(replication, &regattaserver.MetadataServer{Manager: manager})
	return replication
}

func createNodeHost(logger *zap.Logger) (*dragonboat.NodeHost, error) {
	dbl.SetLoggerFactory(rl.LoggerFactory(logger))
	dbl.GetLogger("raft").SetLevel(dbl.DEBUG)
	dbl.GetLogger("rsm").SetLevel(dbl.DEBUG)
	dbl.GetLogger("transport").SetLevel(dbl.DEBUG)
	dbl.GetLogger("dragonboat").SetLevel(dbl.DEBUG)
	dbl.GetLogger("logdb").SetLevel(dbl.DEBUG)

	nhc := config.NodeHostConfig{
		WALDir:         viper.GetString("raft.wal-dir"),
		NodeHostDir:    viper.GetString("raft.node-host-dir"),
		RTTMillisecond: uint64(viper.GetDuration("raft.rtt").Milliseconds()),
		RaftAddress:    viper.GetString("raft.address"),
		ListenAddress:  viper.GetString("raft.listen-address"),
		EnableMetrics:  true,
	}
	nhc.Expert.LogDB = buildLogDBConfig()

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

func buildLogDBConfig() config.LogDBConfig {
	cfg := config.GetSmallMemLogDBConfig()
	cfg.KVRecycleLogFileNum = 4
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	return cfg
}

func onMessage(st storage.KVStorage) kafka.OnMessageFunc {
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
