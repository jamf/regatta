// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/jamf/regatta/log"
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	logger.SetLoggerFactory(log.LoggerFactory(zap.NewNop()))
}

type mockWorkerFactory struct {
	mock.Mock
}

func (m *mockWorkerFactory) create(table string) *worker {
	args := m.Called(table)
	return args.Get(0).(*worker)
}

type mockMetadataClient struct {
	mock.Mock
}

func (m *mockMetadataClient) Get(ctx context.Context, in *proto.MetadataRequest, opts ...grpc.CallOption) (*proto.MetadataResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*proto.MetadataResponse), args.Error(1)
}

func TestManager_reconcile(t *testing.T) {
	r := require.New(t)
	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := tables.NewManager(followerNH, followerAddresses, tableManagerTestConfig())
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

	m := NewManager(followerTM, followerNH, nil, Config{})
	mc := &mockMetadataClient{}
	m.metadataClient = mc
	m.reconcileInterval = 250 * time.Millisecond
	wf := &mockWorkerFactory{}
	m.factory = wf
	m.log = zap.NewNop().Sugar()

	wf.On("create", "test").Once().Return(&worker{
		Table:         "test",
		log:           m.log,
		pollInterval:  1 * time.Second,
		leaseInterval: 1 * time.Second,
		nh:            m.nh,
		tm:            m.tm,
		closer:        make(chan struct{}),
		metrics: struct {
			replicationLeaderIndex   prometheus.Gauge
			replicationFollowerIndex prometheus.Gauge
			replicationLeased        prometheus.Gauge
		}{
			replicationLeaderIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			).WithLabelValues("leader", "test"),
			replicationFollowerIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			).WithLabelValues("follower", "test"),
			replicationLeased: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_leased",
					Help: "Regatta replication has the worker table leased",
				}, []string{"table"},
			).WithLabelValues("test"),
		},
	})

	wf.On("create", "test2").Once().Return(&worker{
		Table:         "test2",
		log:           m.log,
		pollInterval:  1 * time.Second,
		leaseInterval: 1 * time.Second,
		nh:            m.nh,
		tm:            m.tm,
		closer:        make(chan struct{}),
		metrics: struct {
			replicationLeaderIndex   prometheus.Gauge
			replicationFollowerIndex prometheus.Gauge
			replicationLeased        prometheus.Gauge
		}{
			replicationLeaderIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			).WithLabelValues("leader", "test2"),
			replicationFollowerIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			).WithLabelValues("follower", "test2"),
			replicationLeased: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_leased",
					Help: "Regatta replication has the worker table leased",
				}, []string{"table"},
			).WithLabelValues("test2"),
		},
	})

	mc.On("Get", mock.Anything, &proto.MetadataRequest{}, mock.Anything).Return(&proto.MetadataResponse{Tables: []*proto.Table{
		{
			Name: "test",
		},
		{
			Name: "test2",
		},
	}}, nil)

	m.Start()
	r.Eventually(func() bool {
		return m.hasWorker("test")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")
	r.Eventually(func() bool {
		return m.hasWorker("test2")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")
	m.Close()
	r.Empty(m.workers.registry)
}

func TestManager_reconcileTables(t *testing.T) {
	r := require.New(t)
	leaderTM, followerTM, leaderNH, followerNH, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create replicator")
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)

	m := NewManager(followerTM, followerNH, conn, Config{})

	t.Log("create table")
	r.NoError(leaderTM.CreateTable("test"))
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("create another table")
	r.NoError(leaderTM.CreateTable("test2"))
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test2")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("skip network errors")
	r.NoError(conn.Close())

	tabs, err := followerTM.GetTables()
	r.NoError(err)
	r.Len(tabs, 2)
}

func startRaftNode() (*dragonboat.NodeHost, map[uint64]string, error) {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 1,
		RaftAddress:    testNodeAddress,
	}
	_ = nhc.Prepare()
	nhc.Expert.FS = vfs.NewMem()
	nhc.Expert.Engine.ExecShards = 1
	nhc.Expert.LogDB.Shards = 1
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, nil, err
	}
	return nh, map[uint64]string{1: testNodeAddress}, nil
}

func getTestPort() int {
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func tableManagerTestConfig() tables.Config {
	return tables.Config{
		NodeID: 1,
		Table:  tables.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024},
		Meta:   tables.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	}
}
