package replication

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/log"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
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
	m.reconcileInterval = 250 * time.Millisecond
	wf := &mockWorkerFactory{}
	m.factory = wf
	m.log = zap.NewNop().Sugar()

	m.Start()

	wf.On("create", "test").Once().Return(&worker{
		Table:         "test",
		log:           m.log,
		pollInterval:  1 * time.Second,
		leaseInterval: 1 * time.Second,
		nh:            m.nh,
		tm:            m.tm,
		closer:        make(chan struct{}),
		metrics: struct {
			replicationIndex  prometheus.Gauge
			replicationLeased prometheus.Gauge
		}{
			replicationIndex: prometheus.NewGaugeVec(
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
			replicationIndex  prometheus.Gauge
			replicationLeased prometheus.Gauge
		}{
			replicationIndex: prometheus.NewGaugeVec(
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

	r.NoError(followerTM.CreateTable("test"))
	r.Eventually(func() bool {
		return m.hasWorker("test")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")

	r.NoError(followerTM.CreateTable("test2"))
	r.Eventually(func() bool {
		return m.hasWorker("test2")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")

	r.NoError(followerTM.DeleteTable("test"))
	r.Eventually(func() bool {
		return !m.hasWorker("test")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not deleted from registry")

	m.Close()
	r.Empty(m.workers.registry)
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