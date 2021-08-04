package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap/zaptest"
)

type mockWorkerFactory struct {
	mock.Mock
}

func (m *mockWorkerFactory) Create(table string) *worker {
	args := m.Called(table)
	return args.Get(0).(*worker)
}

func TestManager_reconcile(t *testing.T) {
	r := require.New(t)
	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := tables.NewManager(followerNH, followerAddresses, tableManagerTestConfig)
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

	m := NewManager(followerTM, followerNH, nil)
	m.Interval = 250 * time.Millisecond
	wf := &mockWorkerFactory{}
	m.factory = wf
	m.log = zaptest.NewLogger(t).Sugar()

	m.Start()

	wf.On("Create", "test").Once().Return(&worker{
		Table:    "test",
		log:      m.log,
		interval: 1 * time.Second,
		nh:       m.nh,
		tm:       m.tm,
		closer:   make(chan struct{}),
	})

	wf.On("Create", "test2").Once().Return(&worker{
		Table:    "test2",
		log:      m.log,
		interval: 1 * time.Second,
		nh:       m.nh,
		tm:       m.tm,
		closer:   make(chan struct{}),
	})

	r.NoError(followerTM.CreateTable("test"))
	r.Eventually(func() bool {
		_, ok := m.workers.registry["test"]
		return ok
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")

	r.NoError(followerTM.CreateTable("test2"))
	r.Eventually(func() bool {
		_, ok := m.workers.registry["test"]
		return ok
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")

	r.NoError(followerTM.DeleteTable("test"))
	r.Eventually(func() bool {
		_, ok := m.workers.registry["test"]
		return !ok
	}, 10*time.Second, 250*time.Millisecond, "replication worker not deleted from registry")

	m.Close()
	r.Empty(m.workers.registry)
}
