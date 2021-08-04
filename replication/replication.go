package replication

import (
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type workerCreator interface {
	create(table string) *worker
}

// NewManager constructs a new replication Manager out of tables.Manager, dragonboat.NodeHost and replication API grpc.ClientConn.
func NewManager(tm *tables.Manager, nh *dragonboat.NodeHost, conn *grpc.ClientConn) *Manager {
	replicationLog := zap.S().Named("replication")
	return &Manager{
		Interval: 30 * time.Second,
		tm:       tm,
		factory: &workerFactory{
			interval:       10 * time.Second,
			timeout:        5 * time.Minute,
			tm:             tm,
			log:            replicationLog,
			nh:             nh,
			logClient:      proto.NewLogClient(conn),
			snapshotClient: proto.NewSnapshotClient(conn),
		},
		workers: struct {
			registry map[string]*worker
			mtx      sync.RWMutex
			wg       sync.WaitGroup
		}{
			registry: make(map[string]*worker),
		},
		log:    replicationLog.Named("manager"),
		closer: make(chan struct{}),
	}
}

// Manager schedules replication workers.
type Manager struct {
	Interval time.Duration
	tm       *tables.Manager
	factory  workerCreator
	workers  struct {
		registry map[string]*worker
		mtx      sync.RWMutex
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
	nh     *dragonboat.NodeHost
}

// Start starts the replication manager goroutine, Close will stop it.
func (m *Manager) Start() {
	go func() {
		err := m.tm.WaitUntilReady()
		if err != nil {
			m.log.Errorf("manager failed to start: %v", err)
			return
		}

		t := time.NewTicker(m.Interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := m.reconcile(); err != nil {
					m.log.Warnf("reconciler error: %v", err)
				}
			case <-m.closer:
				m.log.Info("replication stopped")
				return
			}
		}
	}()
}

func (m *Manager) reconcile() error {
	tbs, err := m.tm.GetTables()
	if err != nil {
		return err
	}

	for _, tbl := range tbs {
		if _, ok := m.workers.registry[tbl.Name]; !ok {
			worker := m.factory.create(tbl.Name)
			m.startWorker(worker)
		}
	}

	for name, worker := range m.workers.registry {
		found := false
		for _, tbl := range tbs {
			if tbl.Name == name {
				found = true
				break
			}
		}
		if !found {
			m.stopWorker(worker)
		}
	}
	return nil
}

// Close will stop replication goroutine - could be called just once.
func (m *Manager) Close() {
	m.closer <- struct{}{}
	for _, worker := range m.workers.registry {
		m.stopWorker(worker)
	}
	m.workers.wg.Wait()
}

func (m *Manager) hasWorker(name string) bool {
	m.workers.mtx.RLock()
	defer m.workers.mtx.RUnlock()
	_, ok := m.workers.registry[name]
	return ok
}

func (m *Manager) startWorker(worker *worker) {
	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()

	m.log.Infof("launching replication for table %s", worker.Table)
	m.workers.registry[worker.Table] = worker
	worker.Start()
	m.workers.wg.Add(1)
}

func (m *Manager) stopWorker(worker *worker) {
	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()

	m.log.Infof("stopping replication for table %s", worker.Table)
	worker.Close()
	m.workers.wg.Done()
	delete(m.workers.registry, worker.Table)
}
