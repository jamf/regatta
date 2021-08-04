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

func NewManager(tm *tables.Manager, nh *dragonboat.NodeHost, conn *grpc.ClientConn) *Manager {
	return &Manager{
		Interval: 30 * time.Second,
		tm:       tm,
		factory: workerFactory{
			interval:       10 * time.Second,
			timeout:        5 * time.Minute,
			tm:             tm,
			log:            zap.S().Named("replication"),
			nh:             nh,
			logClient:      proto.NewLogClient(conn),
			snapshotClient: proto.NewSnapshotClient(conn),
		},
		workers: struct {
			registry map[string]*worker
			mtx      sync.Mutex
			wg       sync.WaitGroup
		}{
			registry: make(map[string]*worker),
		},
		log:    zap.S().Named("replication").Named("data"),
		closer: make(chan struct{}),
		nh:     nh,
	}
}

type Manager struct {
	Interval time.Duration
	tm       *tables.Manager
	factory  workerFactory
	workers  struct {
		registry map[string]*worker
		mtx      sync.Mutex
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
	nh     *dragonboat.NodeHost
}

func (m *Manager) Start() {
	go func() {
		err := m.tm.WaitUntilReady()
		if err != nil {
			return
		}

		t := time.NewTicker(m.Interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err := m.reconcile()
				if err != nil {
					return
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

	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()
	for _, tbl := range tbs {
		if _, ok := m.workers.registry[tbl.Name]; !ok {
			m.log.Infof("launching replication for table %s", tbl.Name)
			worker := m.factory.Create(tbl.Name)
			m.workers.registry[tbl.Name] = worker
			worker.Start()
			m.workers.wg.Add(1)
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
			m.log.Infof("stopping replication for table %s", name)
			worker.Close()
			m.workers.wg.Done()
		}
	}
	return nil
}

func (m *Manager) Close() {
	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()

	for name, worker := range m.workers.registry {
		m.log.Infof("stopping replication for table %s", name)
		worker.Close()
		m.workers.wg.Done()
	}
	m.workers.wg.Wait()
	close(m.closer)
}
