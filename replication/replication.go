package replication

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/proto"
)

type workerCreator interface {
	create(table string) *worker
}

type WorkerConfig struct {
	PollInterval        time.Duration
	LeaseInterval       time.Duration
	LogRPCTimeout       time.Duration
	SnapshotRPCTimeout  time.Duration
	MaxRecoveryInFlight int64
	MaxSnapshotRecv     uint64
}

type Config struct {
	ReconcileInterval time.Duration
	Workers           WorkerConfig
}

// NewManager constructs a new replication Manager out of tables.Manager, dragonboat.NodeHost and replication API grpc.ClientConn.
func NewManager(tm *tables.Manager, nh *dragonboat.NodeHost, conn *grpc.ClientConn, cfg Config) *Manager {
	replicationLog := zap.S().Named("replication")

	replicationIndexGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "regatta_replication_index",
			Help: "Regatta replication index",
		}, []string{"role", "table"},
	)
	replicationLeaseGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "regatta_replication_leased",
			Help: "Regatta replication has the worker table leased",
		}, []string{"table"},
	)

	return &Manager{
		reconcileInterval: cfg.ReconcileInterval,
		tm:                tm,
		metadataClient:    proto.NewMetadataClient(conn),
		factory: &workerFactory{
			pollInterval:      cfg.Workers.PollInterval,
			leaseInterval:     cfg.Workers.LeaseInterval,
			logTimeout:        cfg.Workers.LogRPCTimeout,
			snapshotTimeout:   cfg.Workers.SnapshotRPCTimeout,
			maxSnapshotRecv:   cfg.Workers.MaxSnapshotRecv,
			recoverySemaphore: semaphore.NewWeighted(cfg.Workers.MaxRecoveryInFlight),
			tm:                tm,
			log:               replicationLog,
			nh:                nh,
			logClient:         proto.NewLogClient(conn),
			snapshotClient:    proto.NewSnapshotClient(conn),
			metrics: struct {
				replicationIndex  *prometheus.GaugeVec
				replicationLeased *prometheus.GaugeVec
			}{replicationIndex: replicationIndexGauge, replicationLeased: replicationLeaseGauge},
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
	reconcileInterval time.Duration
	tm                *tables.Manager
	metadataClient    proto.MetadataClient
	factory           workerCreator
	workers           struct {
		registry map[string]*worker
		mtx      sync.RWMutex
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
	nh     *dragonboat.NodeHost
}

func (m *Manager) Describe(descs chan<- *prometheus.Desc) {
	if factory, ok := m.factory.(*workerFactory); ok {
		factory.metrics.replicationIndex.Describe(descs)
		factory.metrics.replicationLeased.Describe(descs)
	}
}

func (m *Manager) Collect(metrics chan<- prometheus.Metric) {
	if factory, ok := m.factory.(*workerFactory); ok {
		factory.metrics.replicationIndex.Collect(metrics)
		factory.metrics.replicationLeased.Collect(metrics)
	}
}

// Start starts the replication manager goroutine, Close will stop it.
func (m *Manager) Start() {
	go func() {
		err := m.tm.WaitUntilReady()
		if err != nil {
			m.log.Errorf("manager failed to start: %v", err)
			return
		}
		t := time.NewTicker(m.reconcileInterval)
		defer t.Stop()
		for {
			if err := m.reconcileTables(); err != nil {
				m.log.Errorf("failed to reconcile tables: %v", err)
			}
			if err := m.reconcileWorkers(); err != nil {
				m.log.Errorf("failed to reconcile replication workers: %v", err)
			}
			select {
			case <-t.C:
				continue
			case <-m.closer:
				m.log.Info("replication stopped")
				return
			}
		}
	}()
}

func (m *Manager) reconcileTables() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := m.metadataClient.Get(ctx, &proto.MetadataRequest{})
	if err != nil {
		return err
	}
	for _, tabs := range response.GetTables() {
		if err := m.tm.CreateTable(tabs.Name); err != nil && !errors.Is(err, tables.ErrTableExists) {
			return err
		}
	}
	return nil
}

func (m *Manager) reconcileWorkers() error {
	tbs, err := m.tm.GetTables()
	if err != nil {
		return err
	}

	for _, tbl := range tbs {
		if !m.hasWorker(tbl.Name) {
			m.startWorker(m.factory.create(tbl.Name))
		}
	}

	m.workers.mtx.RLock()
	defer m.workers.mtx.RUnlock()
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
	m.workers.wg.Add(1)
	worker.Start()
}

func (m *Manager) stopWorker(worker *worker) {
	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()

	m.log.Infof("stopping replication for table %s", worker.Table)
	worker.Close()
	m.workers.wg.Done()
	delete(m.workers.registry, worker.Table)
}
