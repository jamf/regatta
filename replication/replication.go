// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/proto"
)

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
func NewManager(e *storage.Engine, queue *storage.IndexNotificationQueue, conn *grpc.ClientConn, cfg Config) *Manager {
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
		engine:            e,
		metadataClient:    regattapb.NewMetadataClient(conn),
		factory: &workerFactory{
			queue:             queue,
			reconcileInterval: cfg.ReconcileInterval,
			pollInterval:      cfg.Workers.PollInterval,
			leaseInterval:     cfg.Workers.LeaseInterval,
			logTimeout:        cfg.Workers.LogRPCTimeout,
			snapshotTimeout:   cfg.Workers.SnapshotRPCTimeout,
			maxSnapshotRecv:   cfg.Workers.MaxSnapshotRecv,
			recoverySemaphore: semaphore.NewWeighted(cfg.Workers.MaxRecoveryInFlight),
			engine:            e,
			log:               replicationLog,
			logClient:         regattapb.NewLogClient(conn),
			snapshotClient:    regattapb.NewSnapshotClient(conn),
			metrics: struct {
				replicationIndex  *prometheus.GaugeVec
				replicationLeased *prometheus.GaugeVec
			}{replicationIndex: replicationIndexGauge, replicationLeased: replicationLeaseGauge},
		},
		workers: struct {
			registry map[string]*worker
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
	engine            *storage.Engine
	metadataClient    regattapb.MetadataClient
	factory           *workerFactory
	workers           struct {
		registry map[string]*worker
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
}

func (m *Manager) Describe(descs chan<- *prometheus.Desc) {
	m.factory.metrics.replicationIndex.Describe(descs)
	m.factory.metrics.replicationLeased.Describe(descs)
}

func (m *Manager) Collect(metrics chan<- prometheus.Metric) {
	m.factory.metrics.replicationIndex.Collect(metrics)
	m.factory.metrics.replicationLeased.Collect(metrics)
}

// Start starts the replication manager goroutine, Close will stop it.
func (m *Manager) Start() {
	go func() {
		t := time.NewTicker(m.reconcileInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-m.closer:
				m.log.Info("replication stopped")
				return
			}
			if err := m.reconcileTables(); err != nil {
				m.log.Errorf("failed to reconcile tables: %v", err)
				continue
			}
			if err := m.reconcileWorkers(); err != nil {
				m.log.Errorf("failed to reconcile replication workers: %v", err)
				continue
			}
		}
	}()
}

func (m *Manager) reconcileTables() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := m.metadataClient.Get(ctx, &regattapb.MetadataRequest{})
	if err != nil {
		return err
	}
	leaderTables := response.GetTables()
	followerTables, err := m.engine.GetTables()
	if err != nil {
		return err
	}
	var toCreate, toDelete []string

	for _, ft := range followerTables {
		if !slices.ContainsFunc(leaderTables, func(lt *regattapb.Table) bool {
			return ft.Name == lt.Name
		}) {
			toDelete = append(toDelete, ft.Name)
		}
	}

	for _, ft := range leaderTables {
		if !slices.ContainsFunc(followerTables, func(lt table.Table) bool {
			return ft.Name == lt.Name
		}) {
			toCreate = append(toCreate, ft.Name)
		}
	}

	for _, name := range toDelete {
		if err := m.engine.DeleteTable(name); err != nil && !errors.Is(err, serrors.ErrTableNotFound) {
			return err
		}
	}

	for _, name := range toCreate {
		if _, err := m.engine.CreateTable(name); err != nil && !errors.Is(err, serrors.ErrTableExists) {
			return err
		}
	}
	return nil
}

func (m *Manager) reconcileWorkers() error {
	tbs, err := m.engine.GetTables()
	if err != nil {
		return err
	}

	for _, tbl := range tbs {
		if !m.hasWorker(tbl.Name) {
			m.startWorker(m.factory.create(tbl.Name))
		}
	}

	var toStop []*worker

	for name, w := range m.workers.registry {
		if !slices.ContainsFunc(tbs, func(t table.Table) bool {
			return t.Name == name
		}) {
			toStop = append(toStop, w)
		}
	}

	for _, w := range toStop {
		m.stopWorker(w)
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
	_, ok := m.workers.registry[name]
	return ok
}

func (m *Manager) startWorker(worker *worker) {
	m.log.Infof("launching replication for table %s", worker.table)
	m.workers.registry[worker.table] = worker
	m.workers.wg.Add(1)
	worker.Start()
}

func (m *Manager) stopWorker(worker *worker) {
	m.log.Infof("stopping replication for table %s", worker.table)
	worker.Close()
	m.workers.wg.Done()
	delete(m.workers.registry, worker.table)
}
