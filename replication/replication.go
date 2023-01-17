// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jamf/regatta/proto"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/storage/tables"
	"github.com/lni/dragonboat/v4"
	"github.com/prometheus/client_golang/prometheus"
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
			if toDelete, toCreate, err := m.tablesDiff(); err != nil {
				m.log.Errorf("could not compute table diff: %v", err)
			} else {
				// TODO(jsfpdn): Fix issue of starvation of reconciliation. Some worker can be pulling
				// snapshot from leader, postponing the stopping of the worker, blocking the whole
				// reconciliation goroutine.
				if len(toCreate) > 0 {
					m.log.Infof("reconciling tables - will replicate tables %v", toCreate)
					toStart := m.createNewTables(toCreate)
					m.startWorkers(toStart)
				}

				if len(toDelete) > 0 {
					m.log.Infof("reconciling tables - will stop replicating tables %v", toDelete)
					m.stopWorkers(toDelete)
					m.deleteTables(toDelete)
				}
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

// createNewTables to be replicated from the leader cluster. Returns names of tables that
// were created successfully and for which the replication routine should be started.
func (m *Manager) createNewTables(tables []string) (toStart []string) {
	unsuccessful := []string{}

	for _, table := range tables {
		if err := m.tm.CreateTable(table); err != nil && !errors.Is(err, serrors.ErrTableExists) {
			// Error is logged instead of returned to not halt the replication of other tables from
			// leader to follower cluster. We will try to create the table during the next tick.
			m.log.Errorf("could not create new table %s: %v", table, err)
			unsuccessful = append(unsuccessful, table)
		}
	}

	// Return only those tables that were created and their replication routines should be started.
	return filterUnsuccessful(tables, unsuccessful)
}

func (m *Manager) deleteTables(tables []string) {
	for _, table := range tables {
		// TODO(jsfpdn): Do we want to delete the data in the filesystem?
		if err := m.tm.DeleteTable(table); err != nil {
			// Error is logged instead of returned to not halt the deletion of other tables
			// in the follower cluster. We will try to delete the table during the next tick.
			m.log.Errorf("could not delete table %s: %v", table, err)
		}
	}
}

// startWorkers responsible for replicating the supplied tables.
func (m *Manager) startWorkers(tables []string) {
	for _, table := range tables {
		if !m.hasWorker(table) {
			m.startWorker(m.factory.create(table))
		} else {
			m.log.Warnf("newly created table %s already had a worker!", table)
		}
	}
}

// stopWorkers responsible for replicating the supplied tables.
func (m *Manager) stopWorkers(tables []string) {
	for _, table := range tables {
		if worker, ok := m.workers.registry[table]; ok {
			m.stopWorker(worker)
		} else {
			m.log.Warnf("tried to stop worker for table %s which does not exist", table)
		}
	}
}

// tablesDiff computes what tables to delete and to create.
func (m *Manager) tablesDiff() (toDelete, toCreate []string, err error) {
	ft, err := m.tm.GetTables()
	if err != nil {
		return nil, nil, fmt.Errorf("could not get follower's tables: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := m.metadataClient.Get(ctx, &proto.MetadataRequest{})
	if err != nil {
		return nil, nil, fmt.Errorf("could not get leader's tables: %v", err)
	}

	follower := sliceToMap(ft, func(entry table.Table) string { return entry.Name })
	leader := sliceToMap(response.GetTables(), func(entry *proto.Table) string { return entry.Name })
	toDelete, toCreate = diff(follower, leader)
	return toDelete, toCreate, nil
}

// sliceToMap transforms the supplied slice to map according to the function f.
func sliceToMap[K any](slice []K, f func(entry K) string) map[string]bool {
	res := map[string]bool{}
	for _, item := range slice {
		res[f(item)] = true
	}

	return res
}

// diff computes what tables should be created and which should be deleted.
// ft \ lt gives tables to be deleted, lt \ ft gives tables to be created.
func diff(follower map[string]bool, leader map[string]bool) (toDelete, toCreate []string) {
	// Tables present in the leader and not in the follower should be created.
	for table := range leader {
		if _, ok := follower[table]; !ok {
			toCreate = append(toCreate, table)
		}
	}

	// Tables present in the follower and not in the leader should be deleted.
	for table := range follower {
		if _, ok := leader[table]; !ok {
			toDelete = append(toDelete, table)
		}
	}

	return toDelete, toCreate
}

// filterUnsuccessful from the supplied slice of tables.
func filterUnsuccessful(tables, unsuccessful []string) []string {
	successful := []string{}

	tt := sliceToMap(unsuccessful, func(entry string) string { return entry })
	for _, table := range tables {
		if _, ok := tt[table]; !ok {
			successful = append(successful, table)
		}
	}

	return successful
}

// Close will stop replication goroutine - could be called just once.
func (m *Manager) Close() {
	m.closer <- struct{}{}
	for _, worker := range m.listWorkers() {
		m.stopWorker(worker)
	}
	m.workers.wg.Wait()
}

func (m *Manager) listWorkers() []*worker {
	workers := []*worker{}

	m.workers.mtx.Lock()
	defer m.workers.mtx.Unlock()

	for _, worker := range m.workers.registry {
		workers = append(workers, worker)
	}

	return workers
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
