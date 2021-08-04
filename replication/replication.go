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
		tm: tm,
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
	tm      *tables.Manager
	factory workerFactory
	workers struct {
		registry map[string]*worker
		mtx      sync.Mutex
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
	nh     *dragonboat.NodeHost
}

func (d *Manager) Start() {
	go func() {
		err := d.tm.WaitUntilReady()
		if err != nil {
			return
		}

		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err := d.reconcile()
				if err != nil {
					return
				}
			case <-d.closer:
				d.log.Info("replication stopped")
				return
			}
		}
	}()
}

func (d *Manager) reconcile() error {
	tbs, err := d.tm.GetTables()
	if err != nil {
		return err
	}

	d.workers.mtx.Lock()
	defer d.workers.mtx.Unlock()
	for _, tbl := range tbs {
		if _, ok := d.workers.registry[tbl.Name]; !ok {
			d.log.Infof("launching replication for table %s", tbl.Name)
			worker := d.factory.Create(tbl.Name)
			d.workers.registry[tbl.Name] = worker
			worker.Start()
			d.workers.wg.Add(1)
		}
	}

	for name, worker := range d.workers.registry {
		found := false
		for _, tbl := range tbs {
			if tbl.Name == name {
				found = true
				break
			}
		}
		if !found {
			d.log.Infof("stopping replication for table %s", name)
			worker.Close()
			d.workers.wg.Done()
		}
	}
	return nil
}

func (d *Manager) Close() {
	d.workers.mtx.Lock()
	defer d.workers.mtx.Unlock()

	for name, worker := range d.workers.registry {
		d.log.Infof("stopping replication for table %s", name)
		worker.Close()
		d.workers.wg.Done()
	}
	d.workers.wg.Wait()
	close(d.closer)
}
