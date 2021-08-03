package replication

import (
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
)

func NewData(tm *tables.Manager, nh *dragonboat.NodeHost, logc proto.LogClient) *Data {
	return &Data{
		tm:   tm,
		logc: logc,
		workers: struct {
			registry map[string]*Log
			mtx      sync.Mutex
			wg       sync.WaitGroup
		}{
			registry: make(map[string]*Log),
		},
		log:    zap.S().Named("replication").Named("data"),
		closer: make(chan struct{}),
		nh:     nh,
	}
}

type Data struct {
	tm      *tables.Manager
	workers struct {
		registry map[string]*Log
		mtx      sync.Mutex
		wg       sync.WaitGroup
	}
	log    *zap.SugaredLogger
	closer chan struct{}
	logc   proto.LogClient
	nh     *dragonboat.NodeHost
}

func (d *Data) Replicate() {
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

func (d *Data) reconcile() error {
	tbs, err := d.tm.GetTables()
	if err != nil {
		return err
	}

	d.workers.mtx.Lock()
	defer d.workers.mtx.Unlock()
	for _, tbl := range tbs {
		if _, ok := d.workers.registry[tbl.Name]; !ok {
			d.log.Infof("launching replication for table %s", tbl.Name)
			worker := NewLog(d.logc, d.tm, d.nh, tbl.Name, 30*time.Second)
			d.workers.registry[tbl.Name] = worker
			worker.Replicate()
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

func (d *Data) Close() {
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
