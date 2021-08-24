package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/lni/dragonboat/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/snapshot"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

var (
	ErrLeaderBehind = errors.New("leader behind")
	ErrUseSnapshot  = errors.New("use snapshot")
)

type workerFactory struct {
	pollInterval      time.Duration
	leaseInterval     time.Duration
	logTimeout        time.Duration
	snapshotTimeout   time.Duration
	maxSnapshotRecv   uint64
	recoverySemaphore *semaphore.Weighted
	tm                *tables.Manager
	log               *zap.SugaredLogger
	nh                *dragonboat.NodeHost
	logClient         proto.LogClient
	snapshotClient    proto.SnapshotClient
	metrics           struct {
		replicationIndex  *prometheus.GaugeVec
		replicationLeased *prometheus.GaugeVec
	}
}

func (f *workerFactory) create(table string) *worker {
	return &worker{
		logClient:         f.logClient,
		snapshotClient:    f.snapshotClient,
		tm:                f.tm,
		nh:                f.nh,
		pollInterval:      f.pollInterval,
		leaseInterval:     f.leaseInterval,
		logTimeout:        f.logTimeout,
		snapshotTimeout:   f.snapshotTimeout,
		recoverySemaphore: f.recoverySemaphore,
		maxSnapshotRecv:   f.maxSnapshotRecv,
		Table:             table,
		closer:            make(chan struct{}),
		log:               f.log.Named(table),
		metrics: struct {
			replicationIndex  prometheus.Gauge
			replicationLeased prometheus.Gauge
		}{
			replicationIndex:  f.metrics.replicationIndex.WithLabelValues("follower", table),
			replicationLeased: f.metrics.replicationLeased.WithLabelValues(table),
		},
	}
}

// worker connects to the log replication service and synchronizes the local state.
type worker struct {
	Table             string
	pollInterval      time.Duration
	leaseInterval     time.Duration
	logTimeout        time.Duration
	snapshotTimeout   time.Duration
	tm                *tables.Manager
	closer            chan struct{}
	log               *zap.SugaredLogger
	nh                *dragonboat.NodeHost
	logClient         proto.LogClient
	snapshotClient    proto.SnapshotClient
	leased            uint32
	recoverySemaphore *semaphore.Weighted
	maxSnapshotRecv   uint64
	metrics           struct {
		replicationIndex  prometheus.Gauge
		replicationLeased prometheus.Gauge
	}
	wg sync.WaitGroup
}

// Start launches the replication goroutine. To stop it, call worker.Close.
func (w *worker) Start() {
	w.wg.Add(1)
	go func() {
		defer func() {
			w.log.Info("lease routine stopped")
			w.wg.Done()
		}()

		w.log.Info("lease routine started")
		t := time.NewTicker(w.leaseInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err := w.tm.LeaseTable(w.Table, w.leaseInterval*4)
				if err == nil {
					prev := atomic.SwapUint32(&w.leased, 1)
					if prev == 0 {
						w.log.Info("lease acquired")
					}
					w.metrics.replicationLeased.Set(1)
				} else {
					prev := atomic.SwapUint32(&w.leased, 0)
					if prev == 1 {
						w.log.Info("lease lost")
					}
					w.metrics.replicationLeased.Set(0)
				}
			case <-w.closer:
				return
			}
		}
	}()

	w.wg.Add(1)
	go func() {
		defer func() {
			w.log.Info("replication routine stopped")
			w.wg.Done()
		}()

		w.log.Info("replication routine started")
		t := time.NewTicker(w.pollInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				leaderIndex, clusterID, err := w.tableState()
				if err != nil {
					w.log.Errorf("cannot query leader index: %v", err)
					continue
				}
				w.metrics.replicationIndex.Set(float64(leaderIndex))

				if atomic.LoadUint32(&w.leased) != 1 {
					w.log.Debug("skipping replication - table not leased")
					continue
				}

				if err := w.do(leaderIndex, clusterID); err != nil {
					if err == ErrLeaderBehind {
						w.log.Errorf("the leader log is behind the replication will stop")
						return
					}
					w.log.Warnf("worker error %v", err)
				}
			case <-w.closer:
				return
			}
		}
	}()
}

// Close stops the replication.
func (w *worker) Close() {
	close(w.closer)
	w.wg.Wait()

	ok, err := w.tm.ReturnTable(w.Table)
	if err != nil {
		w.log.Errorf("returning table failed %v", err)
	}
	if ok {
		w.log.Info("table returned")
	}
}

func (w *worker) do(leaderIndex, clusterID uint64) error {
	replicateRequest := &proto.ReplicateRequest{
		LeaderIndex: leaderIndex + 1,
		Table:       []byte(w.Table),
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.logTimeout)
	defer cancel()
	stream, err := w.logClient.Replicate(ctx, replicateRequest)
	if err != nil {
		return fmt.Errorf("could not open log stream: %w", err)
	}

	if err = w.read(ctx, stream, clusterID); err != nil {
		switch err {
		case ErrUseSnapshot:
			if w.recoverySemaphore.TryAcquire(1) {
				defer w.recoverySemaphore.Release(1)
				return w.recover()
			}
			w.log.Info("maximum number of recoveries of already running")
			ok, err := w.tm.ReturnTable(w.Table)
			if err != nil {
				return err
			}
			if ok {
				w.log.Info("table returned")
			}
			return nil
		case ErrLeaderBehind:
			return ErrLeaderBehind
		default:
			return fmt.Errorf("could not store the log stream: %w", err)
		}
	}
	return nil
}

func (w *worker) tableState() (uint64, uint64, error) {
	t, err := w.tm.GetTable(w.Table)
	if err != nil {
		return 0, 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.logTimeout)
	defer cancel()
	idxRes, err := t.LeaderIndex(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get leader index key: %w", err)
	}
	return idxRes.Index, t.ClusterID, nil
}

// read commands from the stream and save them to the cluster.
func (w *worker) read(ctx context.Context, stream proto.Log_ReplicateClient, clusterID uint64) error {
	for {
		replicateRes, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not read from the stream: %v", err)
		}

		switch res := replicateRes.Response.(type) {
		case *proto.ReplicateResponse_CommandsResponse:
			if err := w.proposeBatch(ctx, res.CommandsResponse.GetCommands(), clusterID); err != nil {
				return fmt.Errorf("could not propose: %w", err)
			}
		case *proto.ReplicateResponse_ErrorResponse:
			if res.ErrorResponse.Error == proto.ReplicateError_LEADER_BEHIND {
				return ErrLeaderBehind
			}

			if res.ErrorResponse.Error == proto.ReplicateError_USE_SNAPSHOT {
				return ErrUseSnapshot
			}

			return fmt.Errorf(
				"unknown replicate error response '%s' with id %d",
				res.ErrorResponse.Error.String(),
				res.ErrorResponse.Error,
			)
		}
	}
}

func (w *worker) proposeBatch(ctx context.Context, commands []*proto.ReplicateCommand, clusterID uint64) error {
	var buff []byte
	for _, c := range commands {
		size := c.Command.SizeVT()
		if cap(buff) < size {
			buff = make([]byte, 0, size)
		}
		buff = buff[:size]
		n, err := c.Command.MarshalToSizedBufferVT(buff)
		if err != nil {
			return fmt.Errorf("could not marshal command: %w", err)
		}

		if _, err := w.nh.SyncPropose(ctx, w.nh.GetNoOPSession(clusterID), buff[:n]); err != nil {
			return fmt.Errorf("could not SyncPropose: %w", err)
		}
	}
	return nil
}

func (w *worker) recover() error {
	w.log.Info("recovering from snapshot")
	ctx, cancel := context.WithTimeout(context.Background(), w.snapshotTimeout)
	defer cancel()
	stream, err := w.snapshotClient.Stream(ctx, &proto.SnapshotRequest{Table: []byte(w.Table)})
	if err != nil {
		return err
	}

	sf, err := snapshot.NewTemp()
	if err != nil {
		return err
	}
	defer func() {
		err := sf.Close()
		if err != nil {
			return
		}
		_ = os.Remove(sf.Path())
	}()

	r := &snapshot.Reader{Stream: stream}
	if w.maxSnapshotRecv != 0 {
		r.Bucket = ratelimit.NewBucketWithRate(float64(w.maxSnapshotRecv), int64(w.maxSnapshotRecv)*2)
	}

	_, err = io.Copy(sf.File, r)
	if err != nil {
		return err
	}

	err = sf.Sync()
	if err != nil {
		return err
	}
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	w.log.Info("snapshot stream saved, loading table")
	err = w.tm.LoadTableFromSnapshot(w.Table, sf)
	if err != nil {
		return err
	}
	w.log.Info("table recovered")
	return nil
}
