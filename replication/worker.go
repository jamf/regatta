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
	pb "google.golang.org/protobuf/proto"
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
func (l *worker) Start() {
	l.wg.Add(1)
	go func() {
		defer func() {
			l.log.Info("lease routine stopped")
			l.wg.Done()
		}()

		l.log.Info("lease routine started")
		t := time.NewTicker(l.leaseInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err := l.tm.LeaseTable(l.Table, l.leaseInterval*4)
				if err == nil {
					prev := atomic.SwapUint32(&l.leased, 1)
					if prev == 0 {
						l.log.Info("lease acquired")
					}
					l.metrics.replicationLeased.Set(1)
				} else {
					prev := atomic.SwapUint32(&l.leased, 0)
					if prev == 1 {
						l.log.Info("lease lost")
					}
					l.metrics.replicationLeased.Set(0)
				}
			case <-l.closer:
				return
			}
		}
	}()

	l.wg.Add(1)
	go func() {
		defer func() {
			l.log.Info("replication routine stopped")
			l.wg.Done()
		}()

		l.log.Info("replication routine started")
		t := time.NewTicker(l.pollInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				leaderIndex, clusterID, err := l.tableState()
				if err != nil {
					l.log.Errorf("cannot query leader index: %v", err)
					continue
				}
				l.metrics.replicationIndex.Set(float64(leaderIndex))

				if atomic.LoadUint32(&l.leased) != 1 {
					l.log.Debug("skipping replication - table not leased")
					continue
				}

				if err := l.do(leaderIndex, clusterID); err != nil {
					if err == ErrLeaderBehind {
						l.log.Errorf("the leader log is behind the replication will stop")
						return
					}
					l.log.Warnf("worker error %v", err)
				}
			case <-l.closer:
				return
			}
		}
	}()
}

// Close stops the replication.
func (l *worker) Close() {
	close(l.closer)
	l.wg.Wait()
}

func (l *worker) do(leaderIndex, clusterID uint64) error {
	replicateRequest := &proto.ReplicateRequest{
		LeaderIndex: leaderIndex + 1,
		Table:       []byte(l.Table),
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.logTimeout)
	defer cancel()
	stream, err := l.logClient.Replicate(ctx, replicateRequest)
	if err != nil {
		return fmt.Errorf("could not open log stream: %w", err)
	}

	if err = l.read(ctx, stream, clusterID); err != nil {
		switch err {
		case ErrUseSnapshot:
			if l.recoverySemaphore.TryAcquire(1) {
				defer l.recoverySemaphore.Release(1)
				return l.recover()
			}
			l.log.Info("maximum number of recoveries of already running, returning table")
			return l.tm.ReturnTable(l.Table)
		case ErrLeaderBehind:
			return ErrLeaderBehind
		default:
			return fmt.Errorf("could not store the log stream: %w", err)
		}
	}
	return nil
}

func (l *worker) tableState() (uint64, uint64, error) {
	t, err := l.tm.GetTable(l.Table)
	if err != nil {
		return 0, 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.logTimeout)
	defer cancel()
	idxRes, err := t.LeaderIndex(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get leader index key: %w", err)
	}
	return idxRes.Index, t.ClusterID, nil
}

// read commands from the stream and save them to the cluster.
func (l *worker) read(ctx context.Context, stream proto.Log_ReplicateClient, clusterID uint64) error {
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
			if err := l.proposeBatch(ctx, res.CommandsResponse.GetCommands(), clusterID); err != nil {
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

func (l *worker) proposeBatch(ctx context.Context, commands []*proto.ReplicateCommand, clusterID uint64) error {
	for _, c := range commands {
		bytes, err := pb.Marshal(c.Command)
		if err != nil {
			return fmt.Errorf("could not marshal command: %w", err)
		}

		if _, err := l.nh.SyncPropose(ctx, l.nh.GetNoOPSession(clusterID), bytes); err != nil {
			return fmt.Errorf("could not SyncPropose: %w", err)
		}
	}
	return nil
}

func (l *worker) recover() error {
	l.log.Info("recovering from snapshot")
	ctx, cancel := context.WithTimeout(context.Background(), l.snapshotTimeout)
	defer cancel()
	stream, err := l.snapshotClient.Stream(ctx, &proto.SnapshotRequest{Table: []byte(l.Table)})
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
	if l.maxSnapshotRecv != 0 {
		r.Bucket = ratelimit.NewBucketWithRate(float64(l.maxSnapshotRecv), int64(l.maxSnapshotRecv)*2)
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
	l.log.Info("snapshot stream saved, loading table")
	err = l.tm.LoadTableFromSnapshot(l.Table, sf)
	if err != nil {
		return err
	}
	l.log.Info("table recovered")
	return nil
}
