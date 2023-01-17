// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/replication/snapshot"
	serror "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/tables"
	"github.com/lni/dragonboat/v4"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO make configurable.
const desiredProposalSize = 256 * 1024

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
			replicationLeaderIndex   prometheus.Gauge
			replicationFollowerIndex prometheus.Gauge
			replicationLeased        prometheus.Gauge
		}{
			replicationLeaderIndex:   f.metrics.replicationIndex.WithLabelValues("leader", table),
			replicationFollowerIndex: f.metrics.replicationIndex.WithLabelValues("follower", table),
			replicationLeased:        f.metrics.replicationLeased.WithLabelValues(table),
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
		replicationLeaderIndex   prometheus.Gauge
		replicationFollowerIndex prometheus.Gauge
		replicationLeased        prometheus.Gauge
	}
	wg sync.WaitGroup
}

// Start launches the replication goroutine. To stop it, call worker.Close.
func (w *worker) Start() {
	// Sleep up to reconcile interval to prevent the thundering herd
	// #nosec G404 -- Weak random number generator can be used because we do not care whether the result can be predicted.
	time.Sleep(time.Duration(rand.Intn(int(w.pollInterval.Milliseconds()))) * time.Millisecond)

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
				idx, clusterID, err := w.tableState()
				if err != nil {
					w.log.Errorf("cannot query leader index: %v", err)
					continue
				}
				w.metrics.replicationFollowerIndex.Set(float64(idx))
				if atomic.LoadUint32(&w.leased) != 1 {
					w.log.Debug("skipping replication - table not leased")
					continue
				}

				if err := w.do(idx, clusterID); err != nil {
					switch err {
					case serror.ErrLogBehind:
						w.log.Errorf("the leader log is behind ... backing off")
					case serror.ErrLogAhead:
						if w.recoverySemaphore.TryAcquire(1) {
							func() {
								defer w.recoverySemaphore.Release(1)
								if err := w.recover(); err != nil {
									w.log.Warnf("error in recovering table: %v", err)
								}
							}()
						} else {
							w.log.Info("maximum number of recoveries already running")
							if _, err := w.tm.ReturnTable(w.Table); err != nil {
								w.log.Warnf("error retruning table: %v", err)
							}
						}
					default:
						w.log.Warnf("worker error: %v", err)
					}
					continue
				}
				t.Reset(w.pollInterval)
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
	} else {
		w.log.Error("table was not returned")
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

	for {
		replicateRes, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if e, ok := status.FromError(err); ok && e.Code() == codes.NotFound {
			return serror.ErrTableNotFound
		} else if err != nil {
			return fmt.Errorf("error reading replication stream: %w", err)
		}

		if replicateRes.LeaderIndex != 0 {
			w.metrics.replicationLeaderIndex.Set(float64(replicateRes.LeaderIndex))
		}

		switch res := replicateRes.Response.(type) {
		case *proto.ReplicateResponse_CommandsResponse:
			if err := w.proposeBatch(ctx, res.CommandsResponse.GetCommands(), clusterID); err != nil {
				return fmt.Errorf("could not propose: %w", err)
			}
		case *proto.ReplicateResponse_ErrorResponse:
			switch res.ErrorResponse.Error {
			case proto.ReplicateError_LEADER_BEHIND:
				return serror.ErrLogBehind
			case proto.ReplicateError_USE_SNAPSHOT:
				return serror.ErrLogAhead
			default:
				return fmt.Errorf(
					"unknown replicate error response '%s' with id %d",
					res.ErrorResponse.Error.String(),
					res.ErrorResponse.Error,
				)
			}
		}
	}
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

func (w *worker) proposeBatch(ctx context.Context, commands []*proto.ReplicateCommand, clusterID uint64) error {
	seq := proto.CommandFromVTPool()
	defer seq.ReturnToVTPool()
	session := w.nh.GetNoOPSession(clusterID)

	var buff []byte
	propose := func() error {
		defer func() {
			seq.Sequence = seq.Sequence[:0]
			seq.LeaderIndex = nil
		}()
		size := seq.SizeVT()
		if cap(buff) < size {
			buff = make([]byte, 0, size)
		}
		buff = buff[:size]
		n, err := seq.MarshalToSizedBufferVT(buff)
		if err != nil {
			return fmt.Errorf("could not marshal command: %w", err)
		}

		if _, err := w.nh.SyncPropose(ctx, session, buff[:n]); err != nil {
			return fmt.Errorf("could not SyncPropose: %w", err)
		}
		w.metrics.replicationFollowerIndex.Set(float64(*seq.LeaderIndex))
		return nil
	}

	seq.Type = proto.Command_SEQUENCE
	for i, c := range commands {
		seq.Sequence = append(seq.Sequence, c.Command)
		seq.LeaderIndex = &c.LeaderIndex
		if seq.SizeVT() >= desiredProposalSize || i == len(commands)-1 {
			err := propose()
			if err != nil {
				return err
			}
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
		r.Limiter = rate.NewLimiter(rate.Limit(w.maxSnapshotRecv), int(w.maxSnapshotRecv))
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
	err = w.tm.Restore(w.Table, sf)
	if err != nil {
		return err
	}
	w.log.Info("table recovered")
	return nil
}
