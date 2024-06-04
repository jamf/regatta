// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jamf/regatta/raft/client"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/replication/snapshot"
	"github.com/jamf/regatta/storage"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/kv"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// TODO make configurable.
	desiredProposalSize   = 256 * 1024
	queueLenCheckInterval = 50 * time.Millisecond
	metricsCheckInterval  = 5 * time.Second
)

type replicateResult int

const (
	resultUnknown replicateResult = iota
	resultLeaderBehind
	resultLeaderAhead
	resultFollowerLagging
	resultFollowerTailing
	resultTableNotExists
	resultBackoff
)

type workerFactory struct {
	reconcileInterval time.Duration
	pollInterval      time.Duration
	leaseInterval     time.Duration
	logTimeout        time.Duration
	snapshotTimeout   time.Duration
	maxSnapshotRecv   uint64
	recoverySemaphore *semaphore.Weighted
	log               *zap.SugaredLogger
	store             replicationManagerStore
	engine            *storage.Engine
	queue             *storage.IndexNotificationQueue
	logClient         regattapb.LogClient
	snapshotClient    regattapb.SnapshotClient
	metrics           struct {
		replicationIndex  *prometheus.GaugeVec
		replicationLeased *prometheus.GaugeVec
	}
}

type tableQueue struct {
	table string
	queue *storage.IndexNotificationQueue
}

func (q tableQueue) Len() int {
	if q.queue == nil {
		return 0
	}
	return q.queue.Len(q.table)
}

type tableQueueLenStore struct {
	clock clock.Clock
	table string
	store replicationManagerStore
	id    uint64
}

func (tql tableQueueLenStore) Max() (uint64, error) {
	key := fmt.Sprintf("queue/%s/*", tql.table)
	p, err := tql.store.GetAllValues(key)
	if err != nil {
		return 0, err
	}
	var m uint64
	for _, s := range p {
		sep := strings.Split(s, "$")
		t, err := strconv.ParseInt(sep[0], 10, 64)
		if err != nil {
			return 0, err
		}
		pt := time.UnixMilli(t)
		if tql.clock.Since(pt) >= 30*time.Second {
			return 0, nil
		}
		v, err := strconv.ParseUint(sep[1], 10, 64)
		if err != nil {
			return 0, err
		}
		m = max(m, v)
	}
	return m, nil
}

func (tql tableQueueLenStore) Set(i uint64) error {
	key := fmt.Sprintf("queue/%s/%d", tql.table, tql.id)
	p, err := tql.store.Get(key)
	if err != nil && !errors.Is(err, kv.ErrNotExist) {
		return err
	}
	_, err = tql.store.Set(key, fmt.Sprintf("%d$%d", tql.clock.Now().UnixMilli(), i), p.Ver)
	return err
}

func (tql tableQueueLenStore) Get() (uint64, error) {
	key := fmt.Sprintf("queue/%s/%d", tql.table, tql.id)
	p, err := tql.store.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	sep := strings.Split(p.Value, "$")
	ct, err := strconv.ParseInt(sep[0], 10, 64)
	if err != nil {
		return 0, err
	}
	pt := time.UnixMilli(ct)
	if tql.clock.Since(pt) >= 30*time.Second {
		return 0, nil
	}
	v, err := strconv.ParseUint(sep[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (f *workerFactory) create(table string) *worker {
	return &worker{
		workerFactory: f,
		table:         table,
		closer:        make(chan struct{}),
		leaseChan:     make(chan bool),
		log:           f.log.Named(table),
		queue:         tableQueue{table: table, queue: f.queue},
		store:         tableQueueLenStore{table: table, store: f.store, id: f.engine.Config().NodeID, clock: clock.New()},
		throttle:      newThrottle(f.pollInterval),
		immediate:     make(chan time.Time, 1),
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

type replicationThrottle struct {
	intervals [5]time.Duration
	speed     int
}

func (t *replicationThrottle) current() time.Duration {
	return t.intervals[t.speed]
}

func (t *replicationThrottle) up() {
	t.speed = min(t.speed+1, len(t.intervals)-1)
}

func (t *replicationThrottle) down() {
	t.speed = max(t.speed-1, 0)
}

func newThrottle(pollInterval time.Duration) replicationThrottle {
	return replicationThrottle{intervals: [5]time.Duration{pollInterval, pollInterval / 2, pollInterval / 4, pollInterval / 16, pollInterval / 256}}
}

// worker connects to the log replication service and synchronizes the local state.
type worker struct {
	*workerFactory
	table     string
	closer    chan struct{}
	log       *zap.SugaredLogger
	queue     tableQueue
	store     tableQueueLenStore
	throttle  replicationThrottle
	leased    atomic.Bool
	leaseChan chan bool
	metrics   struct {
		replicationLeaderIndex   prometheus.Gauge
		replicationFollowerIndex prometheus.Gauge
		replicationLeased        prometheus.Gauge
	}
	wg        sync.WaitGroup
	immediate chan time.Time
}

// Start launches the replication goroutine. To stop it, call worker.Close.
func (w *worker) Start() {
	// Sleep up to reconcile interval to prevent the thundering herd
	// #nosec G404 -- Weak random number generator can be used because we do not care whether the result can be predicted.
	time.Sleep(time.Duration(rand.IntN(int(w.pollInterval.Milliseconds()))) * time.Millisecond)

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
				err := w.engine.LeaseTable(w.table, w.leaseInterval*4)
				if err == nil {
					prev := w.leased.Swap(true)
					if !prev {
						w.metrics.replicationLeased.Set(1)
					}
				} else {
					prev := w.leased.Swap(false)
					if prev {
						w.metrics.replicationLeased.Set(0)
					}
				}
			case <-w.closer:
				return
			}
		}
	}()

	w.wg.Add(1)
	go func() {
		defer func() {
			w.log.Info("stats routine stopped")
			w.wg.Done()
		}()

		w.log.Info("stats routine started")
		tq := time.NewTicker(queueLenCheckInterval)
		defer tq.Stop()
		tidx := time.NewTicker(metricsCheckInterval)
		defer tidx.Stop()
		for {
			select {
			case <-tidx.C:
				if idx, _, err := w.tableState(); err == nil {
					w.metrics.replicationFollowerIndex.Set(float64(idx))
				}
			case <-tq.C:
				if v, _ := w.store.Get(); v != uint64(w.queue.Len()) {
					err := w.store.Set(uint64(w.queue.Len()))
					if err != nil {
						w.log.Errorf("unable to store current replication queue len: %v", err)
					}
				}
				if m, _ := w.store.Max(); m > 0 {
					select {
					case w.immediate <- time.Now():
					default:
					}
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
			case <-w.immediate:
			case <-w.closer:
				return
			}

			if !w.leased.Load() {
				w.log.Debug("skipping replication - table not leased")
				continue
			}
			t.Reset(w.throttle.current())
			if u, _ := w.store.Max(); u > 0 {
				w.throttle.up()
			} else {
				w.throttle.down()
			}
			idx, id, err := w.tableState()
			if err != nil {
				if errors.Is(err, serrors.ErrTableNotFound) {
					w.log.Debugf("table not found: %v", err)
					continue
				}
				w.log.Errorf("cannot query leader index: %v", err)
				continue
			}
			result, err := w.do(idx, w.engine.GetNoOPSession(id))
			switch result {
			case resultTableNotExists:
				w.log.Infof("the leader table disappeared ... backing off")
				// Give reconciler time to clean up the table.
				<-t.C
				t.Reset(2 * w.reconcileInterval)
			case resultLeaderBehind:
				w.log.Errorf("the leader log is behind ... backing off")
				// Give leader time to catch up.
				<-t.C
				t.Reset(10 * w.pollInterval)
			case resultBackoff:
				w.log.Infof("the leader asked for backoff ... backing off")
				// Give leader time to catch up.
				<-t.C
				t.Reset(10 * w.pollInterval)
			case resultLeaderAhead:
				if w.recoverySemaphore.TryAcquire(1) {
					func() {
						defer w.recoverySemaphore.Release(1)
						if err := w.recover(); err != nil {
							w.log.Warnf("error in recovering table: %v", err)
						}
					}()
				} else {
					w.log.Info("maximum number of recoveries already running")
					if _, err := w.engine.ReturnTable(w.table); err != nil {
						w.log.Warnf("error returning table: %v", err)
					}
				}
			case resultFollowerLagging:
				// Burst when lagging behind the leader.
				w.throttle.up()
			case resultFollowerTailing:
			case resultUnknown:
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						w.log.Warnf("unable to read leader log in time: %v", err)
					} else {
						w.log.Warnf("unknown worker error: %v", err)
					}
				}
			}
		}
	}()
}

// Close stops the replication.
func (w *worker) Close() {
	w.log.Info("worker stopped")
	close(w.closer)
	w.wg.Wait()

	ok, err := w.engine.ReturnTable(w.table)
	if err != nil {
		w.log.Errorf("returning table failed %v", err)
	}
	if ok {
		w.log.Info("table returned")
	}
}

func (w *worker) do(leaderIndex uint64, session *client.Session) (replicateResult, error) {
	replicateRequest := &regattapb.ReplicateRequest{
		LeaderIndex: leaderIndex + 1,
		Table:       []byte(w.table),
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.logTimeout)
	defer cancel()
	stream, err := w.logClient.Replicate(ctx, replicateRequest, grpc.WaitForReady(true))
	if err != nil {
		if c, ok := status.FromError(err); ok && c.Code() == codes.Unavailable {
			return resultTableNotExists, fmt.Errorf("could not open log stream: %w", err)
		}
		return resultUnknown, fmt.Errorf("could not open log stream: %w", err)
	}
	var applied uint64
	for {
		replicateRes, err := stream.Recv()
		if err == io.EOF {
			return resultUnknown, nil
		}
		if err != nil {
			if c, ok := status.FromError(err); ok && c.Code() == codes.NotFound {
				return resultTableNotExists, fmt.Errorf("error reading replication stream: %w", c.Err())
			}
			if c, ok := status.FromError(err); ok && c.Code() == codes.FailedPrecondition {
				return resultBackoff, fmt.Errorf("error reading replication stream: %w", c.Err())
			}
			return resultUnknown, fmt.Errorf("error reading replication stream: %w", err)
		}

		if replicateRes.LeaderIndex != 0 {
			w.metrics.replicationLeaderIndex.Set(float64(replicateRes.LeaderIndex))
		}

		switch res := replicateRes.Response.(type) {
		case *regattapb.ReplicateResponse_CommandsResponse:
			applied, err = w.proposeBatch(ctx, res.CommandsResponse.GetCommands(), session)
			if err != nil {
				return resultUnknown, fmt.Errorf("could not propose: %w", err)
			}
		case *regattapb.ReplicateResponse_ErrorResponse:
			switch res.ErrorResponse.Error {
			case regattapb.ReplicateError_LEADER_BEHIND:
				return resultLeaderBehind, nil
			case regattapb.ReplicateError_USE_SNAPSHOT:
				return resultLeaderAhead, nil
			default:
				return resultUnknown, fmt.Errorf(
					"unknown replicate error response '%s' with id %d",
					res.ErrorResponse.Error.String(),
					res.ErrorResponse.Error,
				)
			}
		default:
			if applied != 0 && applied < replicateRes.LeaderIndex {
				return resultFollowerLagging, nil
			}
			return resultFollowerTailing, nil
		}
	}
}

func (w *worker) tableState() (uint64, uint64, error) {
	t, err := w.engine.GetTable(w.table)
	if err != nil {
		return 0, 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.logTimeout)
	defer cancel()
	idxRes, err := t.LeaderIndex(ctx, false)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get leader index key: %w", err)
	}
	return idxRes.Index, t.ClusterID, nil
}

func (w *worker) proposeBatch(ctx context.Context, commands []*regattapb.ReplicateCommand, session *client.Session) (uint64, error) {
	seq := regattapb.CommandFromVTPool()
	defer seq.ReturnToVTPool()
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
		if _, err := w.engine.SyncPropose(ctx, session, buff[:n]); err != nil {
			return fmt.Errorf("could not propose sequence: %w", err)
		}
		w.metrics.replicationFollowerIndex.Set(float64(*seq.LeaderIndex))
		return nil
	}

	var lastApplied uint64
	seq.Type = regattapb.Command_SEQUENCE
	for i, c := range commands {
		seq.Sequence = append(seq.Sequence, c.Command)
		seq.LeaderIndex = &c.LeaderIndex
		if seq.SizeVT() >= desiredProposalSize || i == len(commands)-1 {
			if err := propose(); err != nil {
				return lastApplied, err
			}
			lastApplied = c.LeaderIndex
		}
	}

	return lastApplied, nil
}

func (w *worker) recover() error {
	w.log.Info("recovering from snapshot")
	ctx, cancel := context.WithTimeout(context.Background(), w.snapshotTimeout)
	defer cancel()
	stream, err := w.snapshotClient.Stream(ctx, &regattapb.SnapshotRequest{Table: []byte(w.table)})
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
	err = w.engine.Restore(w.table, sf)
	if err != nil {
		return err
	}
	w.log.Info("table recovered")
	return nil
}
