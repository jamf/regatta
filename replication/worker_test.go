// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Test_worker_do(t *testing.T) {
	r := require.New(t)
	leaderTM, followerTM, leaderNH, followerNH, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create tables")
	_, err := leaderTM.CreateTable("test")
	r.NoError(err)
	_, err = followerTM.CreateTable("test")
	r.NoError(err)

	var at table.ActiveTable
	t.Log("load some data")
	r.Eventually(func() bool {
		at, err = leaderTM.GetTable("test")
		return err == nil
	}, 5*time.Second, 500*time.Millisecond, "table not created in time")

	keyCount := 1000
	r.NoError(fillData(keyCount, at))

	t.Log("create worker")
	conn, err := grpc.Dial(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)
	logger, obs := observer.New(zap.DebugLevel)
	f := &workerFactory{
		logTimeout:    time.Minute,
		tm:            followerTM,
		logClient:     regattapb.NewLogClient(conn),
		nh:            followerNH,
		log:           zap.New(logger).Sugar(),
		pollInterval:  500 * time.Millisecond,
		leaseInterval: 500 * time.Millisecond,
		metrics: struct {
			replicationIndex  *prometheus.GaugeVec
			replicationLeased *prometheus.GaugeVec
		}{
			replicationIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			),
			replicationLeased: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_leased",
					Help: "Regatta replication has the worker table leased",
				}, []string{"table"},
			),
		},
	}
	w := f.create("test")
	idx, id, err := w.tableState()
	r.NoError(err)
	_, err = w.do(idx, id)
	r.NoError(err)
	table, err := followerTM.GetTable("test")
	r.NoError(err)

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := table.Range(ctx, &regattapb.RangeRequest{
			Table:        []byte("test"),
			Key:          []byte{0},
			RangeEnd:     []byte{0},
			Linearizable: true,
			CountOnly:    true,
		})
		r.NoError(err)
		r.Equal(int64(keyCount), response.Count)
	}()

	idxBefore, _, err := w.tableState()
	r.NoError(err)

	t.Log("reset table")
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r.NoError(table.Reset(ctx))
	}()
	idx, id, err = w.tableState()
	r.NoError(err)
	r.Equal(uint64(0), idx)

	t.Log("do after reset")
	_, err = w.do(idx, id)
	r.NoError(err)

	idxAfter, _, err := w.tableState()
	r.NoError(err)
	r.Equal(idxBefore, idxAfter)

	err = leaderTM.DeleteTable("test")
	r.NoError(err)
	t.Log("create empty table test")
	_, err = leaderTM.CreateTable("test")
	r.NoError(err)

	t.Log("load some data")
	r.Eventually(func() bool {
		at, err = leaderTM.GetTable("test")
		return err == nil
	}, 5*time.Second, 500*time.Millisecond, "table not created in time")
	r.NoError(err)

	keyCount = 90
	r.NoError(fillData(keyCount, at))
	w.Start()
	idx, id, err = w.tableState()
	r.NoError(err)
	r.Eventually(func() bool {
		return obs.FilterMessage("the leader log is behind ... backing off").Len() > 0
	}, 5*time.Second, 500*time.Millisecond)

	keyCount = 1000
	r.NoError(fillData(keyCount, at))
	r.Eventually(func() bool {
		idx, id, err = w.tableState()
		r.NoError(err)
		return idx > uint64(200)
	}, 5*time.Second, 500*time.Millisecond)
}

func fillData(keyCount int, at table.ActiveTable) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for i := 0; i < keyCount; i++ {
		if _, err := at.Put(ctx, &regattapb.PutRequest{
			Key:   []byte(fmt.Sprintf("foo-%d", i)),
			Value: []byte("bar"),
		}); err != nil {
			return err
		}
	}
	return nil
}

func Test_worker_recover(t *testing.T) {
	r := require.New(t)
	leaderTM, followerTM, leaderNH, _, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create tables")
	_, err := leaderTM.CreateTable("test")
	r.NoError(err)
	_, err = leaderTM.CreateTable("test2")
	r.NoError(err)

	var at table.ActiveTable
	t.Log("load some data")
	r.Eventually(func() bool {
		var err error
		at, err = leaderTM.GetTable("test")
		return err == nil
	}, 5*time.Second, 500*time.Millisecond, "table not created in time")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = at.Put(ctx, &regattapb.PutRequest{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	})
	r.NoError(err)

	t.Log("create worker")
	conn, err := grpc.Dial(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)
	w := &worker{table: "test", workerFactory: &workerFactory{snapshotTimeout: time.Minute, tm: followerTM, snapshotClient: regattapb.NewSnapshotClient(conn)}, log: zaptest.NewLogger(t).Sugar()}

	t.Log("recover table from leader")
	r.NoError(w.recover())
	tab, err := followerTM.GetTable("test")
	r.NoError(err)
	r.Equal("test", tab.Name)
	ir, err := tab.LeaderIndex(ctx, false)
	r.NoError(err)
	r.Greater(ir.Index, uint64(1))

	w = &worker{table: "test2", workerFactory: &workerFactory{snapshotTimeout: time.Minute, tm: followerTM, snapshotClient: regattapb.NewSnapshotClient(conn)}, log: zaptest.NewLogger(t).Sugar()}
	t.Log("recover second table from leader")
	r.NoError(w.recover())
	tab, err = followerTM.GetTable("test2")
	r.NoError(err)
	r.Equal("test2", tab.Name)
}
