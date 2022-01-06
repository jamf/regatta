package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
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
	r.NoError(leaderTM.CreateTable("test"))
	r.NoError(followerTM.CreateTable("test"))
	time.Sleep(5 * time.Second)

	t.Log("load some data")
	at, err := leaderTM.GetTable("test")
	r.NoError(err)

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for i := 0; i < 50; i++ {
			_, err = at.Put(ctx, &proto.PutRequest{
				Key:   []byte(fmt.Sprintf("foo-%d", i)),
				Value: []byte("bar"),
			})
			r.NoError(err)
		}
	}()

	t.Log("create worker")
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)
	w := &worker{
		Table:      "test",
		logTimeout: time.Minute,
		tm:         followerTM,
		logClient:  proto.NewLogClient(conn),
		nh:         followerNH,
		log:        zaptest.NewLogger(t).Sugar(),
		metrics: struct {
			replicationIndex  prometheus.Gauge
			replicationLeased prometheus.Gauge
		}{
			replicationIndex: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_index",
					Help: "Regatta replication index",
				}, []string{"role", "table"},
			).WithLabelValues("follower", "test"),
			replicationLeased: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "regatta_replication_leased",
					Help: "Regatta replication has the worker table leased",
				}, []string{"table"},
			).WithLabelValues("test"),
		},
	}
	idx, id, err := w.tableState()
	r.NoError(err)
	r.NoError(w.do(idx, id))
	table, err := followerTM.GetTable("test")
	r.NoError(err)

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := table.Range(ctx, &proto.RangeRequest{
			Table:        []byte("test"),
			Key:          []byte{0},
			RangeEnd:     []byte{0},
			Linearizable: true,
			CountOnly:    true,
		})
		r.NoError(err)
		r.Equal(int64(50), response.Count)
	}()

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
	r.NoError(w.do(idx, id))

	idx, _, err = w.tableState()
	r.NoError(err)
	r.Greater(idx, uint64(0))
}

func Test_worker_recover(t *testing.T) {
	r := require.New(t)
	leaderTM, followerTM, leaderNH, _, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create tables")
	r.NoError(leaderTM.CreateTable("test"))
	r.NoError(leaderTM.CreateTable("test2"))
	time.Sleep(5 * time.Second)

	t.Log("load some data")
	at, err := leaderTM.GetTable("test")
	r.NoError(err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = at.Put(ctx, &proto.PutRequest{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	})
	r.NoError(err)

	t.Log("create worker")
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)
	w := &worker{Table: "test", snapshotTimeout: time.Minute, tm: followerTM, snapshotClient: proto.NewSnapshotClient(conn), log: zaptest.NewLogger(t).Sugar()}

	t.Log("recover table from leader")
	r.NoError(w.recover())
	tab, err := followerTM.GetTable("test")
	r.NoError(err)
	r.Equal("test", tab.Name)
	ir, err := tab.LeaderIndex(ctx)
	r.NoError(err)
	r.Greater(ir.Index, uint64(1))

	w = &worker{Table: "test2", snapshotTimeout: time.Minute, tm: followerTM, snapshotClient: proto.NewSnapshotClient(conn), log: zaptest.NewLogger(t).Sugar()}
	t.Log("recover second table from leader")
	r.NoError(w.recover())
	tab, err = followerTM.GetTable("test2")
	r.NoError(err)
	r.Equal("test2", tab.Name)
}

func prepareLeaderAndFollowerRaft(t *testing.T) (leaderTM *tables.Manager, followerTM *tables.Manager, leaderNH *dragonboat.NodeHost, followerNH *dragonboat.NodeHost, closer func()) {
	r := require.New(t)
	t.Log("start leader Raft")
	leaderNH, leaderAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create leader table manager")
	leaderTM = tables.NewManager(leaderNH, leaderAddresses, tableManagerTestConfig())
	r.NoError(leaderTM.Start())
	r.NoError(leaderTM.WaitUntilReady())

	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM = tables.NewManager(followerNH, followerAddresses, tableManagerTestConfig())
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())

	closer = func() {
		leaderTM.Close()
		leaderNH.Close()
		followerTM.Close()
		followerNH.Close()
	}
	return
}

func startReplicationServer(manager *tables.Manager, nh *dragonboat.NodeHost) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	db, err := nh.GetReadOnlyLogDB()
	if err != nil {
		panic(err)
	}
	proto.RegisterMetadataServer(server, &regattaserver.MetadataServer{Tables: manager})
	proto.RegisterSnapshotServer(server, &regattaserver.SnapshotServer{Tables: manager})
	proto.RegisterLogServer(server, &regattaserver.LogServer{Tables: manager, NodeID: manager.NodeID(), Log: zap.NewNop().Sugar(), DB: db})
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	// Let the server start.
	time.Sleep(100 * time.Millisecond)
	return server
}
