// Copyright JAMF Software, LLC

package replication

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftpb"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/replication/snapshot"
	"github.com/jamf/regatta/storage"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testReplicationServer struct {
	regattapb.UnimplementedLogServer
	regattapb.UnimplementedMetadataServer
	regattapb.UnimplementedSnapshotServer
	metaResp     *regattapb.MetadataResponse
	metaErr      error
	repResp      []*regattapb.ReplicateResponse
	repErr       error
	snapshotFile string
}

func (t testReplicationServer) Get(context.Context, *regattapb.MetadataRequest) (*regattapb.MetadataResponse, error) {
	return t.metaResp, t.metaErr
}

func (t testReplicationServer) Replicate(_ *regattapb.ReplicateRequest, s regattapb.Log_ReplicateServer) error {
	for _, r := range t.repResp {
		s.Send(r)
	}
	return t.repErr
}

func (t testReplicationServer) Stream(_ *regattapb.SnapshotRequest, s regattapb.Snapshot_StreamServer) error {
	f, err := os.Open(t.snapshotFile)
	if err != nil {
		return err
	}
	_, err = io.Copy(&snapshot.Writer{Sender: s}, f)
	return err
}

func testServer(t *testing.T, regf func(server *grpc.Server)) *grpc.ClientConn {
	lis := bufconn.Listen(10 * 1024 * 1024)
	srv := grpc.NewServer()
	regf(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(":0",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return conn
}

func TestManager_reconcile(t *testing.T) {
	r := require.New(t)
	t.Log("start follower Raft")
	_, followerEngine := prepareLeaderAndFollowerEngine(t)

	conn := testServer(t, func(server *grpc.Server) {
		s := testReplicationServer{metaResp: &regattapb.MetadataResponse{Tables: []*regattapb.Table{
			{
				Name: "test",
			},
			{
				Name: "test2",
			},
		}}}
		regattapb.RegisterMetadataServer(server, s)
		regattapb.RegisterLogServer(server, s)
	})

	queue := storage.NewNotificationQueue()
	go queue.Run()
	m := NewManager(followerEngine, queue, conn, Config{
		ReconcileInterval: 250 * time.Millisecond,
		Workers: WorkerConfig{
			PollInterval:        10 * time.Millisecond,
			LeaseInterval:       100 * time.Millisecond,
			LogRPCTimeout:       100 * time.Millisecond,
			SnapshotRPCTimeout:  100 * time.Millisecond,
			MaxRecoveryInFlight: 1,
			MaxSnapshotRecv:     0,
		},
	})
	m.Start()
	r.Eventually(func() bool {
		return m.hasWorker("test")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")
	r.Eventually(func() bool {
		return m.hasWorker("test2")
	}, 10*time.Second, 250*time.Millisecond, "replication worker not found in registry")
	m.Close()
	r.Empty(m.workers.registry)
}

func TestManager_reconcileTables(t *testing.T) {
	r := require.New(t)
	leaderEngine, followerEngine := prepareLeaderAndFollowerEngine(t)
	srv := startReplicationServer(leaderEngine)
	defer srv.Shutdown()

	t.Log("create replicator")
	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)

	m := NewManager(followerEngine, nil, conn, Config{})

	t.Log("create table")
	_, err = leaderEngine.CreateTable("test")
	r.NoError(err)
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerEngine.GetTable("test")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("create another table")
	_, err = leaderEngine.CreateTable("test2")
	r.NoError(err)
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerEngine.GetTable("test2")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("skip network errors")
	r.NoError(conn.Close())

	tabs, err := followerEngine.GetTables()
	r.NoError(err)
	r.Len(tabs, 2)
}

func TestManager_recover(t *testing.T) {
	r := require.New(t)
	t.Log("start follower Raft")
	_, followerEngine := prepareLeaderAndFollowerEngine(t)

	conn := testServer(t, func(server *grpc.Server) {
		s := testReplicationServer{
			metaResp: &regattapb.MetadataResponse{Tables: []*regattapb.Table{
				{
					Name: "test",
				},
			}},
			repResp: []*regattapb.ReplicateResponse{
				{
					LeaderIndex: 100,
					Response:    &regattapb.ReplicateResponse_ErrorResponse{ErrorResponse: &regattapb.ReplicateErrResponse{Error: regattapb.ReplicateError_USE_SNAPSHOT}},
				},
			},
			snapshotFile: "snapshot/testdata/snapshot.bin",
		}
		regattapb.RegisterMetadataServer(server, s)
		regattapb.RegisterLogServer(server, s)
		regattapb.RegisterSnapshotServer(server, s)
	})

	queue := storage.NewNotificationQueue()
	go queue.Run()
	m := NewManager(followerEngine, queue, conn, Config{
		ReconcileInterval: 250 * time.Millisecond,
		Workers: WorkerConfig{
			PollInterval:        10 * time.Millisecond,
			LeaseInterval:       100 * time.Millisecond,
			LogRPCTimeout:       100 * time.Millisecond,
			SnapshotRPCTimeout:  10 * time.Second,
			MaxRecoveryInFlight: 1,
			MaxSnapshotRecv:     512,
		},
	})
	r.NoError(m.Start())
	defer m.Close()
	r.Eventually(func() bool {
		return m.factory.engine.HasNodeInfo(10002, 1)
	}, 10*time.Second, 1*time.Second)
}

func getTestPort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func prepareLeaderAndFollowerEngine(t *testing.T) (leaderTM *storage.Engine, followerTM *storage.Engine) {
	t.Helper()
	r := require.New(t)
	t.Log("start leader Raft")
	leaderAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	leaderTM, err := storage.New(storage.Config{
		FS:             vfs.NewMem(),
		Log:            zaptest.NewLogger(t).Sugar(),
		InitialMembers: map[uint64]string{1: leaderAddress},
		Gossip: storage.GossipConfig{
			BindAddress: fmt.Sprintf("127.0.0.1:%d", getTestPort()),
			ClusterName: "leader",
		},
		NodeID:         1,
		RTTMillisecond: 5,
		RaftAddress:    leaderAddress,
		Table:          storage.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024, TableCacheSize: 1024},
		Meta:           storage.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	})
	r.NoError(err)
	r.NoError(leaderTM.Start())

	t.Log("start follower Raft")
	followerAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	followerTM, err = storage.New(storage.Config{
		FS:             vfs.NewMem(),
		Log:            zaptest.NewLogger(t).Sugar(),
		InitialMembers: map[uint64]string{1: followerAddress},
		Gossip: storage.GossipConfig{
			BindAddress: fmt.Sprintf("127.0.0.1:%d", getTestPort()),
			ClusterName: "follower",
		},
		NodeID:         1,
		RTTMillisecond: 5,
		RaftAddress:    followerAddress,
		Table:          storage.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024, TableCacheSize: 1024},
		Meta:           storage.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	})
	r.NoError(err)
	r.NoError(followerTM.Start())

	t.Cleanup(func() {
		_ = leaderTM.Close()
		_ = followerTM.Close()
	})
	return
}

func startReplicationServer(engine *storage.Engine) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	l, _ := net.Listen("tcp", testNodeAddress)
	server := regattaserver.NewServer(l, zap.NewNop().Sugar())
	regattapb.RegisterMetadataServer(server, &regattaserver.MetadataServer{Tables: engine})
	regattapb.RegisterSnapshotServer(server, &regattaserver.SnapshotServer{Tables: engine})
	regattapb.RegisterLogServer(
		server,
		regattaserver.NewLogServer(
			engine,
			&testLogReader{nh: engine.NodeHost},
			zap.NewNop(),
			1024,
		),
	)
	go func() {
		err := server.Serve()
		if err != nil {
			panic(err)
		}
	}()
	// Let the server start.
	time.Sleep(100 * time.Millisecond)
	return server
}

type testLogReader struct {
	nh *raft.NodeHost
}

func (t *testLogReader) QueryRaftLog(ctx context.Context, clusterID uint64, logRange raft.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
	// Empty log range should return immediately.
	if logRange.FirstIndex == logRange.LastIndex {
		return nil, nil
	}
	rs, err := t.nh.QueryRaftLog(clusterID, logRange.FirstIndex, logRange.LastIndex, maxSize)
	if err != nil {
		return nil, err
	}
	defer rs.Release()
	result := <-rs.ResultC()
	ent, _ := result.RaftLogs()
	return ent, nil
}
