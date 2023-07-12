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
	"github.com/jamf/regatta/proto"
	"github.com/jamf/regatta/regattaserver"
	"github.com/jamf/regatta/replication/snapshot"
	"github.com/jamf/regatta/storage/table"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testReplicationServer struct {
	proto.UnimplementedLogServer
	proto.UnimplementedMetadataServer
	proto.UnimplementedSnapshotServer
	metaResp     *proto.MetadataResponse
	metaErr      error
	repResp      []*proto.ReplicateResponse
	repErr       error
	snapshotFile string
}

func (t testReplicationServer) Get(context.Context, *proto.MetadataRequest) (*proto.MetadataResponse, error) {
	return t.metaResp, t.metaErr
}

func (t testReplicationServer) Replicate(_ *proto.ReplicateRequest, s proto.Log_ReplicateServer) error {
	for _, r := range t.repResp {
		s.Send(r)
	}
	return t.repErr
}

func (t testReplicationServer) Stream(_ *proto.SnapshotRequest, s proto.Snapshot_StreamServer) error {
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
	conn, err := grpc.DialContext(context.Background(), "",
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
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := table.NewManager(followerNH, followerAddresses, tableManagerTestConfig())
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

	conn := testServer(t, func(server *grpc.Server) {
		s := testReplicationServer{metaResp: &proto.MetadataResponse{Tables: []*proto.Table{
			{
				Name: "test",
			},
			{
				Name: "test2",
			},
		}}}
		proto.RegisterMetadataServer(server, s)
		proto.RegisterLogServer(server, s)
	})

	m := NewManager(followerTM, followerNH, conn, Config{
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
	leaderTM, followerTM, leaderNH, followerNH, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create replicator")
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	r.NoError(err)

	m := NewManager(followerTM, followerNH, conn, Config{})

	t.Log("create table")
	r.NoError(leaderTM.CreateTable("test"))
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("create another table")
	r.NoError(leaderTM.CreateTable("test2"))
	r.NoError(m.reconcileTables())
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test2")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("skip network errors")
	r.NoError(conn.Close())

	tabs, err := followerTM.GetTables()
	r.NoError(err)
	r.Len(tabs, 2)
}

func TestWorker_recover(t *testing.T) {
	r := require.New(t)
	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := table.NewManager(followerNH, followerAddresses, tableManagerTestConfig())
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

	conn := testServer(t, func(server *grpc.Server) {
		s := testReplicationServer{
			metaResp: &proto.MetadataResponse{Tables: []*proto.Table{
				{
					Name: "test",
				},
			}},
			repResp: []*proto.ReplicateResponse{
				{
					LeaderIndex: 100,
					Response:    &proto.ReplicateResponse_ErrorResponse{ErrorResponse: &proto.ReplicateErrResponse{Error: proto.ReplicateError_USE_SNAPSHOT}},
				},
			},
			snapshotFile: "snapshot/testdata/snapshot.bin",
		}
		proto.RegisterMetadataServer(server, s)
		proto.RegisterLogServer(server, s)
		proto.RegisterSnapshotServer(server, s)
	})

	m := NewManager(followerTM, followerNH, conn, Config{
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
	m.Start()
	defer m.Close()
	r.Eventually(func() bool {
		return m.factory.nh.HasNodeInfo(10002, 1)
	}, 10*time.Second, 1*time.Second)
}

func startRaftNode() (*dragonboat.NodeHost, map[uint64]string, error) {
	testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 1,
		RaftAddress:    testNodeAddress,
	}
	_ = nhc.Prepare()
	nhc.Expert.FS = vfs.NewMem()
	nhc.Expert.Engine.ExecShards = 1
	nhc.Expert.LogDB.Shards = 1
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, nil, err
	}
	return nh, map[uint64]string{1: testNodeAddress}, nil
}

func getTestPort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func tableManagerTestConfig() table.Config {
	return table.Config{
		NodeID: 1,
		Table:  table.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024, TableCacheSize: 1024},
		Meta:   table.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	}
}

func prepareLeaderAndFollowerRaft(t *testing.T) (leaderTM *table.Manager, followerTM *table.Manager, leaderNH *dragonboat.NodeHost, followerNH *dragonboat.NodeHost, closer func()) {
	r := require.New(t)
	t.Log("start leader Raft")
	leaderNH, leaderAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create leader table manager")
	leaderTM = table.NewManager(leaderNH, leaderAddresses, tableManagerTestConfig())
	r.NoError(leaderTM.Start())
	r.NoError(leaderTM.WaitUntilReady())

	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM = table.NewManager(followerNH, followerAddresses, tableManagerTestConfig())
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

func startReplicationServer(manager *table.Manager, nh *dragonboat.NodeHost) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	proto.RegisterMetadataServer(server, &regattaserver.MetadataServer{Tables: manager})
	proto.RegisterSnapshotServer(server, &regattaserver.SnapshotServer{Tables: manager})
	proto.RegisterLogServer(
		server,
		regattaserver.NewLogServer(
			manager,
			&testLogReader{nh: nh},
			zap.NewNop(),
			1024,
		),
	)
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

type testLogReader struct {
	nh *dragonboat.NodeHost
}

func (t *testLogReader) QueryRaftLog(ctx context.Context, clusterID uint64, logRange dragonboat.LogRange, maxSize uint64) ([]raftpb.Entry, error) {
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
