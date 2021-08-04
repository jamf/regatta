package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
)

func Test_worker_recover(t *testing.T) {
	r := require.New(t)
	t.Log("start leader Raft")
	leaderNH, leaderAddresses, err := startRaftNode()
	r.NoError(err)
	defer leaderNH.Close()

	t.Log("create leader table manager")
	leaderTM := tables.NewManager(leaderNH, leaderAddresses, tableManagerTestConfig)
	r.NoError(leaderTM.Start())
	r.NoError(leaderTM.WaitUntilReady())
	defer leaderTM.Close()
	t.Log("start the metadata server")
	metaSrv := startSnapshotServer(leaderTM)
	defer metaSrv.Shutdown()

	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := tables.NewManager(followerNH, followerAddresses, tableManagerTestConfig)
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

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
	conn, err := grpc.Dial(metaSrv.Addr, grpc.WithInsecure())
	r.NoError(err)
	w := &worker{tm: followerTM, snapshotClient: proto.NewSnapshotClient(conn), log: zaptest.NewLogger(t).Sugar()}

	t.Log("recover table from leader")
	r.NoError(w.recover(context.Background(), "test", 30*time.Second))
	tab, err := followerTM.GetTable("test")
	r.NoError(err)
	r.Equal("test", tab.Name)

	t.Log("recover second table from leader")
	r.NoError(w.recover(context.Background(), "test2", 30*time.Second))
	tab, err = followerTM.GetTable("test2")
	r.NoError(err)
	r.Equal("test2", tab.Name)
}
