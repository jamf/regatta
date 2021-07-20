package replication

import (
	"fmt"
	"net"
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"google.golang.org/grpc"
)

var tableManagerTestConfig = tables.Config{
	NodeID: 1,
	Table:  tables.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem()},
	Meta:   tables.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
}

func TestMetadata_Replicate(t *testing.T) {
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
	metaSrv := startMetadataServer(leaderTM)
	defer metaSrv.Shutdown()

	t.Log("start follower Raft")
	followerNH, followerAddresses, err := startRaftNode()
	r.NoError(err)

	t.Log("create follower table manager")
	followerTM := tables.NewManager(followerNH, followerAddresses, tableManagerTestConfig)
	r.NoError(followerTM.Start())
	r.NoError(followerTM.WaitUntilReady())
	defer followerTM.Close()

	t.Log("create replicator")
	conn, err := grpc.Dial(metaSrv.Addr, grpc.WithInsecure())
	r.NoError(err)
	metar := NewMetadata(proto.NewMetadataClient(conn), followerTM)
	metar.Interval = 250 * time.Millisecond

	t.Log("start the replicator")
	metar.Replicate()
	defer metar.Close()

	t.Log("create table")
	r.NoError(leaderTM.CreateTable("test"))
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("create another table")
	r.NoError(leaderTM.CreateTable("test2"))
	r.Eventually(func() bool {
		_, err := followerTM.GetTable("test2")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "table not created in time")

	t.Log("skip network errors")
	r.NoError(conn.Close())
	time.Sleep(3 * metar.Interval)

	tabs, err := followerTM.GetTables()
	r.NoError(err)
	r.Len(tabs, 2)
}

func startMetadataServer(manager *tables.Manager) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	proto.RegisterMetadataServer(server, &regattaserver.MetadataServer{Tables: manager})
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	return server
}

func startRaftNode() (*dragonboat.NodeHost, map[uint64]string, error) {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
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
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
