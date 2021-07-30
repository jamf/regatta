package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/regattaserver"
	"github.com/wandera/regatta/storage/tables"
	"google.golang.org/grpc"
	pb "google.golang.org/protobuf/proto"
)

func TestSnapshot_Recover(t *testing.T) {
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

	conn, err := grpc.Dial(metaSrv.Addr, grpc.WithInsecure())
	r.NoError(err)
	snap := NewSnapshot(proto.NewSnapshotClient(conn), followerTM)

	t.Log("recover table from leader")
	r.NoError(snap.Recover(context.Background(), "test"))
	tab, err := followerTM.GetTable("test")
	r.NoError(err)
	r.Equal("test", tab.Name)

	t.Log("recover second table from leader")
	r.NoError(snap.Recover(context.Background(), "test2"))
	tab, err = followerTM.GetTable("test2")
	r.NoError(err)
	r.Equal("test2", tab.Name)
}

func Test_snapshotFile_Read(t *testing.T) {
	r := require.New(t)
	sf := snapshotFile{}
	open, err := os.Open("testdata/snapshot.bin")
	r.NoError(err)
	sf.File = open
	defer func() {
		_ = sf.Close()
	}()

	buff := make([]byte, 1024)
	for {
		n, err := sf.Read(buff)
		if n > 0 {
			r.NoError(err)
			r.NoError(pb.Unmarshal(buff[:n], &proto.Command{}))
		} else {
			r.Equal(io.EOF, err)
			break
		}
	}
}

func Test_snapshotFile_Write(t *testing.T) {
	r := require.New(t)
	sf, err := newSnapshotFile(t.TempDir(), snapshotFilenamePattern)
	r.NoError(err)
	defer func() {
		_ = sf.Close()
	}()

	bts, _ := pb.Marshal(&proto.Command{Type: proto.Command_PUT, Kv: &proto.KeyValue{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	}})
	n, err := sf.Write(bts)
	r.NoError(err)
	r.NoError(sf.Sync())
	r.Equal(len(bts)+8, n)
}

func startSnapshotServer(manager *tables.Manager) *regattaserver.RegattaServer {
	testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
	server := regattaserver.NewServer(testNodeAddress, false)
	proto.RegisterSnapshotServer(server, &regattaserver.SnapshotServer{Tables: manager})
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	return server
}
