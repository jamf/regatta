package replication

import (
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"google.golang.org/grpc"
)

var tableManagerTestConfig = func() tables.Config {
	return tables.Config{
		NodeID: 1,
		Table:  tables.TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), MaxInMemLogSize: 1024 * 1024, BlockCacheSize: 1024},
		Meta:   tables.MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	}
}

func TestMetadata_Replicate(t *testing.T) {
	r := require.New(t)
	leaderTM, followerTM, leaderNH, _, closer := prepareLeaderAndFollowerRaft(t)
	defer closer()
	srv := startReplicationServer(leaderTM, leaderNH)
	defer srv.Shutdown()

	t.Log("create replicator")
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
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
