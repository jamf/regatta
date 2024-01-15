// Copyright JAMF Software, LLC

package regattaserver

import (
	"io"
	"net"
	"testing"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/google/uuid"
	"github.com/jamf/regatta/storage"
	"github.com/jamf/regatta/storage/table"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type MockTableService struct {
	tables []table.Table
	error  error
}

func (t MockTableService) CreateTable(name string) error {
	return nil
}

func (t MockTableService) DeleteTable(name string) error {
	return nil
}

func (t MockTableService) GetTables() ([]table.Table, error) {
	return t.tables, t.error
}

func (t MockTableService) GetTable(name string) (table.ActiveTable, error) {
	return t.tables[0].AsActive(nil), t.error
}

func (t MockTableService) Restore(name string, reader io.Reader) error {
	return t.error
}

func newInMemTestEngine(t *testing.T, tables ...string) *storage.Engine {
	testAddr := func() string {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		defer l.Close()
		return l.Addr().String()
	}

	raftAddr := testAddr()
	gossipAddr := testAddr()

	e, err := storage.New(storage.Config{
		ClientAddress:  raftAddr,
		NodeID:         1,
		InitialMembers: map[uint64]string{1: raftAddr},
		NodeHostDir:    "/nh",
		RTTMillisecond: 10,
		RaftAddress:    raftAddr,
		EnableMetrics:  false,
		Gossip: storage.GossipConfig{
			ClusterName:    uuid.New().String(),
			BindAddress:    gossipAddr,
			InitialMembers: []string{gossipAddr},
		},
		Table: storage.TableConfig{
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			SnapshotEntries:    10,
			CompactionOverhead: 5,
			MaxInMemLogSize:    1024,
			FS:                 pvfs.NewMem(),
			DataDir:            "/data",
			BlockCacheSize:     1024,
			TableCacheSize:     64,
			RecoveryType:       table.RecoveryTypeCheckpoint,
		},
		Meta: storage.MetaConfig{
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			SnapshotEntries:    10,
			CompactionOverhead: 5,
			MaxInMemLogSize:    1024,
		},
		LogDBImplementation: storage.Tan,
		FS:                  vfs.NewMem(),
		Log:                 zaptest.NewLogger(t).Sugar(),
	})
	require.NoError(t, err)
	require.NoError(t, e.Start())
	require.NoError(t, e.WaitUntilReady())
	for _, tableName := range tables {
		require.NoError(t, e.CreateTable(tableName))
	}
	t.Cleanup(func() {
		_ = e.Close()
	})
	return e
}
