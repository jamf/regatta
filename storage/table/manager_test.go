// Copyright JAMF Software, LLC

package table

import (
	"net"
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/config"
	"github.com/jamf/regatta/replication/snapshot"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/kv"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

var minimalTestConfig = func() Config {
	return Config{
		NodeID: 1,
		Table:  TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), BlockCacheSize: 1024, TableCacheSize: 1024},
		Meta:   MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	}
}

func TestManager_CreateTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode(t)
	defer node.Close()

	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.Start()
	defer tm.Close()

	t.Log("create table")
	_, err := tm.CreateTable(testTableName)
	r.NoError(err)

	t.Log("get table")
	tab, err := tm.GetTable(testTableName)
	r.NoError(err)
	r.Equal(testTableName, tab.Name)
	r.Greater(tab.ClusterID, tableIDsRangeStart)

	t.Log("create existing table")
	_, err = tm.CreateTable(testTableName)
	r.ErrorIs(err, serrors.ErrTableExists)

	ts, err := tm.GetTables()
	r.NoError(err)
	r.Len(ts, 1)
}

func TestManager_DeleteTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode(t)
	defer node.Close()

	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.cleanupGracePeriod = 0
	tm.Start()
	defer tm.Close()

	t.Log("create table")
	_, err := tm.CreateTable(testTableName)
	r.NoError(err)

	t.Log("get table")
	tab, err := tm.GetTable(testTableName)
	r.NoError(err)
	r.NoError(tm.reconcile())

	time.Sleep(1 * time.Second)
	t.Log("delete table")
	r.NoError(tm.DeleteTable(testTableName))
	r.NoError(tm.reconcile())

	t.Log("check table")
	_, err = tm.GetTable(testTableName)
	r.ErrorIs(err, serrors.ErrTableNotFound)

	t.Log("delete non-existent table")
	_, err = tm.GetTable("foo")
	r.ErrorIs(err, serrors.ErrTableNotFound)
	r.NoError(tm.cleanup())

	// LogDB cleaned
	_, err = tm.nh.GetLogReader(tab.ClusterID)
	r.ErrorIs(err, raft.ErrLogDBNotCreatedOrClosed)

	// FS cleaned
	files, err := tm.cfg.Table.FS.List("")
	r.NoError(err)
	r.Len(files, 1, "FS should contain only a root directory (named after hostname)")
}

func TestManager_LeaseTable(t *testing.T) {
	const existingTable = "existingTable"
	type args struct {
		name     string
		duration time.Duration
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Lease existing table",
			args: args{name: existingTable},
		},
		{
			name: "Lease unknown table",
			args: args{name: "unknown"},
		},
	}

	node, m := startRaftNode(t)
	defer node.Close()
	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.Start()
	defer tm.Close()
	_, err := tm.CreateTable(existingTable)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tm.LeaseTable(tt.args.name, tt.args.duration)
			require.NoError(t, err)
		})
	}
}

func TestManager_ReturnTable(t *testing.T) {
	const (
		existingTable = "existingTable"
		leasedTable   = "leasedTable"
	)
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Return existing table",
			args: args{name: existingTable},
		},
		{
			name: "Return leased table",
			args: args{name: leasedTable},
			want: true,
		},
		{
			name: "Return unknown table",
			args: args{name: "unknown"},
		},
	}

	node, m := startRaftNode(t)
	defer node.Close()
	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.Start()
	defer tm.Close()
	_, err := tm.CreateTable(existingTable)
	require.NoError(t, err)

	_, err = tm.CreateTable(leasedTable)
	require.NoError(t, err)
	require.NoError(t, tm.LeaseTable(leasedTable, 60*time.Second))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tm.ReturnTable(tt.args.name)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestManager_GetTable(t *testing.T) {
	const existingTable = "existingTable"
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    ActiveTable
		wantErr error
	}{
		{
			name: "Get existing table",
			args: args{name: existingTable},
			want: ActiveTable{
				Table: Table{Name: existingTable},
			},
		},
		{
			name:    "Get unknown table",
			args:    args{name: "unknown"},
			wantErr: serrors.ErrTableNotFound,
		},
	}

	node, m := startRaftNode(t)
	defer node.Close()
	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.Start()
	defer tm.Close()
	_, err := tm.CreateTable(existingTable)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := tm.GetTable(tt.args.name)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want.Name, got.Name)
		})
	}
}

func TestManager_Restore(t *testing.T) {
	const existingTable = "existingTable"
	node, m := startRaftNode(t)
	defer node.Close()
	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.Start()
	defer tm.Close()
	_, err := tm.CreateTable(existingTable)
	require.NoError(t, err)

	tab, err := tm.GetTable(existingTable)
	require.NoError(t, err)

	sf, err := snapshot.OpenFile("testdata/snapshot.bin")
	require.NoError(t, err)
	require.NoError(t, tm.Restore(existingTable, sf))

	tab2, err := tm.GetTable(existingTable)
	require.NoError(t, err)

	require.Greater(t, tab2.ClusterID, tab.ClusterID, "restored table should have higher ID assigned")
}

func TestManager_reconcile(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode(t)
	defer node.Close()

	const reconcileInterval = 1 * time.Second
	tm := NewManager(node, m, &kv.MapStore{}, minimalTestConfig())
	tm.reconcileInterval = reconcileInterval

	tm.Start()
	defer tm.Close()
	_, err := tm.createTable(testTableName)
	r.NoError(err)
	time.Sleep(reconcileInterval * 3)

	r.NoError(tm.DeleteTable(testTableName))
	time.Sleep(reconcileInterval * 3)
	_, err = tm.GetTable(testTableName)
	r.ErrorIs(err, serrors.ErrTableNotFound)
}

func Test_diffTables(t *testing.T) {
	type args struct {
		tables   map[string]Table
		raftInfo []raft.ShardInfo
	}
	tests := []struct {
		name        string
		args        args
		wantToStart map[uint64]Table
		wantToStop  []uint64
	}{
		{
			name: "Start a single table",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
				},
				raftInfo: []raft.ShardInfo{},
			},
			wantToStart: map[uint64]Table{
				10001: {
					Name:      "foo",
					ClusterID: 10001,
				},
			},
			wantToStop: nil,
		},
		{
			name: "Start a single table with invalid ClusterID",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10,
					},
				},
				raftInfo: []raft.ShardInfo{},
			},
			wantToStart: nil,
			wantToStop:  nil,
		},
		{
			name: "Stop a single table",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
					"bar": {
						Name:      "foo",
						ClusterID: 10002,
					},
				},
				raftInfo: []raft.ShardInfo{
					{
						ShardID: 10001,
					},
					{
						ShardID: 10002,
					},
					{
						ShardID: 10003,
					},
				},
			},
			wantToStart: nil,
			wantToStop:  []uint64{10003},
		},
		{
			name: "Stop a single table with invalid ClusterID",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
					"bar": {
						Name:      "foo",
						ClusterID: 10002,
					},
				},
				raftInfo: []raft.ShardInfo{
					{
						ShardID: 10001,
					},
					{
						ShardID: 10002,
					},
					{
						ShardID: 10,
					},
				},
			},
			wantToStart: nil,
			wantToStop:  nil,
		},
		{
			name: "Start a recovery table",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						RecoverID: 10001,
					},
				},
				raftInfo: []raft.ShardInfo{},
			},
			wantToStart: map[uint64]Table{
				10001: {
					Name:      "foo",
					RecoverID: 10001,
				},
			},
			wantToStop: nil,
		},
		{
			name: "Recover existing table table",
			args: args{
				tables: map[string]Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
						RecoverID: 10002,
					},
				},
				raftInfo: []raft.ShardInfo{},
			},
			wantToStart: map[uint64]Table{
				10001: {
					Name:      "foo",
					ClusterID: 10001,
					RecoverID: 10002,
				},
				10002: {
					Name:      "foo",
					ClusterID: 10001,
					RecoverID: 10002,
				},
			},
			wantToStop: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			gotToStart, gotToStop := diffTables(tt.args.tables, tt.args.raftInfo)
			r.Equal(tt.wantToStart, gotToStart)
			r.Equal(tt.wantToStop, gotToStop)
		})
	}
}

func startRaftNode(t *testing.T) (*raft.NodeHost, map[uint64]string) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, l.Close())
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 1,
		RaftAddress:    l.Addr().String(),
		EnableMetrics:  true,
	}
	require.NoError(t, nhc.Prepare())
	nhc.Expert.FS = vfs.NewMem()
	nhc.Expert.Engine.ExecShards = 1
	nhc.Expert.LogDB.Shards = 1
	nh, err := raft.NewNodeHost(nhc)
	require.NoError(t, err)
	return nh, map[uint64]string{1: l.Addr().String()}
}
