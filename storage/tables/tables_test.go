// Copyright JAMF Software, LLC

package tables

import (
	"fmt"
	"net"
	"testing"
	"time"

	pvfs "github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/log"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	logger.SetLoggerFactory(log.LoggerFactory(zap.NewNop()))
}

var minimalTestConfig = func() Config {
	return Config{
		NodeID: 1,
		Table:  TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem(), BlockCacheSize: 1024},
		Meta:   MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
	}
}

func TestManager_CreateTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode()
	defer node.Close()

	tm := NewManager(node, m, minimalTestConfig())
	r.NoError(tm.Start())
	defer tm.Close()
	r.NoError(tm.WaitUntilReady())

	t.Log("create table")
	r.NoError(tm.CreateTable(testTableName))

	t.Log("get table")
	tab, err := tm.GetTable(testTableName)
	r.NoError(err)
	r.Equal(testTableName, tab.Name)
	r.Greater(tab.ClusterID, tableIDsRangeStart)

	t.Log("create existing table")
	r.ErrorIs(tm.CreateTable(testTableName), serrors.ErrTableExists)
}

func TestManager_DeleteTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode()
	defer node.Close()

	tm := NewManager(node, m, minimalTestConfig())
	tm.cleanupGracePeriod = 0
	r.NoError(tm.Start())
	defer tm.Close()
	r.NoError(tm.WaitUntilReady())

	t.Log("create table")
	r.NoError(tm.CreateTable(testTableName))

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
	r.ErrorIs(err, dragonboat.ErrLogDBNotCreatedOrClosed)

	// FS cleaned
	files, err := tm.cfg.Table.FS.List("")
	r.Equal(1, len(files), "FS should contain only a root directory (named after hostname)")
}

func TestManager_GetTable(t *testing.T) {
	const existingTable = "existingTable"
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    table.ActiveTable
		wantErr error
	}{
		{
			name: "Get existing table",
			args: args{name: existingTable},
			want: table.ActiveTable{
				Table: table.Table{Name: existingTable},
			},
		},
		{
			name:    "Get unknown table",
			args:    args{name: "unknown"},
			wantErr: serrors.ErrTableNotFound,
		},
	}

	node, m := startRaftNode()
	defer node.Close()
	tm := NewManager(node, m, minimalTestConfig())
	_ = tm.Start()
	defer tm.Close()
	_ = tm.WaitUntilReady()
	_ = tm.CreateTable(existingTable)

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

func TestManager_reconcile(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode()
	defer node.Close()

	const reconcileInterval = 1 * time.Second
	tm := NewManager(node, m, minimalTestConfig())
	tm.reconcileInterval = reconcileInterval

	r.NoError(tm.Start())
	defer tm.Close()
	r.NoError(tm.WaitUntilReady())
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
		tables   map[string]table.Table
		raftInfo []dragonboat.ShardInfo
	}
	tests := []struct {
		name        string
		args        args
		wantToStart map[uint64]table.Table
		wantToStop  []uint64
	}{
		{
			name: "Start a single table",
			args: args{
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
				},
				raftInfo: []dragonboat.ShardInfo{},
			},
			wantToStart: map[uint64]table.Table{
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
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10,
					},
				},
				raftInfo: []dragonboat.ShardInfo{},
			},
			wantToStart: nil,
			wantToStop:  nil,
		},
		{
			name: "Stop a single table",
			args: args{
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
					"bar": {
						Name:      "foo",
						ClusterID: 10002,
					},
				},
				raftInfo: []dragonboat.ShardInfo{
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
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
					},
					"bar": {
						Name:      "foo",
						ClusterID: 10002,
					},
				},
				raftInfo: []dragonboat.ShardInfo{
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
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						RecoverID: 10001,
					},
				},
				raftInfo: []dragonboat.ShardInfo{},
			},
			wantToStart: map[uint64]table.Table{
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
				tables: map[string]table.Table{
					"foo": {
						Name:      "foo",
						ClusterID: 10001,
						RecoverID: 10002,
					},
				},
				raftInfo: []dragonboat.ShardInfo{},
			},
			wantToStart: map[uint64]table.Table{
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

func getTestPort() int {
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startRaftNode() (*dragonboat.NodeHost, map[uint64]string) {
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
		zap.S().Panic(err)
	}
	return nh, map[uint64]string{1: testNodeAddress}
}
