package tables

import (
	"context"
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
	"github.com/wandera/regatta/storage/table"
	"go.uber.org/zap"
)

var minimalTestConfig = Config{
	NodeID: 1,
	Table:  TableConfig{HeartbeatRTT: 1, ElectionRTT: 5, FS: pvfs.NewMem()},
	Meta:   MetaConfig{HeartbeatRTT: 1, ElectionRTT: 5},
}

func TestManager_CreateTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode()
	defer node.Close()

	tm := NewManager(node, m, minimalTestConfig)
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
	r.ErrorIs(tm.CreateTable(testTableName), ErrTableExists)
}

func TestManager_DeleteTable(t *testing.T) {
	const testTableName = "test"
	r := require.New(t)
	node, m := startRaftNode()
	defer node.Close()

	tm := NewManager(node, m, minimalTestConfig)
	r.NoError(tm.Start())
	defer tm.Close()
	r.NoError(tm.WaitUntilReady())

	t.Log("create table")
	r.NoError(tm.CreateTable(testTableName))

	t.Log("get table")
	_, err := tm.GetTable(testTableName)
	r.NoError(err)

	t.Log("delete table")
	r.NoError(tm.DeleteTable(testTableName))
	r.NoError(tm.reconcile())

	t.Log("check table")
	_, err = tm.GetTable(testTableName)
	r.ErrorIs(err, ErrTableDoesNotExist)

	t.Log("delete non-existent table")
	_, err = tm.GetTable("foo")
	r.ErrorIs(err, ErrTableDoesNotExist)
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
			wantErr: ErrTableDoesNotExist,
		},
	}

	node, m := startRaftNode()
	defer node.Close()
	tm := NewManager(node, m, minimalTestConfig)
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
	tm := NewManager(node, m, minimalTestConfig)
	tm.reconcileInterval = reconcileInterval

	r.NoError(tm.Start())
	defer tm.Close()
	r.NoError(tm.WaitUntilReady())
	_, err := tm.createTable(testTableName)
	r.NoError(err)
	time.Sleep(reconcileInterval * 3)
	at, err := tm.GetTable(testTableName)
	r.NoError(err)

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	t.Log("test table is started")
	_, err = at.Hash(ctx, &proto.HashRequest{})
	r.NoError(err)

	r.NoError(tm.DeleteTable(testTableName))
	time.Sleep(reconcileInterval * 3)
	_, err = tm.GetTable(testTableName)
	r.ErrorIs(err, ErrTableDoesNotExist)
}

func Test_diffTables(t *testing.T) {
	type args struct {
		tables   map[string]table.Table
		raftInfo []dragonboat.ClusterInfo
	}
	tests := []struct {
		name        string
		args        args
		wantToStart []table.Table
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
				raftInfo: []dragonboat.ClusterInfo{},
			},
			wantToStart: []table.Table{
				{
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
				raftInfo: []dragonboat.ClusterInfo{},
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
				raftInfo: []dragonboat.ClusterInfo{
					{
						ClusterID: 10001,
					},
					{
						ClusterID: 10002,
					},
					{
						ClusterID: 10003,
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
				raftInfo: []dragonboat.ClusterInfo{
					{
						ClusterID: 10001,
					},
					{
						ClusterID: 10002,
					},
					{
						ClusterID: 10,
					},
				},
			},
			wantToStart: nil,
			wantToStop:  nil,
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
