// Copyright JAMF Software, LLC

package kv

import (
	"context"
	"fmt"
	"net"
	"path"
	"testing"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

var testData = map[string]string{
	"/app/db/pass":  "foo",
	"/app/db/user":  "admin",
	"/app/web/port": "443",
	"/app/web/url":  "app.example.com",
}

var dirTestData = map[string]string{
	"/deis/database/user":             "user",
	"/deis/database/pass":             "pass",
	"/deis/services/key":              "value",
	"/deis/services/notaservice/foo":  "bar",
	"/deis/services/srv1/node1":       "10.244.1.1:80",
	"/deis/services/srv1/node2":       "10.244.1.2:80",
	"/deis/services/srv1/node3":       "10.244.1.3:80",
	"/deis/services/srv2/node1":       "10.244.2.1:80",
	"/deis/services/srv2/node2":       "10.244.2.2:80",
	"/deis/prefix/node1":              "prefix_node1",
	"/deis/prefix/node2/leafnode":     "prefix_node2",
	"/deis/prefix/node3/leafnode":     "prefix_node3",
	"/deis/prefix_a/node4":            "prefix_a_node4",
	"/deis/prefixb/node5/leafnode":    "prefixb_node5",
	"/deis/dirprefix/node1":           "prefix_node1",
	"/deis/dirprefix/node2/leafnode":  "prefix_node2",
	"/deis/dirprefix/node3/leafnode":  "prefix_node3",
	"/deis/dirprefix_a/node4":         "prefix_a_node4",
	"/deis/dirprefixb/node5/leafnode": "prefixb_node5",
}

type store interface {
	Set(key string, value string, ver uint64) (Pair, error)
	Delete(key string, ver uint64) error
	Exists(key string) (bool, error)
	Get(key string) (Pair, error)
	GetAll(pattern string) ([]Pair, error)
	GetAllValues(pattern string) ([]string, error)
	List(filePath string) ([]string, error)
	ListDir(filePath string) ([]string, error)
}

func fillData(store store, data map[string]string) {
	for k, v := range data {
		_, err := store.Set(k, v, 0)
		if err != nil {
			panic(err)
		}
	}
}

// Check interface compatibility.
var (
	_ store = &MapStore{}
	_ store = &RaftStore{}
)

func mapStoreFunc(t *testing.T) store {
	return &MapStore{}
}

func raftStoreFunc(t *testing.T) store {
	return newRaftStore(t)
}

func TestStore_Exists(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		store func(t *testing.T) store
		args  args
		want  bool
	}{
		{
			name:  "Get specific key - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/app/db/pass"},
			want:  true,
		},
		{
			name:  "Get nonexistent key - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/nonenexistent"},
			want:  false,
		},
		{
			name:  "Get invalid pattern - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/non\\&6/=="},
			want:  false,
		},
		{
			name:  "Get specific key - RaftStore",
			store: raftStoreFunc,
			args:  args{key: "/app/db/pass"},
			want:  true,
		},
		{
			name:  "Get nonexistent key - RaftStore",
			store: raftStoreFunc,
			args:  args{key: "/nonenexistent/*"},
			want:  false,
		},
		{
			name:  "Get invalid pattern - RaftStore",
			store: raftStoreFunc,
			args:  args{key: "/non\\&6/=="},
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, testData)
			got, _ := store.Exists(tt.args.key)
			r.Equal(tt.want, got)
		})
	}
}

func TestStore_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		store   func(t *testing.T) store
		args    args
		want    Pair
		wantErr error
	}{
		{
			name:  "Get specific key - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/app/db/pass"},
			want:  Pair{Key: "/app/db/pass", Value: "foo"},
		},
		{
			name:    "Get nonexistent key - MapStore",
			store:   mapStoreFunc,
			args:    args{key: "/nonenexistent"},
			wantErr: ErrNotExist,
		},
		{
			name:    "Get invalid pattern - MapStore",
			store:   mapStoreFunc,
			args:    args{key: "/non\\&6/=="},
			wantErr: ErrNotExist,
		},
		{
			name:  "Get specific key - RaftStore",
			store: raftStoreFunc,
			args:  args{key: "/app/db/pass"},
			want:  Pair{Key: "/app/db/pass", Value: "foo"},
		},
		{
			name:    "Get nonexistent key - RaftStore",
			store:   raftStoreFunc,
			args:    args{key: "/nonenexistent/*"},
			wantErr: ErrNotExist,
		},
		{
			name:    "Get invalid pattern - RaftStore",
			store:   raftStoreFunc,
			args:    args{key: "/non\\&6/=="},
			wantErr: ErrNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, testData)
			got, err := store.Get(tt.args.key)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want.Key, got.Key)
			r.Equal(tt.want.Value, got.Value)
		})
	}
}

func TestStore_GetAll(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name      string
		args      args
		store     func(t *testing.T) store
		want      []Pair
		wantCount int
		wantErr   error
	}{
		{
			name:  "GetAll /deis/database/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/database/*"},
			want: []Pair{
				{
					Key:   "/deis/database/pass",
					Value: "pass",
				},
				{
					Key:   "/deis/database/user",
					Value: "user",
				},
			},
		},
		{
			name:  "GetAll /deis/services/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/services/*"},
			want: []Pair{
				{
					Key:   "/deis/services/key",
					Value: "value",
				},
			},
		},
		{
			name:  "GetAll /deis/services/*/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/services/*/*"},
			want: []Pair{
				{
					Key:   "/deis/services/notaservice/foo",
					Value: "bar",
				},
				{
					Key:   "/deis/services/srv1/node1",
					Value: "10.244.1.1:80",
				},
				{
					Key:   "/deis/services/srv1/node2",
					Value: "10.244.1.2:80",
				},
				{
					Key:   "/deis/services/srv1/node3",
					Value: "10.244.1.3:80",
				},
				{
					Key:   "/deis/services/srv2/node1",
					Value: "10.244.2.1:80",
				},
				{
					Key:   "/deis/services/srv2/node2",
					Value: "10.244.2.2:80",
				},
			},
		},
		{
			name:  "GetAll /nonenexistent/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/nonenexistent/*"},
			want:  []Pair{},
		},
		{
			name:    "GetAll invalid pattern - MapStore",
			store:   mapStoreFunc,
			args:    args{pattern: "/non\\&6/==[]"},
			want:    []Pair{},
			wantErr: path.ErrBadPattern,
		},
		{
			name:      "GetAll /deis/database/* - RaftStore",
			store:     raftStoreFunc,
			args:      args{pattern: "/deis/database/*"},
			wantCount: 2,
		},
		{
			name:      "GetAll /deis/services/* - RaftStore",
			store:     raftStoreFunc,
			args:      args{pattern: "/deis/services/*"},
			wantCount: 1,
		},
		{
			name:      "GetAll /deis/services/*/* - RaftStore",
			store:     raftStoreFunc,
			args:      args{pattern: "/deis/services/*/*"},
			wantCount: 6,
		},
		{
			name:  "GetAll /nonenexistent/* - RaftStore",
			store: raftStoreFunc,
			args:  args{pattern: "/nonenexistent/*"},
			want:  []Pair{},
		},
		{
			name:    "GetAll invalid pattern - RaftStore",
			store:   raftStoreFunc,
			args:    args{pattern: "/non\\&6/==[]"},
			want:    []Pair{},
			wantErr: path.ErrBadPattern,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, dirTestData)
			got, err := store.GetAll(tt.args.pattern)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			if tt.wantCount > 0 {
				r.Len(got, tt.wantCount)
			} else {
				r.Equal(tt.want, got)
			}
		})
	}
}

func TestStore_GetAllValues(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name    string
		args    args
		store   func(t *testing.T) store
		want    []string
		wantErr error
	}{
		{
			name:  "GetAllValues /deis/database/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/database/*"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "GetAllValues /deis/services/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/services/*"},
			want:  []string{"value"},
		},
		{
			name:  "GetAllValues /deis/services/*/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/deis/services/*/*"},
			want:  []string{"10.244.1.1:80", "10.244.1.2:80", "10.244.1.3:80", "10.244.2.1:80", "10.244.2.2:80", "bar"},
		},
		{
			name:  "GetAllValues /nonenexistent/* - MapStore",
			store: mapStoreFunc,
			args:  args{pattern: "/nonenexistent/*"},
			want:  []string{},
		},
		{
			name:    "GetAllValues invalid pattern - MapStore",
			store:   mapStoreFunc,
			args:    args{pattern: "/non\\&6/==[]"},
			want:    []string{},
			wantErr: path.ErrBadPattern,
		},
		{
			name:  "GetAllValues /deis/database/* - RaftStore",
			store: raftStoreFunc,
			args:  args{pattern: "/deis/database/*"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "GetAllValues /deis/services/* - RaftStore",
			store: raftStoreFunc,
			args:  args{pattern: "/deis/services/*"},
			want:  []string{"value"},
		},
		{
			name:  "GetAllValues /deis/services/*/* - RaftStore",
			store: raftStoreFunc,
			args:  args{pattern: "/deis/services/*/*"},
			want:  []string{"10.244.1.1:80", "10.244.1.2:80", "10.244.1.3:80", "10.244.2.1:80", "10.244.2.2:80", "bar"},
		},
		{
			name:  "GetAllValues /nonenexistent/* - RaftStore",
			store: raftStoreFunc,
			args:  args{pattern: "/nonenexistent/*"},
			want:  []string{},
		},
		{
			name:    "GetAllValues invalid pattern - RaftStore",
			store:   raftStoreFunc,
			args:    args{pattern: "/non\\&6/==[]"},
			want:    []string{},
			wantErr: path.ErrBadPattern,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, dirTestData)
			got, err := store.GetAllValues(tt.args.pattern)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestStore_List(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		store   func(t *testing.T) store
		want    []string
		wantErr bool
	}{
		{
			name:  "List /deis/database - MapStore",
			store: mapStoreFunc,
			args:  args{filePath: "/deis/database"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "List /deis/services - MapStore",
			store: mapStoreFunc,
			args:  args{filePath: "/deis/services"},
			want:  []string{"key", "notaservice", "srv1", "srv2"},
		},
		{
			name:  "List /deis/database - RaftStore",
			store: raftStoreFunc,
			args:  args{filePath: "/deis/database"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "List /deis/services - RaftStore",
			store: raftStoreFunc,
			args:  args{filePath: "/deis/services"},
			want:  []string{"key", "notaservice", "srv1", "srv2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, dirTestData)
			got, err := store.List(tt.args.filePath)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func TestStore_ListDir(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		store   func(t *testing.T) store
		args    args
		want    []string
		wantErr bool
	}{
		{
			name:  "List /deis/database - MapStore",
			store: mapStoreFunc,
			args:  args{filePath: "/deis/database"},
			want:  []string{},
		},
		{
			name:  "List /deis/services - MapStore",
			store: mapStoreFunc,
			args:  args{filePath: "/deis/services"},
			want:  []string{"notaservice", "srv1", "srv2"},
		},
		{
			name:  "List /deis/database - RaftStore",
			store: raftStoreFunc,
			args:  args{filePath: "/deis/database"},
			want:  []string{},
		},
		{
			name:  "List /deis/services - RaftStore",
			store: raftStoreFunc,
			args:  args{filePath: "/deis/services"},
			want:  []string{"notaservice", "srv1", "srv2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, dirTestData)
			got, err := store.ListDir(tt.args.filePath)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func TestStore_Set(t *testing.T) {
	type arg struct {
		key     string
		value   string
		ver     uint64
		wantErr bool
	}
	type args []arg
	tests := []struct {
		name  string
		store func(t *testing.T) store
		args  args
	}{
		{
			name:  "Key overwrite test - MapStore",
			store: mapStoreFunc,
			args: args{
				{key: "/key", value: "val", ver: 0},
				{key: "/key", value: "val", ver: 0},
			},
		},
		{
			name:  "Key overwrite test - RaftStore",
			store: raftStoreFunc,
			args: args{
				{key: "/key", value: "val", ver: 0},
				{key: "/key", value: "val", ver: 0, wantErr: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			for k, v := range testData {
				set, err := store.Set(k, v, 0)
				r.NoError(err)
				r.Equal(set.Key, k)
				r.Equal(set.Value, v)
			}
			for _, a := range tt.args {
				set, err := store.Set(a.key, a.value, a.ver)
				if a.wantErr {
					r.Error(err)
					return
				}
				r.NoError(err)
				r.Equal(set.Key, a.key)
				r.Equal(set.Value, a.value)
			}
		})
	}
}

func TestStore_Delete(t *testing.T) {
	type args struct {
		key          string
		ver          uint64
		fetchVersion bool
	}
	tests := []struct {
		name    string
		store   func(t *testing.T) store
		args    args
		wantErr bool
	}{
		{
			name:  "Simple delete - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/app/db/pass"},
		},
		{
			name:    "Simple delete - RaftStore",
			store:   raftStoreFunc,
			args:    args{key: "/app/db/pass"},
			wantErr: true,
		},
		{
			name:  "Delete with version - MapStore",
			store: mapStoreFunc,
			args:  args{key: "/app/db/pass", fetchVersion: true},
		},
		{
			name:  "Delete with version  - RaftStore",
			store: raftStoreFunc,
			args:  args{key: "/app/db/pass", fetchVersion: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			store := tt.store(t)
			fillData(store, testData)
			if tt.args.fetchVersion {
				val, err := store.Get(tt.args.key)
				r.NoError(err)
				tt.args.ver = val.Ver
			}

			err := store.Delete(tt.args.key, tt.args.ver)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)

			t.Log("check that record got deleted")
			_, err = store.Get(tt.args.key)
			r.Equal(ErrNotExist, err)

			t.Log("check that just a single record got deleted")
			all, err := store.GetAllValues("/*/*/*")
			r.NoError(err)
			r.Len(all, len(testData)-1)
		})
	}
}

func newRaftStore(t *testing.T) *RaftStore {
	t.Helper()
	getTestPort := func() int {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}
	testNodeAddress := fmt.Sprintf("127.0.0.1:%d", getTestPort())

	startRaftNode := func() *raft.NodeHost {
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
		nh, err := raft.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}
		return nh
	}

	node := startRaftNode()
	t.Cleanup(node.Close)
	rs := &RaftStore{NodeHost: node, ClusterID: 1}
	require.NoError(t, rs.Start(RaftConfig{
		NodeID:             1,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		SnapshotEntries:    10000,
		CompactionOverhead: 5000,
		InitialMembers:     map[uint64]string{1: testNodeAddress},
	}))
	require.NoError(t, rs.WaitForLeader(context.Background()))
	return rs
}
