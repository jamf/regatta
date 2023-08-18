// Copyright JAMF Software, LLC

package kv

import (
	"fmt"
	"net"
	"path"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
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
		_, _ = store.Set(k, v, 0)
	}
}

// Check interface compatibility.
var (
	_ store = &MapStore{}
	_ store = &RaftStore{}
)

func mapStoreFunc() store {
	return &MapStore{}
}

func raftStoreFunc() store {
	return newRaftStore()
}

func TestStore_Exists(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		store func() store
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
			store := tt.store()
			fillData(store, testData)
			got, _ := store.Exists(tt.args.key)
			r.Equal(tt.want, got)
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
			}
		})
	}
}

func TestStore_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		store   func() store
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
			store := tt.store()
			fillData(store, testData)
			got, err := store.Get(tt.args.key)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want.Key, got.Key)
			r.Equal(tt.want.Value, got.Value)
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
				r.GreaterOrEqual(got.Ver, uint64(0))
			}
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
		store     func() store
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
			store := tt.store()
			fillData(store, dirTestData)
			got, err := store.GetAll(tt.args.pattern)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			if tt.wantCount > 0 {
				r.Equal(tt.wantCount, len(got))
			} else {
				r.Equal(tt.want, got)
			}
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
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
		store   func() store
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
			store := tt.store()
			fillData(store, dirTestData)
			got, err := store.GetAllValues(tt.args.pattern)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
			}
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
		store   func() store
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
			store := tt.store()
			fillData(store, dirTestData)
			got, err := store.List(tt.args.filePath)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
			}
		})
	}
}

func TestStore_ListDir(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		store   func() store
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
			store := tt.store()
			fillData(store, dirTestData)
			got, err := store.ListDir(tt.args.filePath)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
			}
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
		store func() store
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
			store := tt.store()
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
				if rs, ok := store.(*RaftStore); ok {
					rs.NodeHost.Close()
				}
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
		store   func() store
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
			store := tt.store()
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
			if rs, ok := store.(*RaftStore); ok {
				rs.NodeHost.Close()
			}
		})
	}
}

func newRaftStore() *RaftStore {
	getTestPort := func() int {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}

	startRaftNode := func() *dragonboat.NodeHost {
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
			panic(err)
		}

		cc := config.Config{
			ReplicaID:          1,
			ShardID:            1,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		}

		err = nh.StartConcurrentReplica(map[uint64]string{1: testNodeAddress}, false, NewLFSM(), cc)
		if err != nil {
			panic(err)
		}

		ready := make(chan struct{})
		go func() {
			for {
				i := nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				if i.ShardInfoList[0].LeaderID == cc.ReplicaID {
					close(ready)
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()

		// Listen on our channel AND a timeout channel - which ever happens first.
		select {
		case <-ready:
		case <-time.After(30 * time.Second):
			panic("unable to start test Dragonboat in timeout of 30s")
		}

		return nh
	}

	return &RaftStore{NodeHost: startRaftNode(), ClusterID: 1}
}
