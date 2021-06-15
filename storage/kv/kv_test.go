package kv

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

var testData = map[string]string{
	"/app/db/pass":               "foo",
	"/app/db/user":               "admin",
	"/app/port":                  "443",
	"/app/url":                   "app.example.com",
	"/app/vhosts/host1":          "app.example.com",
	"/app/upstream/host1":        "203.0.113.0.1:8080",
	"/app/upstream/host1/domain": "app.example.com",
	"/app/upstream/host2":        "203.0.113.0.2:8080",
	"/app/upstream/host2/domain": "app.example.com",
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
	Exists(key string) bool
	Get(key string) (Pair, error)
	GetAll(pattern string) (Pairs, error)
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

func TestStore_Exists(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		store store
		args  args
		want  bool
	}{
		{
			name:  "Get specific key - MapStore",
			store: &MapStore{},
			args:  args{key: "/app/db/pass"},
			want:  true,
		},
		{
			name:  "Get nonexistent key - MapStore",
			store: &MapStore{},
			args:  args{key: "/nonenexistent"},
			want:  false,
		},
		{
			name:  "Get invalid pattern - MapStore",
			store: &MapStore{},
			args:  args{key: "/non\\&6/=="},
			want:  false,
		},
		{
			name:  "Get specific key - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{key: "/app/db/pass"},
			want:  true,
		},
		{
			name:  "Get nonexistent key - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{key: "/nonenexistent/*"},
			want:  false,
		},
		{
			name:  "Get invalid pattern - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{key: "/non\\&6/=="},
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, testData)
			got := tt.store.Exists(tt.args.key)
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
		store   store
		args    args
		want    Pair
		wantErr bool
	}{
		{
			name:  "Get specific key - MapStore",
			store: &MapStore{},
			args:  args{key: "/app/db/pass"},
			want:  Pair{Key: "/app/db/pass", Value: "foo"},
		},
		{
			name:    "Get nonexistent key - MapStore",
			store:   &MapStore{},
			args:    args{key: "/nonenexistent"},
			wantErr: true,
		},
		{
			name:    "Get invalid pattern - MapStore",
			store:   &MapStore{},
			args:    args{key: "/non\\&6/=="},
			wantErr: true,
		},
		{
			name:  "Get specific key - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{key: "/app/db/pass"},
			want:  Pair{Key: "/app/db/pass", Value: "foo"},
		},
		{
			name:    "Get nonexistent key - RaftStore",
			store:   newRaftStore(vfs.NewMem()),
			args:    args{key: "/nonenexistent/*"},
			wantErr: true,
		},
		{
			name:    "Get invalid pattern - RaftStore",
			store:   newRaftStore(vfs.NewMem()),
			args:    args{key: "/non\\&6/=="},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, testData)
			got, err := tt.store.Get(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want.Key, got.Key)
			r.Equal(tt.want.Value, got.Value)
			if _, ok := tt.store.(*RaftStore); ok {
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
		store     store
		want      Pairs
		wantCount int
		wantErr   bool
	}{
		{
			name:  "GetAll /deis/database/* - MapStore",
			store: &MapStore{},
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
			store: &MapStore{},
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
			store: &MapStore{},
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
			store: &MapStore{},
			args:  args{pattern: "/nonenexistent/*"},
			want:  []Pair{},
		},
		{
			name:  "GetAll invalid pattern - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/non\\&6/=="},
			want:  []Pair{},
		},
		{
			name:      "GetAll /deis/database/* - RaftStore",
			store:     newRaftStore(vfs.NewMem()),
			args:      args{pattern: "/deis/database/*"},
			wantCount: 2,
		},
		{
			name:      "GetAll /deis/services/* - RaftStore",
			store:     newRaftStore(vfs.NewMem()),
			args:      args{pattern: "/deis/services/*"},
			wantCount: 1,
		},
		{
			name:      "GetAll /deis/services/*/* - RaftStore",
			store:     newRaftStore(vfs.NewMem()),
			args:      args{pattern: "/deis/services/*/*"},
			wantCount: 6,
		},
		{
			name:  "GetAll /nonenexistent/* - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/nonenexistent/*"},
			want:  []Pair{},
		},
		{
			name:  "GetAll invalid pattern - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/non\\&6/=="},
			want:  []Pair{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, dirTestData)
			got, err := tt.store.GetAll(tt.args.pattern)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			if tt.wantCount > 0 {
				r.Equal(tt.wantCount, got.Len())
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
		store   store
		want    []string
		wantErr bool
	}{
		{
			name:  "GetAllValues /deis/database/* - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/deis/database/*"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "GetAllValues /deis/services/* - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/deis/services/*"},
			want:  []string{"value"},
		},
		{
			name:  "GetAllValues /deis/services/*/* - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/deis/services/*/*"},
			want:  []string{"10.244.1.1:80", "10.244.1.2:80", "10.244.1.3:80", "10.244.2.1:80", "10.244.2.2:80", "bar"},
		},
		{
			name:  "GetAllValues /nonenexistent/* - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/nonenexistent/*"},
			want:  []string{},
		},
		{
			name:  "GetAllValues invalid pattern - MapStore",
			store: &MapStore{},
			args:  args{pattern: "/non\\&6/=="},
			want:  []string{},
		},
		{
			name:  "GetAllValues /deis/database/* - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/deis/database/*"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "GetAllValues /deis/services/* - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/deis/services/*"},
			want:  []string{"value"},
		},
		{
			name:  "GetAllValues /deis/services/*/* - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/deis/services/*/*"},
			want:  []string{"10.244.1.1:80", "10.244.1.2:80", "10.244.1.3:80", "10.244.2.1:80", "10.244.2.2:80", "bar"},
		},
		{
			name:  "GetAllValues /nonenexistent/* - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/nonenexistent/*"},
			want:  []string{},
		},
		{
			name:  "GetAllValues invalid pattern - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{pattern: "/non\\&6/=="},
			want:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, dirTestData)
			got, err := tt.store.GetAllValues(tt.args.pattern)
			if tt.wantErr {
				r.Error(err)
				return
			}
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
		store   store
		want    []string
		wantErr bool
	}{
		{
			name:  "List /deis/database - MapStore",
			store: &MapStore{},
			args:  args{filePath: "/deis/database"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "List /deis/services - MapStore",
			store: &MapStore{},
			args:  args{filePath: "/deis/services"},
			want:  []string{"key", "notaservice", "srv1", "srv2"},
		},
		{
			name:  "List /deis/database - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{filePath: "/deis/database"},
			want:  []string{"pass", "user"},
		},
		{
			name:  "List /deis/services - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{filePath: "/deis/services"},
			want:  []string{"key", "notaservice", "srv1", "srv2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, dirTestData)
			got, err := tt.store.List(tt.args.filePath)
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
		store   store
		args    args
		want    []string
		wantErr bool
	}{
		{
			name:  "List /deis/database - MapStore",
			store: &MapStore{},
			args:  args{filePath: "/deis/database"},
			want:  []string{},
		},
		{
			name:  "List /deis/services - MapStore",
			store: &MapStore{},
			args:  args{filePath: "/deis/services"},
			want:  []string{"notaservice", "srv1", "srv2"},
		},
		{
			name:  "List /deis/database - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{filePath: "/deis/database"},
			want:  []string{},
		},
		{
			name:  "List /deis/services - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{filePath: "/deis/services"},
			want:  []string{"notaservice", "srv1", "srv2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, dirTestData)
			got, err := tt.store.ListDir(tt.args.filePath)
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
		store store
		args  args
	}{
		{
			name:  "Key overwrite test - MapStore",
			store: &MapStore{},
			args: args{
				{key: "/key", value: "val", ver: 0},
				{key: "/key", value: "val", ver: 0},
			},
		},
		{
			name:  "Key overwrite test - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args: args{
				{key: "/key", value: "val", ver: 0},
				{key: "/key", value: "val", ver: 0, wantErr: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			for k, v := range testData {
				set, err := tt.store.Set(k, v, 0)
				r.NoError(err)
				r.Equal(set.Key, k)
				r.Equal(set.Value, v)
			}
			for _, a := range tt.args {
				set, err := tt.store.Set(a.key, a.value, a.ver)
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
		store   store
		args    args
		wantErr bool
	}{
		{
			name:  "Simple delete - MapStore",
			store: &MapStore{},
			args:  args{key: "/app/db/pass"},
		},
		{
			name:    "Simple delete - RaftStore",
			store:   newRaftStore(vfs.NewMem()),
			args:    args{key: "/app/db/pass"},
			wantErr: true,
		},
		{
			name:  "Delete with version - MapStore",
			store: &MapStore{},
			args:  args{key: "/app/db/pass", fetchVersion: true},
		},
		{
			name:  "Delete with version  - RaftStore",
			store: newRaftStore(vfs.NewMem()),
			args:  args{key: "/app/db/pass", fetchVersion: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			fillData(tt.store, testData)
			if tt.args.fetchVersion {
				val, err := tt.store.Get(tt.args.key)
				r.NoError(err)
				tt.args.ver = val.Ver
			}

			err := tt.store.Delete(tt.args.key, tt.args.ver)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			_, err = tt.store.Get(tt.args.key)
			r.Equal(ErrNotExist, err)
		})
	}
}

func newRaftStore(ifs vfs.FS) *RaftStore {
	getTestPort := func() int {
		l, _ := net.Listen("tcp", ":0")
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}

	startRaftNode := func() *dragonboat.NodeHost {
		testNodeAddress := fmt.Sprintf("localhost:%d", getTestPort())
		nhc := config.NodeHostConfig{
			WALDir:         "wal",
			NodeHostDir:    "dragonboat",
			RTTMillisecond: 1,
			RaftAddress:    testNodeAddress,
		}
		_ = nhc.Prepare()
		nhc.Expert.FS = ifs
		nhc.Expert.Engine.ExecShards = 1
		nhc.Expert.LogDB.Shards = 1
		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			panic(err)
		}

		cc := config.Config{
			NodeID:             1,
			ClusterID:          1,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10000,
			CompactionOverhead: 5000,
		}

		err = nh.StartConcurrentCluster(map[uint64]string{1: testNodeAddress}, false, NewLFSM(), cc)
		if err != nil {
			panic(err)
		}

		ready := make(chan struct{})
		go func() {
			for {
				i := nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				if i.ClusterInfoList[0].IsLeader {
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
