package logreader

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/util"
)

type mockLogQuerier struct {
	mock.Mock
}

func (m *mockLogQuerier) QueryRaftLog(shardID uint64, firstIndex uint64, lastIndex uint64, maxSize uint64) (*dragonboat.RequestState, error) {
	called := m.Called(shardID, firstIndex, lastIndex, maxSize)
	return called.Get(0).(*dragonboat.RequestState), called.Error(1)
}

func TestLogReader_NodeDeleted(t *testing.T) {
	type args struct {
		info raftio.NodeInfo
	}
	tests := []struct {
		name          string
		initialShards []uint64
		args          args
		assert        func(*testing.T, *util.SyncMap[uint64, *shard])
	}{
		{
			name:          "remove existing cache shard",
			initialShards: []uint64{1},
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(1))
				require.False(t, ok, "unexpected cache shard")
			},
		},
		{
			name: "remove non-existent cache shard",
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(1))
				require.False(t, ok, "unexpected cache shard")
			},
		},
		{
			name:          "remove existent cache shard from list",
			initialShards: []uint64{1, 2, 3, 4},
			args: args{info: raftio.NodeInfo{
				ShardID:   2,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(2))
				require.False(t, ok, "unexpected cache shard")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LogReader{}
			for _, shard := range tt.initialShards {
				l.getShard(shard)
			}
			l.NodeDeleted(tt.args.info)
			tt.assert(t, &l.shardCache)
		})
	}
}

func TestLogReader_NodeReady(t *testing.T) {
	type args struct {
		info raftio.NodeInfo
	}
	tests := []struct {
		name          string
		args          args
		initialShards []uint64
		assert        func(*testing.T, *util.SyncMap[uint64, *shard])
	}{
		{
			name: "add ready node",
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(1))
				require.True(t, ok, "missing cache shard")
			},
		},
		{
			name: "add existing node",
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			initialShards: []uint64{1},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(1))
				require.True(t, ok, "missing cache shard")
			},
		},
		{
			name:          "add ready node to list",
			initialShards: []uint64{1, 3, 5, 6},
			args: args{info: raftio.NodeInfo{
				ShardID:   2,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				_, ok := s.Load(uint64(2))
				require.True(t, ok, "missing cache shard")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LogReader{}
			for _, shard := range tt.initialShards {
				l.getShard(shard)
			}
			l.NodeReady(tt.args.info)
			tt.assert(t, &l.shardCache)
		})
	}
}

func TestLogReader_QueryRaftLog(t *testing.T) {
	type fields struct {
		ShardCacheSize int
	}
	type args struct {
		clusterID uint64
		logRange  dragonboat.LogRange
		maxSize   uint64
		timeout   time.Duration
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		cacheContent []raftpb.Entry
		on           func(*mockLogQuerier)
		assert       func(*testing.T, *mockLogQuerier)
		want         []raftpb.Entry
		wantErr      require.ErrorAssertionFunc
	}{
		{
			name: "cache size zero valid args",
			fields: fields{
				ShardCacheSize: 0,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: dragonboat.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1000,
			},
			on: func(querier *mockLogQuerier) {
				results := make(chan dragonboat.RequestResult, 1)
				results <- dragonboat.RequestResult{}
				querier.On("QueryRaftLog", uint64(1), uint64(100), uint64(1000), uint64(1000)).
					Return(&dragonboat.RequestState{CompletedC: results}, nil)
			},
			wantErr: require.Error,
		},
		{
			name: "cache size zero invalid logRange",
			fields: fields{
				ShardCacheSize: 0,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: dragonboat.LogRange{
					FirstIndex: 0,
					LastIndex:  1000,
				},
				maxSize: 1000,
			},
			assert: func(t *testing.T, querier *mockLogQuerier) {
				querier.AssertNotCalled(t, "QueryRaftLog")
			},
			wantErr: require.NoError,
		},
		{
			name: "cache returned values in the middle",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: dragonboat.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1000,
			},
			cacheContent: createEntries(500, 600),
			on: func(querier *mockLogQuerier) {
				results := make(chan dragonboat.RequestResult, 1)
				results <- dragonboat.RequestResult{}
				querier.On("QueryRaftLog", uint64(1), uint64(100), uint64(500), mock.Anything).
					Return(&dragonboat.RequestState{CompletedC: results}, nil)
			},
			wantErr: require.Error,
		},
		{
			name:    "empty log range",
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			querier := &mockLogQuerier{}
			if tt.on != nil {
				tt.on(querier)
			}
			l := &LogReader{
				ShardCacheSize: tt.fields.ShardCacheSize,
				LogQuerier:     querier,
			}
			if len(tt.cacheContent) > 0 {
				l.getShard(tt.args.clusterID).put(tt.cacheContent)
			}
			ctx, cancel := context.WithTimeout(context.TODO(), tt.args.timeout)
			got, err := l.QueryRaftLog(ctx, tt.args.clusterID, tt.args.logRange, tt.args.maxSize)
			cancel()
			tt.wantErr(t, err)
			if tt.assert != nil {
				tt.assert(t, querier)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
