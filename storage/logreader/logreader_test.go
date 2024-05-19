// Copyright JAMF Software, LLC

package logreader

import (
	"context"
	"testing"
	"time"

	"github.com/jamf/regatta/raft"
	"github.com/jamf/regatta/raft/raftio"
	"github.com/jamf/regatta/raft/raftpb"
	serror "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/util"
	"github.com/jamf/regatta/util/iter"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockLogQuerier struct {
	mock.Mock
}

func (m *mockLogQuerier) GetLogReader(shardID uint64) (raft.ReadonlyLogReader, error) {
	args := m.Called(shardID)
	return args.Get(0).(raft.ReadonlyLogReader), args.Error(1)
}

type mockLogReader struct {
	mock.Mock
}

func (m *mockLogReader) GetRange() (uint64, uint64) {
	args := m.Called()
	return uint64(args.Int(0)), uint64(args.Int(1))
}

func (m *mockLogReader) NodeState() (raftpb.State, raftpb.Membership) {
	args := m.Called()
	return args.Get(0).(raftpb.State), args.Get(1).(raftpb.Membership)
}

func (m *mockLogReader) Term(index uint64) (uint64, error) {
	args := m.Called(index)
	return uint64(args.Int(0)), args.Error(1)
}

func (m *mockLogReader) Entries(low uint64, high uint64, maxSize uint64) ([]raftpb.Entry, error) {
	args := m.Called(low, high, maxSize)
	return args.Get(0).([]raftpb.Entry), args.Error(1)
}

func (m *mockLogReader) Snapshot() raftpb.Snapshot {
	args := m.Called()
	return args.Get(0).(raftpb.Snapshot)
}

func TestCached_NodeDeleted(t *testing.T) {
	type args struct {
		info raftio.NodeInfo
	}
	type fields struct {
		ShardCache *ShardCache
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		assert func(*testing.T, *util.SyncMap[uint64, *shard])
	}{
		{
			name:   "remove existing cache shard",
			fields: fields{ShardCache: NewShardCache(1)},
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				require.False(t, iter.Contains(s.Keys(), uint64(1)), "unexpected cache shard")
			},
		},
		{
			name:   "remove non-existent cache shard",
			fields: fields{ShardCache: NewShardCache(1)},
			args: args{info: raftio.NodeInfo{
				ShardID:   1,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				require.False(t, iter.Contains(s.Keys(), uint64(1)), "unexpected cache shard")
			},
		},
		{
			name: "remove existent cache shard from list",
			fields: fields{ShardCache: func() *ShardCache {
				c := NewShardCache(100)
				for i := 1; i <= 4; i++ {
					c.shardCache.Store(uint64(i), &shard{})
				}
				return c
			}()},
			args: args{info: raftio.NodeInfo{
				ShardID:   2,
				ReplicaID: 1,
			}},
			assert: func(t *testing.T, s *util.SyncMap[uint64, *shard]) {
				require.False(t, iter.Contains(s.Keys(), uint64(2)), "unexpected cache shard")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Cached{ShardCache: tt.fields.ShardCache}
			l.NodeDeleted(tt.args.info.ShardID)
			tt.assert(t, l.shardCache)
		})
	}
}

func TestCached_QueryRaftLog(t *testing.T) {
	type fields struct {
		ShardCacheSize int
	}
	type args struct {
		clusterID uint64
		logRange  raft.LogRange
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
				logRange: raft.LogRange{
					FirstIndex: 1,
					LastIndex:  1000,
				},
				maxSize: 1000,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5)
				lr.On("Entries", mock.Anything, mock.Anything, mock.Anything).Return([]raftpb.Entry{}, nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
		},
		{
			name: "cache size zero invalid logRange",
			fields: fields{
				ShardCacheSize: 0,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
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
			name: "empty cache valid logRange",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(100), uint64(1000), mock.Anything).Return(createEntries(100, 999), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(100, 999),
		},
		{
			name: "cache hit middle of the range prepend range",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1024 * 1024,
			},
			cacheContent: createEntries(500, 600),
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(100), uint64(500), mock.Anything).Return(createEntries(100, 499), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(100, 600),
		},
		{
			name: "cache hit middle of the range prepend range and limit size",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1024,
			},
			cacheContent: createEntries(500, 600),
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(100), uint64(500), mock.Anything).Return(createEntries(100, 499), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(100, 106),
		},
		{
			name: "cache hit beginning of the range append range",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1024 * 1024,
			},
			cacheContent: createEntries(100, 500),
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(501), uint64(1000), mock.Anything).Return(createEntries(501, 999), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(100, 999),
		},
		{
			name: "cache hit beginning of the range no new entries",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 100,
					LastIndex:  1000,
				},
				maxSize: 1024 * 1024,
			},
			cacheContent: createEntries(100, 500),
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(501), uint64(1000), mock.Anything).Return([]raftpb.Entry{}, nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(100, 500),
		},
		{
			name: "cache miss append range",
			fields: fields{
				ShardCacheSize: 1000,
			},
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 501,
					LastIndex:  1000,
				},
				maxSize: 1024 * 1024,
			},
			cacheContent: createEntries(100, 500),
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(501), uint64(1000), mock.Anything).Return(createEntries(501, 999), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
			want:    createEntries(501, 999),
		},
		{
			name:    "empty log range",
			wantErr: require.NoError,
		},
		{
			name: "up to date",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 5001,
					LastIndex:  6000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
		},
		{
			name: "single record",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 5000,
					LastIndex:  5001,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(5000), uint64(5001), mock.Anything).Return(createEntries(5000, 5001), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			want:    createEntries(5000, 5001),
			wantErr: require.NoError,
		},
		{
			name: "error log ahead",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 1,
					LastIndex:  5000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1000, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, serror.ErrLogAhead)
			},
		},
		{
			name: "error log behind",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 6000,
					LastIndex:  7000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1000, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, serror.ErrLogBehind)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			querier := &mockLogQuerier{}
			if tt.on != nil {
				tt.on(querier)
			}
			l := &Cached{
				ShardCache: NewShardCache(tt.fields.ShardCacheSize),
				LogQuerier: querier,
			}
			if len(tt.cacheContent) > 0 {
				v, _ := l.shardCache.
					Load(tt.args.clusterID)
				v.put(tt.cacheContent)
			}
			ctx, cancel := context.WithTimeout(context.TODO(), tt.args.timeout)
			defer cancel()
			got, err := l.QueryRaftLog(ctx, tt.args.clusterID, tt.args.logRange, tt.args.maxSize)
			tt.wantErr(t, err)
			if tt.assert != nil {
				tt.assert(t, querier)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSimple_QueryRaftLog(t *testing.T) {
	type args struct {
		clusterID uint64
		logRange  raft.LogRange
		maxSize   uint64
		timeout   time.Duration
	}
	tests := []struct {
		name    string
		args    args
		on      func(*mockLogQuerier)
		assert  func(*testing.T, *mockLogQuerier)
		want    []raftpb.Entry
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "empty log range",
			wantErr: require.NoError,
		},
		{
			name: "up to date",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 5001,
					LastIndex:  6000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: require.NoError,
		},
		{
			name: "single record",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 5000,
					LastIndex:  5001,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1, 5000)
				lr.On("Entries", uint64(5000), uint64(5001), mock.Anything).Return(createEntries(5000, 5001), nil)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			want:    createEntries(5000, 5001),
			wantErr: require.NoError,
		},
		{
			name: "error log ahead",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 1,
					LastIndex:  5000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1000, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, serror.ErrLogAhead)
			},
		},
		{
			name: "error log behind",
			args: args{
				timeout:   30 * time.Second,
				clusterID: 1,
				logRange: raft.LogRange{
					FirstIndex: 6000,
					LastIndex:  7000,
				},
				maxSize: 1024 * 1024,
			},
			on: func(querier *mockLogQuerier) {
				lr := &mockLogReader{}
				lr.On("GetRange").Return(1000, 5000)
				querier.On("GetLogReader", uint64(1)).Return(lr, nil)
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, serror.ErrLogBehind)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			querier := &mockLogQuerier{}
			if tt.on != nil {
				tt.on(querier)
			}
			l := &Simple{LogQuerier: querier}
			ctx, cancel := context.WithTimeout(context.TODO(), tt.args.timeout)
			defer cancel()
			got, err := l.QueryRaftLog(ctx, tt.args.clusterID, tt.args.logRange, tt.args.maxSize)
			tt.wantErr(t, err)
			if tt.assert != nil {
				tt.assert(t, querier)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
