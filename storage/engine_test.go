// Copyright JAMF Software, LLC

package storage

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamf/regatta/pebble"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/cluster"
	"github.com/jamf/regatta/storage/kv"
	"github.com/jamf/regatta/storage/logreader"
	"github.com/jamf/regatta/storage/table"
	"github.com/jamf/regatta/util/iter"
	"github.com/jamf/regatta/version"
	lvfs "github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const testTableName = "table"

func TestEngine_Start(t *testing.T) {
	type fields struct {
		cfg Config
	}
	tests := []struct {
		name        string
		fields      fields
		wantStarted bool
		wantErr     require.ErrorAssertionFunc
	}{
		{
			name:        "start test node",
			fields:      fields{cfg: newTestConfig()},
			wantStarted: true,
			wantErr:     require.NoError,
		},
		{
			name: "start test node pebble logdb",
			fields: fields{cfg: func() Config {
				cfg := newTestConfig()
				cfg.LogDBImplementation = Pebble
				return cfg
			}()},
			wantStarted: true,
			wantErr:     require.NoError,
		},
		{
			name: "start test node checkpoint snapshot",
			fields: fields{cfg: func() Config {
				cfg := newTestConfig()
				cfg.Table.RecoveryType = table.RecoveryTypeCheckpoint
				return cfg
			}()},
			wantStarted: true,
			wantErr:     require.NoError,
		},
		{
			name: "start test node snapshot snapshot",
			fields: fields{cfg: func() Config {
				cfg := newTestConfig()
				cfg.Table.RecoveryType = table.RecoveryTypeSnapshot
				return cfg
			}()},
			wantStarted: true,
			wantErr:     require.NoError,
		},
		{
			name: "start tmp fs",
			fields: fields{cfg: func() Config {
				raftPort := getTestPort()
				gossipPort := getTestPort()
				tmpdir := t.TempDir()
				return Config{
					NodeID:         1,
					InitialMembers: map[uint64]string{1: fmt.Sprintf("127.0.0.1:%d", raftPort)},
					NodeHostDir:    filepath.Join(tmpdir, "nh"),
					RTTMillisecond: 5,
					RaftAddress:    fmt.Sprintf("127.0.0.1:%d", raftPort),
					Gossip:         GossipConfig{BindAddress: fmt.Sprintf("127.0.0.1:%d", gossipPort)},
					Table:          TableConfig{DataDir: filepath.Join(tmpdir, "data"), TableCacheSize: 1024, ElectionRTT: 10, HeartbeatRTT: 1},
					Meta:           MetaConfig{ElectionRTT: 10, HeartbeatRTT: 1},
				}
			}()},
			wantStarted: true,
			wantErr:     require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, tt.fields.cfg)
			tt.wantErr(t, e.Start())
			if tt.wantStarted {
				require.NoError(t, e.WaitUntilReady(ctx))
			}
		})
	}
}

func TestEngine_Range(t *testing.T) {
	type args struct {
		ctx context.Context
		req *regattapb.RangeRequest
	}
	tests := []struct {
		name    string
		args    args
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.RangeResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not found",
			prepare: func(t *testing.T, e *Engine) {},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "key not found serializable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "key not found linearizable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table:        []byte(testTableName),
					Key:          []byte("key"),
					Linearizable: true,
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "key found serializable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
				Kvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
				Count: 1,
			},
			wantErr: require.NoError,
		},
		{
			name: "key found linearizable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
				Kvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
				Count: 1,
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.Range(tt.args.ctx, tt.args.req)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEngine_IterateRange(t *testing.T) {
	type args struct {
		ctx context.Context
		req *regattapb.RangeRequest
	}
	tests := []struct {
		name    string
		args    args
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.RangeResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not found",
			prepare: func(t *testing.T, e *Engine) {},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "key not found serializable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "key not found linearizable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table:        []byte(testTableName),
					Key:          []byte("key"),
					Linearizable: true,
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "key found serializable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
				Kvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
				Count: 1,
			},
			wantErr: require.NoError,
		},
		{
			name: "key found range request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table:    []byte(testTableName),
					Key:      []byte("key"),
					RangeEnd: []byte("key_"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
				Kvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
				Count: 1,
			},
			wantErr: require.NoError,
		},
		{
			name: "key found linearizable request",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.RangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			want: &regattapb.RangeResponse{
				Header: &regattapb.ResponseHeader{
					ReplicaId: 1,
					ShardId:   10001,
				},
				Kvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
				Count: 1,
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.IterateRange(tt.args.ctx, tt.args.req)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, iter.First(got))
		})
	}
}

func TestEngine_Put(t *testing.T) {
	type args struct {
		ctx context.Context
		req *regattapb.PutRequest
	}
	tests := []struct {
		name    string
		args    args
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.PutResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not found",
			prepare: func(t *testing.T, e *Engine) {},
			args: args{
				ctx: context.Background(),
				req: &regattapb.PutRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "put new key",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.PutRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			want: &regattapb.PutResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  3,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "overwrite key",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.PutRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
					Value: []byte("value2"),
				},
			},
			want: &regattapb.PutResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "overwrite key and get prev",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.PutRequest{
					Table:  []byte(testTableName),
					Key:    []byte("key"),
					Value:  []byte("value2"),
					PrevKv: true,
				},
			},
			want: &regattapb.PutResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				PrevKv: &regattapb.KeyValue{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.Put(tt.args.ctx, tt.args.req)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEngine_Delete(t *testing.T) {
	type args struct {
		ctx context.Context
		req *regattapb.DeleteRangeRequest
	}
	tests := []struct {
		name    string
		args    args
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.DeleteRangeResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not found",
			prepare: func(t *testing.T, e *Engine) {},
			args: args{
				ctx: context.Background(),
				req: &regattapb.DeleteRangeRequest{
					Table: []byte(testTableName),
					Key:   []byte("key"),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "delete all",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.DeleteRangeRequest{
					Table:    []byte(testTableName),
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
			},
			want: &regattapb.DeleteRangeResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  3,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "delete all and count",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.DeleteRangeRequest{
					Table:    []byte(testTableName),
					Key:      []byte{0},
					RangeEnd: []byte{0},
					Count:    true,
				},
			},
			want: &regattapb.DeleteRangeResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				Deleted: 1,
			},
			wantErr: require.NoError,
		},
		{
			name: "delete all and get prev",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.DeleteRangeRequest{
					Table:    []byte(testTableName),
					Key:      []byte{0},
					RangeEnd: []byte{0},
					PrevKv:   true,
				},
			},
			want: &regattapb.DeleteRangeResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				Deleted: 1,
				PrevKvs: []*regattapb.KeyValue{
					{Key: []byte("key"), Value: []byte("value")},
				},
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.Delete(tt.args.ctx, tt.args.req)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEngine_Txn(t *testing.T) {
	type args struct {
		ctx context.Context
		req *regattapb.TxnRequest
	}
	tests := []struct {
		name    string
		args    args
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.TxnResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "table not found",
			prepare: func(t *testing.T, e *Engine) {},
			args: args{
				ctx: context.Background(),
				req: &regattapb.TxnRequest{
					Table: []byte(testTableName),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "put new key no comp",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.TxnRequest{
					Table: []byte(testTableName),
					Success: []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{
						RequestPut: &regattapb.RequestOp_Put{
							Key:   []byte("key2"),
							Value: []byte("value2"),
						},
					}}},
				},
			},
			want: &regattapb.TxnResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				Succeeded: true,
				Responses: []*regattapb.ResponseOp{{Response: &regattapb.ResponseOp_ResponsePut{
					ResponsePut: &regattapb.ResponseOp_Put{},
				}}},
			},
			wantErr: require.NoError,
		},
		{
			name: "put new key success comp",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.TxnRequest{
					Table: []byte(testTableName),
					Compare: []*regattapb.Compare{{
						Result:      regattapb.Compare_EQUAL,
						Target:      regattapb.Compare_VALUE,
						Key:         []byte("key"),
						TargetUnion: &regattapb.Compare_Value{Value: []byte("value")},
					}},
					Success: []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{
						RequestPut: &regattapb.RequestOp_Put{
							Key:   []byte("key2"),
							Value: []byte("value2"),
						},
					}}},
				},
			},
			want: &regattapb.TxnResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				Succeeded: true,
				Responses: []*regattapb.ResponseOp{{Response: &regattapb.ResponseOp_ResponsePut{
					ResponsePut: &regattapb.ResponseOp_Put{},
				}}},
			},
			wantErr: require.NoError,
		},
		{
			name: "put new key failed cond",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
				_, err := e.Put(context.Background(), &regattapb.PutRequest{Table: []byte(testTableName), Key: []byte("key"), Value: []byte("value")})
				require.NoError(t, err)
			},
			args: args{
				ctx: context.Background(),
				req: &regattapb.TxnRequest{
					Table: []byte(testTableName),
					Compare: []*regattapb.Compare{{
						Result:      regattapb.Compare_EQUAL,
						Target:      regattapb.Compare_VALUE,
						Key:         []byte("key"),
						TargetUnion: &regattapb.Compare_Value{Value: []byte("foo")},
					}},
					Success: []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{
						RequestPut: &regattapb.RequestOp_Put{
							Key:   []byte("key2"),
							Value: []byte("value2"),
						},
					}}},
					Failure: []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{
						RequestPut: &regattapb.RequestOp_Put{
							Key:    []byte("key"),
							Value:  []byte("value2"),
							PrevKv: true,
						},
					}}},
				},
			},
			want: &regattapb.TxnResponse{
				Header: &regattapb.ResponseHeader{
					ShardId:   10001,
					ReplicaId: 1,
					Revision:  4,
				},
				Succeeded: false,
				Responses: []*regattapb.ResponseOp{{Response: &regattapb.ResponseOp_ResponsePut{
					ResponsePut: &regattapb.ResponseOp_Put{
						PrevKv: &regattapb.KeyValue{Key: []byte("key"), Value: []byte("value")},
					},
				}}},
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.Txn(tt.args.ctx, tt.args.req)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEngine_Status(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, e *Engine)
		want    *regattapb.StatusResponse
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "no tables",
			prepare: func(t *testing.T, e *Engine) {},
			wantErr: require.NoError,
			want: &regattapb.StatusResponse{
				Id:      "1",
				Version: version.Version,
				Tables:  make(map[string]*regattapb.TableStatus),
			},
		},
		{
			name: "with single table",
			prepare: func(t *testing.T, e *Engine) {
				createTable(t, e)
			},
			wantErr: require.NoError,
			want: &regattapb.StatusResponse{
				Id:      "1",
				Version: version.Version,
				Tables: map[string]*regattapb.TableStatus{
					testTableName: {
						Leader:   "1",
						RaftTerm: 2,
					},
				},
			},
		},
		{
			name: "engine error",
			prepare: func(t *testing.T, e *Engine) {
				e.Close()
			},
			wantErr: require.NoError,
			want: &regattapb.StatusResponse{
				Id:      "1",
				Version: version.Version,
				Tables:  make(map[string]*regattapb.TableStatus),
				Errors: []string{
					"dragonboat: closed",
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.Status(context.Background(), &regattapb.StatusRequest{})
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEngine_MemberList(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, e *Engine)
		assert  func(t *testing.T, resp *regattapb.MemberListResponse)
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "no tables",
			prepare: func(t *testing.T, e *Engine) {},
			wantErr: require.NoError,
			assert: func(t *testing.T, resp *regattapb.MemberListResponse) {
				require.Len(t, resp.Members, 1)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := cancellableTestContext(t)
			e := newTestEngine(t, newTestConfig())
			require.NoError(t, e.Start())
			require.NoError(t, e.WaitUntilReady(ctx))
			tt.prepare(t, e)
			got, err := e.MemberList(context.Background(), &regattapb.MemberListRequest{})
			tt.wantErr(t, err)
			tt.assert(t, got)
		})
	}
}

func TestNew(t *testing.T) {
	type args struct {
		cfg Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "basic",
			args:    args{cfg: newTestConfig()},
			wantErr: require.NoError,
		},
		{
			name: "missing gossip config",
			args: args{cfg: func() Config {
				cfg := newTestConfig()
				cfg.Gossip = GossipConfig{}
				return cfg
			}()},
			wantErr: require.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := New(tt.args.cfg)
			tt.wantErr(t, err)
			if e != nil {
				_ = e.Close()
			}
		})
	}
}

func createTable(t *testing.T, e *Engine) {
	tab, err := e.CreateTable(testTableName)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		active := tab.AsActive(e)
		_, err := active.LocalIndex(c, true)
		return err == nil
	}, 1*time.Second, 10*time.Millisecond)
}

func newTestConfig() Config {
	fs := lvfs.NewMem()
	raftPort := getTestPort()
	gossipPort := getTestPort()
	return Config{
		NodeID:         1,
		InitialMembers: map[uint64]string{1: fmt.Sprintf("127.0.0.1:%d", raftPort)},
		WALDir:         "/wal",
		NodeHostDir:    "/nh",
		RTTMillisecond: 5,
		RaftAddress:    fmt.Sprintf("127.0.0.1:%d", raftPort),
		Gossip:         GossipConfig{BindAddress: fmt.Sprintf("127.0.0.1:%d", gossipPort), InitialMembers: []string{fmt.Sprintf("127.0.0.1:%d", gossipPort)}},
		Table:          TableConfig{FS: pebble.NewPebbleFS(fs), TableCacheSize: 1024, ElectionRTT: 10, HeartbeatRTT: 1},
		Meta:           MetaConfig{ElectionRTT: 10, HeartbeatRTT: 1},
		FS:             fs,
	}
}

func newTestEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e := &Engine{cfg: cfg, stop: make(chan struct{}), log: zaptest.NewLogger(t).Sugar()}
	e.events = &events{eventsCh: make(chan any, 1), stopc: make(chan struct{}), engine: e}
	nh, err := createNodeHost(e)
	require.NoError(t, err)
	e.NodeHost = nh
	e.LogReader = &logreader.Cached{LogQuerier: nh}
	e.Cluster, err = cluster.New(cfg.Gossip.BindAddress, cfg.Gossip.AdvertiseAddress, "", "", func() cluster.Info {
		return cluster.Info{}
	})
	require.NoError(t, err)
	e.tableStore = &kv.RaftStore{
		NodeHost:  nh,
		ClusterID: tableStoreID,
	}
	e.Manager = table.NewManager(nh, cfg.InitialMembers, e.tableStore, table.Config{
		NodeID: cfg.NodeID,
		Table:  table.TableConfig(cfg.Table),
		Meta:   table.MetaConfig(cfg.Meta),
	})
	t.Cleanup(func() {
		defer func() { recover() }()
		require.NoError(t, e.Close())
	})
	return e
}

func getTestPort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func cancellableTestContext(t *testing.T) context.Context {
	if dln, ok := t.Deadline(); ok {
		ctx, cancel := context.WithDeadline(context.Background(), dln)
		t.Cleanup(cancel)
		return ctx
	}
	return context.Background()
}
