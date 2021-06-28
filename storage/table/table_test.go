package table

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/client"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage"
	"github.com/wandera/regatta/util"
)

var (
	longKey    = []byte(util.RandString(1025))
	longValue  = []byte(util.RandString(1024 * 1024 * 3))
	errUnknown = errors.New("unknown error")
)

type mockRaftHandler struct {
	queryResult    interface{}
	proposalResult sm.Result
	proposalError  error
}

func (m mockRaftHandler) SyncRead(ctx context.Context, id uint64, req interface{}) (interface{}, error) {
	return m.queryResult, m.proposalError
}

func (m mockRaftHandler) StaleRead(id uint64, req interface{}) (interface{}, error) {
	return m.queryResult, m.proposalError
}

func (m mockRaftHandler) SyncPropose(ctx context.Context, session *client.Session, bytes []byte) (sm.Result, error) {
	return m.proposalResult, m.proposalError
}

func (m mockRaftHandler) GetNoOPSession(id uint64) *client.Session {
	return &client.Session{}
}

func TestActiveTable_Range(t *testing.T) {
	type fields struct {
		Table Table
		nh    raftHandler
	}
	type args struct {
		ctx context.Context
		req *proto.RangeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.RangeResponse
		wantErr error
	}{
		{
			name: "Query unknown key",
			fields: fields{
				Table: Table{},
				nh: mockRaftHandler{
					proposalError: pebble.ErrNotFound,
				},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("missing")},
			},
			wantErr: storage.ErrNotFound,
		},
		{
			name: "Query key found",
			fields: fields{
				Table: Table{},
				nh: mockRaftHandler{
					queryResult: &proto.RangeResponse{Count: 1, Kvs: []*proto.KeyValue{{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					}}},
				},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo")},
			},
			want: &proto.RangeResponse{Count: 1, Kvs: []*proto.KeyValue{{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			}}},
		},
		{
			name: "Query key found - linerizable",
			fields: fields{
				Table: Table{},
				nh: mockRaftHandler{
					queryResult: &proto.RangeResponse{Count: 1, Kvs: []*proto.KeyValue{{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					}}},
				},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo"), Linearizable: true},
			},
			want: &proto.RangeResponse{Count: 1, Kvs: []*proto.KeyValue{{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			}}},
		},
		{
			name: "Query unknown error",
			fields: fields{
				Table: Table{},
				nh: mockRaftHandler{
					proposalError: errUnknown,
				},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo")},
			},
			wantErr: errUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			at := &ActiveTable{
				Table: tt.fields.Table,
				nh:    tt.fields.nh,
			}
			got, err := at.Range(tt.args.ctx, tt.args.req)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestActiveTable_Hash(t *testing.T) {
	type fields struct {
		Table Table
		nh    raftHandler
	}
	type args struct {
		ctx context.Context
		req *proto.HashRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.HashResponse
		wantErr bool
	}{
		{
			name:   "Hash store",
			fields: fields{nh: mockRaftHandler{queryResult: &proto.HashResponse{Hash: 1234}}},
			args: args{
				ctx: context.TODO(),
				req: &proto.HashRequest{},
			},
			want: &proto.HashResponse{Hash: 1234},
		},
		{
			name:   "Unknown error",
			fields: fields{nh: mockRaftHandler{proposalError: errUnknown}},
			args: args{
				ctx: context.TODO(),
				req: &proto.HashRequest{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			at := &ActiveTable{
				Table: tt.fields.Table,
				nh:    tt.fields.nh,
			}
			got, err := at.Hash(tt.args.ctx, tt.args.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestActiveTable_Put(t *testing.T) {
	type fields struct {
		Table Table
		nh    raftHandler
	}
	type args struct {
		ctx context.Context
		req *proto.PutRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.PutResponse
		wantErr error
	}{
		{
			name: "Put KV success",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{proposalResult: sm.Result{}},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			want: &proto.PutResponse{},
		},
		{
			name: "Put KV empty key",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte(""),
					Value: []byte("bar"),
				},
			},
			wantErr: storage.ErrEmptyKey,
		},
		{
			name: "Put KV key too long",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   longKey,
					Value: []byte("bar"),
				},
			},
			wantErr: storage.ErrKeyLengthExceeded,
		},
		{
			name: "Put KV value too long",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("foo"),
					Value: longValue,
				},
			},
			wantErr: storage.ErrValueLengthExceeded,
		},
		{
			name: "Put KV unknown error",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{proposalError: errUnknown},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			wantErr: errUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			at := &ActiveTable{
				Table: tt.fields.Table,
				nh:    tt.fields.nh,
			}
			got, err := at.Put(tt.args.ctx, tt.args.req)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestActiveTable_Delete(t *testing.T) {
	type fields struct {
		Table Table
		nh    raftHandler
	}
	type args struct {
		ctx context.Context
		req *proto.DeleteRangeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.DeleteRangeResponse
		wantErr error
	}{
		{
			name: "Delete with empty key",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{},
			},
			wantErr: storage.ErrEmptyKey,
		},
		{
			name: "Delete existing key",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{proposalResult: sm.Result{Value: 1}},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: []byte("foo")},
			},
			want: &proto.DeleteRangeResponse{Deleted: 1},
		},
		{
			name: "Delete non-existent key",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{proposalResult: sm.Result{Value: 0}},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: []byte("foo")},
			},
			want: &proto.DeleteRangeResponse{Deleted: 0},
		},
		{
			name: "Delete key too long",
			fields: fields{
				Table: Table{},
				nh:    mockRaftHandler{},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: longKey},
			},
			wantErr: storage.ErrKeyLengthExceeded,
		},
		{
			name: "Delete unknown error",
			fields: fields{
				Table: Table{},
				nh: mockRaftHandler{
					proposalError: errUnknown,
				},
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: []byte("foo")},
			},
			wantErr: errUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			at := &ActiveTable{
				Table: tt.fields.Table,
				nh:    tt.fields.nh,
			}
			got, err := at.Delete(tt.args.ctx, tt.args.req)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestTable_AsActive(t *testing.T) {
	type fields struct {
		Name      string
		ClusterID uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   ActiveTable
	}{
		{
			name: "Fields are copied",
			fields: fields{
				Name:      "Name",
				ClusterID: 10000,
			},
			want: ActiveTable{Table: Table{
				Name:      "Name",
				ClusterID: 10000,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			tab := Table{
				Name:      tt.fields.Name,
				ClusterID: tt.fields.ClusterID,
			}
			r.Equal(tt.want, tab.AsActive(nil))
		})
	}
}
