// Copyright JAMF Software, LLC

package table

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/log"
	"github.com/jamf/regatta/proto"
	serrors "github.com/jamf/regatta/storage/errors"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/jamf/regatta/util"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

func init() {
	logger.SetLoggerFactory(log.LoggerFactory(zap.NewNop()))
}

var (
	longKey    = []byte(util.RandString(key.LatestVersionLen + 1))
	longValue  = []byte(util.RandString(MaxValueLen + 1))
	errUnknown = errors.New("unknown error")
)

type mockRaftHandler struct {
	mock.Mock
}

func (m *mockRaftHandler) SyncRead(ctx context.Context, id uint64, req interface{}) (interface{}, error) {
	args := m.Called(ctx, id, req)
	return args.Get(0), args.Error(1)
}

func (m *mockRaftHandler) StaleRead(id uint64, req interface{}) (interface{}, error) {
	args := m.Called(id, req)
	return args.Get(0), args.Error(1)
}

func (m *mockRaftHandler) SyncPropose(ctx context.Context, session *client.Session, bytes []byte) (sm.Result, error) {
	args := m.Called(ctx, session, bytes)
	return args.Get(0).(sm.Result), args.Error(1)
}

func (m *mockRaftHandler) GetNoOPSession(id uint64) *client.Session {
	return &client.Session{}
}

func TestActiveTable_Range(t *testing.T) {
	type args struct {
		ctx context.Context
		req *proto.RangeRequest
	}
	tests := []struct {
		name    string
		on      func(*mockRaftHandler)
		assert  func(*mockRaftHandler)
		args    args
		want    *proto.RangeResponse
		wantErr error
	}{
		{
			name: "Query unknown key",
			on: func(handler *mockRaftHandler) {
				handler.
					On("StaleRead", mock.Anything, mock.Anything).
					Return(nil, pebble.ErrNotFound)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("missing")},
			},
			wantErr: serrors.ErrKeyNotFound,
		},
		{
			name: "Query key found",
			on: func(handler *mockRaftHandler) {
				handler.
					On("StaleRead", mock.Anything, mock.Anything).
					Return(&proto.ResponseOp_Range{
						Kvs: []*proto.KeyValue{
							{
								Key:   []byte("foo"),
								Value: []byte("bar"),
							},
						},
						Count: 1,
					}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo")},
			},
			want: &proto.RangeResponse{
				Count: 1,
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
			},
		},
		{
			name: "Query key found - linerizable",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncRead", mock.Anything, mock.Anything, mock.Anything).
					Return(&proto.ResponseOp_Range{
						Kvs: []*proto.KeyValue{
							{
								Key:   []byte("foo"),
								Value: []byte("bar"),
							},
						},
						Count: 1,
					}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo"), Linearizable: true},
			},
			want: &proto.RangeResponse{
				Count: 1,
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
			},
		},
		{
			name: "Query key too long",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: longKey},
			},
			wantErr: serrors.ErrKeyLengthExceeded,
		},
		{
			name: "Query range end too long",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{Key: []byte("foo"), RangeEnd: longKey},
			},
			wantErr: serrors.ErrKeyLengthExceeded,
		},
		{
			name: "Query unknown error",
			on: func(handler *mockRaftHandler) {
				handler.
					On("StaleRead", mock.Anything, mock.Anything).
					Return(nil, errUnknown)
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
			nh := &mockRaftHandler{}
			if tt.on != nil {
				tt.on(nh)
			}
			if tt.assert != nil {
				tt.assert(nh)
			}
			at := &ActiveTable{
				Table: Table{},
				nh:    nh,
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

func TestActiveTable_Put(t *testing.T) {
	type args struct {
		ctx context.Context
		req *proto.PutRequest
	}
	tests := []struct {
		name    string
		on      func(*mockRaftHandler)
		assert  func(*mockRaftHandler)
		args    args
		want    *proto.PutResponse
		wantErr error
	}{
		{
			name: "Put KV success",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mustMarshallProto(&proto.Command{
						Type: proto.Command_PUT,
						Kv:   &proto.KeyValue{Key: []byte("foo"), Value: []byte("bar")},
					})).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{}},
					}}})}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			want: &proto.PutResponse{Header: &proto.ResponseHeader{}},
		},
		{
			name: "Put KV with prev",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mustMarshallProto(&proto.Command{
						Type:    proto.Command_PUT,
						Kv:      &proto.KeyValue{Key: []byte("foo"), Value: []byte("bar")},
						PrevKvs: true,
					})).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponsePut{ResponsePut: &proto.ResponseOp_Put{PrevKv: &proto.KeyValue{Key: []byte("foo"), Value: []byte("val")}}},
					}}})}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:    []byte("foo"),
					Value:  []byte("bar"),
					PrevKv: true,
				},
			},
			want: &proto.PutResponse{PrevKv: &proto.KeyValue{Key: []byte("foo"), Value: []byte("val")}, Header: &proto.ResponseHeader{}},
		},
		{
			name: "Put KV empty key",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte(""),
					Value: []byte("bar"),
				},
			},
			wantErr: serrors.ErrEmptyKey,
		},
		{
			name: "Put KV key too long",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   longKey,
					Value: []byte("bar"),
				},
			},
			wantErr: serrors.ErrKeyLengthExceeded,
		},
		{
			name: "Put KV value too long",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("foo"),
					Value: longValue,
				},
			},
			wantErr: serrors.ErrValueLengthExceeded,
		},
		{
			name: "Put KV unknown error",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mock.Anything).
					Return(sm.Result{}, errUnknown)
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
			nh := &mockRaftHandler{}
			if tt.on != nil {
				tt.on(nh)
			}
			if tt.assert != nil {
				tt.assert(nh)
			}
			at := &ActiveTable{
				Table: Table{},
				nh:    nh,
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
	type args struct {
		ctx context.Context
		req *proto.DeleteRangeRequest
	}
	tests := []struct {
		name    string
		on      func(*mockRaftHandler)
		assert  func(*mockRaftHandler)
		args    args
		want    *proto.DeleteRangeResponse
		wantErr error
	}{
		{
			name: "Delete with empty key",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{},
			},
			wantErr: serrors.ErrEmptyKey,
		},
		{
			name: "Delete existing key",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mustMarshallProto(&proto.Command{
						Type: proto.Command_DELETE,
						Kv:   &proto.KeyValue{Key: []byte("foo")},
					})).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{Deleted: 1}},
					}}})}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: []byte("foo")},
			},
			want: &proto.DeleteRangeResponse{Deleted: 1, Header: &proto.ResponseHeader{}},
		},
		{
			name: "Delete existing key with prev",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mustMarshallProto(&proto.Command{
						Type:    proto.Command_DELETE,
						Kv:      &proto.KeyValue{Key: []byte("foo")},
						PrevKvs: true,
					})).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{PrevKvs: []*proto.KeyValue{{Key: []byte("foo"), Value: []byte("val")}}}},
					}}})}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Key:    []byte("foo"),
					PrevKv: true,
				},
			},
			want: &proto.DeleteRangeResponse{PrevKvs: []*proto.KeyValue{{Key: []byte("foo"), Value: []byte("val")}}, Header: &proto.ResponseHeader{}},
		},
		{
			name: "Delete existing range",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mustMarshallProto(&proto.Command{
						Type:     proto.Command_DELETE,
						Kv:       &proto.KeyValue{Key: []byte("foo")},
						RangeEnd: []byte("foo1"),
					})).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}},
					}}})}, nil)
			},
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Key:      []byte("foo"),
					RangeEnd: []byte("foo1"),
				},
			},
			want: &proto.DeleteRangeResponse{Header: &proto.ResponseHeader{}},
		},
		{
			name: "Delete non-existent key",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: []byte("foo")},
			},
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mock.Anything).
					Return(sm.Result{Data: mustMarshallProto(&proto.CommandResult{Responses: []*proto.ResponseOp{{
						Response: &proto.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &proto.ResponseOp_DeleteRange{}},
					}}})}, nil)
			},
			want: &proto.DeleteRangeResponse{Deleted: 0, Header: &proto.ResponseHeader{}},
		},
		{
			name: "Delete key too long",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{Key: longKey},
			},
			wantErr: serrors.ErrKeyLengthExceeded,
		},
		{
			name: "Delete unknown error",
			on: func(handler *mockRaftHandler) {
				handler.
					On("SyncPropose", mock.Anything, mock.Anything, mock.Anything).
					Return(sm.Result{}, errUnknown)
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
			nh := &mockRaftHandler{}
			if tt.on != nil {
				tt.on(nh)
			}
			if tt.assert != nil {
				tt.assert(nh)
			}
			at := &ActiveTable{
				Table: Table{},
				nh:    nh,
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

func mustMarshallProto(message pb.Message) []byte {
	bytes, err := pb.Marshal(message)
	if err != nil {
		panic(err)
	}
	return bytes
}
