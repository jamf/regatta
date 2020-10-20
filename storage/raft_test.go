package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
)

func TestRaft_Put(t *testing.T) {
	type args struct {
		ctx context.Context
		req *proto.PutRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *proto.PutResponse
		wantErr bool
	}{
		{
			name: "Put an empty data",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{},
			},
			wantErr: true,
		},
		{
			name: "Put a record with empty key",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Table: []byte("table"),
					Value: []byte("value"),
				},
			},
			wantErr: true,
		},
		{
			name: "Put a record with empty table",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			wantErr: true,
		},
		{
			name: "Put a record with empty value",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Table: []byte("table"),
					Key:   []byte("key"),
					Value: []byte{}, // for the sake of simple comparison (storage always returns initialized values)
				},
			},
			want: &proto.PutResponse{
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
			},
		},
		{
			name: "Put a single key pair",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Table: []byte("table"),
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			want: &proto.PutResponse{
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture argument before launching in Parallel
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nh, meta := startRaftNode()
			st := Raft{
				NodeHost: nh,
				Session:  nh.GetNoOPSession(1),
				Metadata: meta,
			}
			defer nh.Stop()
			r := require.New(t)
			t.Log("store the value")
			got, err := st.Put(tt.args.ctx, tt.args.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)

			if tt.want != nil {
				t.Log("check that value is stored")
				val, err := st.Range(tt.args.ctx, &proto.RangeRequest{
					Table: tt.args.req.Table,
					Key:   tt.args.req.Key,
				})
				r.NoError(err)
				r.Equal(tt.args.req.Key, val.Kvs[0].Key)
				r.Equal(tt.args.req.Value, val.Kvs[0].Value)
			}
		})
	}
}

func TestRaft_Delete(t *testing.T) {
	type args struct {
		ctx context.Context
		req *proto.DeleteRangeRequest
	}
	tests := []struct {
		name            string
		args            args
		want            *proto.DeleteRangeResponse
		wantErr         bool
		preInsertedData []*proto.PutRequest
	}{
		{
			name: "Delete empty data",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{},
			},
			wantErr: true,
		},
		{
			name: "Delete record with empty key",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Table: []byte("table"),
				},
			},
			wantErr: true,
		},
		{
			name: "Delete record with empty table",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Key: []byte("key"),
				},
			},
			wantErr: true,
		},
		{
			name: "Delete a non-existent key pair",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Table: []byte("table"),
					Key:   []byte("key"),
				},
			},
			want: &proto.DeleteRangeResponse{
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
				Deleted: 1,
			},
		},
		{
			name: "Delete an existing key pair",
			args: args{
				ctx: context.TODO(),
				req: &proto.DeleteRangeRequest{
					Table: []byte("table1"),
					Key:   []byte("key1"),
				},
			},
			preInsertedData: []*proto.PutRequest{
				{
					Table: []byte("table1"),
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
			want: &proto.DeleteRangeResponse{
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
				Deleted: 1,
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture argument before launching in Parallel
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nh, meta := startRaftNode()
			st := Raft{
				NodeHost: nh,
				Session:  nh.GetNoOPSession(1),
				Metadata: meta,
			}
			defer nh.Stop()
			r := require.New(t)

			if len(tt.preInsertedData) > 0 {
				for _, data := range tt.preInsertedData {
					_, err := st.Put(tt.args.ctx, data)
					r.NoError(err)
				}
			}

			t.Log("delete the value")
			got, err := st.Delete(tt.args.ctx, tt.args.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)

			if tt.want.Deleted > 0 {
				t.Log("check that value is deleted")
				_, err := st.Range(tt.args.ctx, &proto.RangeRequest{
					Table: tt.args.req.Table,
					Key:   tt.args.req.Key,
				})
				r.Error(err)
				r.Equal(ErrNotFound, err)
			}
		})
	}
}

func TestRaft_Range(t *testing.T) {
	type args struct {
		ctx context.Context
		req *proto.RangeRequest
	}
	tests := []struct {
		name            string
		args            args
		want            *proto.RangeResponse
		wantErr         bool
		preInsertedData []*proto.PutRequest
	}{
		{
			name: "Query empty data",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{},
			},
			wantErr: true,
		},
		{
			name: "Query record with empty key",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{
					Table: []byte("table"),
				},
			},
			wantErr: true,
		},
		{
			name: "Query record with empty table",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{
					Key: []byte("key"),
				},
			},
			wantErr: true,
		},
		{
			name: "Query a non-existent key pair",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{
					Table: []byte("table"),
					Key:   []byte("key"),
				},
			},
			wantErr: true,
		},
		{
			name: "Query an existing key pair",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{
					Table: []byte("table1"),
					Key:   []byte("key1"),
				},
			},
			preInsertedData: []*proto.PutRequest{
				{
					Table: []byte("table1"),
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
			want: &proto.RangeResponse{
				Count: 1,
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
		},
		{
			name: "Query an existing key pair - linearized",
			args: args{
				ctx: context.TODO(),
				req: &proto.RangeRequest{
					Table:        []byte("table1"),
					Key:          []byte("key1"),
					Linearizable: true,
				},
			},
			preInsertedData: []*proto.PutRequest{
				{
					Table: []byte("table1"),
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
			want: &proto.RangeResponse{
				Count: 1,
				Header: &proto.ResponseHeader{
					ClusterId:    1,
					MemberId:     1,
					RaftLeaderId: 1,
					RaftTerm:     2,
				},
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture argument before launching in Parallel
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nh, meta := startRaftNode()
			st := Raft{
				NodeHost: nh,
				Session:  nh.GetNoOPSession(1),
				Metadata: meta,
			}
			defer nh.Stop()
			r := require.New(t)

			if len(tt.preInsertedData) > 0 {
				for _, data := range tt.preInsertedData {
					_, err := st.Put(tt.args.ctx, data)
					r.NoError(err)
				}
				defer func() {
					for _, data := range tt.preInsertedData {
						_, err := st.Delete(tt.args.ctx, &proto.DeleteRangeRequest{Table: data.Table, Key: data.Key})
						r.NoError(err)
					}
				}()
			}

			t.Log("query the value")
			got, err := st.Range(tt.args.ctx, tt.args.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}
