package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
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
		want    Result
		wantErr bool
	}{
		{
			name: "Put empty data into RAFT",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{},
			},
			want: Result{
				Value: 0,
			},
		},
		{
			name: "Put a single key pair into RAFT",
			args: args{
				ctx: context.TODO(),
				req: &proto.PutRequest{
					Table: []byte("table"),
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
			want: Result{
				Value: 1,
			},
		},
	}
	for _, tt := range tests {
		nh := startRaftNode()
		st := Raft{
			NodeHost: nh,
			Session:  nh.GetNoOPSession(1),
		}
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			err := st.Reset(context.TODO(), nil)
			r.NoError(err)

			got, err := st.Put(tt.args.ctx, tt.args.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}

func TestRaft_Delete(t *testing.T) {
	type fields struct {
		NodeHost *dragonboat.NodeHost
		Session  *client.Session
	}
	type args struct {
		ctx context.Context
		req *proto.DeleteRangeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Result
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{
				NodeHost: tt.fields.NodeHost,
				Session:  tt.fields.Session,
			}
			got, err := r.Delete(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaft_Hash(t *testing.T) {
	type fields struct {
		NodeHost *dragonboat.NodeHost
		Session  *client.Session
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{
				NodeHost: tt.fields.NodeHost,
				Session:  tt.fields.Session,
			}
			got, err := r.Hash(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Hash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Hash() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaft_Range(t *testing.T) {
	type fields struct {
		NodeHost *dragonboat.NodeHost
		Session  *client.Session
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
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{
				NodeHost: tt.fields.NodeHost,
				Session:  tt.fields.Session,
			}
			got, err := r.Range(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Range() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Range() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaft_Reset(t *testing.T) {
	type fields struct {
		NodeHost *dragonboat.NodeHost
		Session  *client.Session
	}
	type args struct {
		ctx context.Context
		req *proto.ResetRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{
				NodeHost: tt.fields.NodeHost,
				Session:  tt.fields.Session,
			}
			if err := r.Reset(tt.args.ctx, tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("Reset() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
