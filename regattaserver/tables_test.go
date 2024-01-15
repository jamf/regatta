// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jamf/regatta/regattapb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTablesServer_Create(t *testing.T) {
	type fields struct {
		Tables   []string
		AuthFunc func(ctx context.Context) (context.Context, error)
	}
	type args struct {
		req *regattapb.CreateTableRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *regattapb.CreateTableResponse
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "allow all - missing table name",
			fields:  fields{AuthFunc: allowAll},
			args:    args{req: &regattapb.CreateTableRequest{}},
			wantErr: assert.Error,
		},
		{
			name:    "allow all - not existing table",
			fields:  fields{AuthFunc: allowAll},
			args:    args{req: &regattapb.CreateTableRequest{Name: "new"}},
			wantErr: assert.NoError,
			want:    &regattapb.CreateTableResponse{Id: "10001"},
		},
		{
			name:    "allow all - existing table",
			fields:  fields{AuthFunc: allowAll, Tables: []string{"exists"}},
			args:    args{req: &regattapb.CreateTableRequest{Name: "exists"}},
			wantErr: assert.Error,
		},
		{
			name:    "deny all",
			fields:  fields{AuthFunc: denyAll},
			args:    args{req: &regattapb.CreateTableRequest{}},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &TablesServer{
				Tables:   newInMemTestEngine(t, tt.fields.Tables...),
				AuthFunc: tt.fields.AuthFunc,
			}
			got, err := ts.Create(context.TODO(), tt.args.req)
			if !tt.wantErr(t, err, fmt.Sprintf("Create(%v, %v)", context.TODO(), tt.args.req)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Create(%v, %v)", context.TODO(), tt.args.req)
		})
	}
}

func TestTablesServer_Delete(t *testing.T) {
	type fields struct {
		Tables   []string
		AuthFunc func(ctx context.Context) (context.Context, error)
	}
	type args struct {
		req *regattapb.DeleteTableRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *regattapb.DeleteTableResponse
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "allow all - missing table name",
			fields:  fields{AuthFunc: allowAll},
			args:    args{req: &regattapb.DeleteTableRequest{}},
			wantErr: assert.Error,
		},
		{
			name:    "allow all - not existing table",
			fields:  fields{AuthFunc: allowAll},
			args:    args{req: &regattapb.DeleteTableRequest{Name: "nonexistent"}},
			wantErr: assert.Error,
		},
		{
			name:    "allow all - existing table",
			fields:  fields{AuthFunc: allowAll, Tables: []string{"exists"}},
			args:    args{req: &regattapb.DeleteTableRequest{Name: "exists"}},
			wantErr: assert.NoError,
			want:    &regattapb.DeleteTableResponse{},
		},
		{
			name:    "deny all",
			fields:  fields{AuthFunc: denyAll},
			args:    args{req: &regattapb.DeleteTableRequest{}},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &TablesServer{
				Tables:   newInMemTestEngine(t, tt.fields.Tables...),
				AuthFunc: tt.fields.AuthFunc,
			}
			got, err := ts.Delete(context.TODO(), tt.args.req)
			if !tt.wantErr(t, err, fmt.Sprintf("Delete(%v, %v)", context.TODO(), tt.args.req)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Delete(%v, %v)", context.TODO(), tt.args.req)
		})
	}
}

func TestTablesServer_List(t *testing.T) {
	type fields struct {
		Tables   []string
		AuthFunc func(ctx context.Context) (context.Context, error)
	}
	tests := []struct {
		name    string
		fields  fields
		want    *regattapb.ListTablesResponse
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "allow all",
			fields:  fields{AuthFunc: allowAll},
			wantErr: assert.NoError,
		},
		{
			name:    "allow all multiple tables",
			fields:  fields{AuthFunc: allowAll, Tables: []string{"table1", "table2"}},
			wantErr: assert.NoError,
			want: &regattapb.ListTablesResponse{Tables: []*regattapb.TableInfo{
				{
					Name: "table1",
					Id:   "10001",
				},
				{
					Name: "table2",
					Id:   "10002",
				},
			}},
		},
		{
			name:    "deny all",
			fields:  fields{AuthFunc: denyAll},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &TablesServer{
				Tables:   newInMemTestEngine(t, tt.fields.Tables...),
				AuthFunc: tt.fields.AuthFunc,
			}
			_, err := ts.List(context.TODO(), &regattapb.ListTablesRequest{})
			if !tt.wantErr(t, err, fmt.Sprintf("List(%v, %v)", context.TODO(), &regattapb.ListTablesRequest{})) {
				return
			}
		})
	}
}

func TestReadonlyTablesServer_Create(t *testing.T) {
	ts := &ReadonlyTablesServer{}
	_, err := ts.Create(context.TODO(), &regattapb.CreateTableRequest{})
	assert.ErrorIs(t, err, status.Error(codes.Unimplemented, "method Create not implemented for follower"))
}

func TestReadonlyTablesServer_Delete(t *testing.T) {
	ts := &ReadonlyTablesServer{}
	_, err := ts.Delete(context.TODO(), &regattapb.DeleteTableRequest{})
	assert.ErrorIs(t, err, status.Error(codes.Unimplemented, "method Delete not implemented for follower"))
}

func denyAll(ctx context.Context) (context.Context, error) {
	return nil, errors.New("denied")
}

func allowAll(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
