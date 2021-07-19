package regattaserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetadataServer_Get(t *testing.T) {
	type fields struct {
		TableManager TableService
	}
	tests := []struct {
		name    string
		fields  fields
		want    *proto.MetadataResponse
		wantErr error
	}{
		{
			name: "Get metadata - no tables",
			fields: fields{
				TableManager: MockTableService{
					tables: []table.Table{},
				},
			},
			want: &proto.MetadataResponse{Tables: nil},
		},
		{
			name: "Get metadata - single table",
			fields: fields{
				TableManager: MockTableService{
					tables: []table.Table{
						{
							Name: "foo",
						},
					},
				},
			},
			want: &proto.MetadataResponse{Tables: []*proto.Table{
				{
					Name: "foo",
					Type: proto.Table_REPLICATED,
				},
			}},
		},
		{
			name: "Get metadata - multiple tables",
			fields: fields{
				TableManager: MockTableService{
					tables: []table.Table{
						{
							Name: "foo",
						},
						{
							Name: "bar",
						},
					},
				},
			},
			want: &proto.MetadataResponse{Tables: []*proto.Table{
				{
					Name: "foo",
					Type: proto.Table_REPLICATED,
				},
				{
					Name: "bar",
					Type: proto.Table_REPLICATED,
				},
			}},
		},
		{
			name: "Get metadata - deadline exceeded",
			fields: fields{
				TableManager: MockTableService{
					error: context.DeadlineExceeded,
				},
			},
			wantErr: status.Errorf(codes.Internal, "unknown err %v", context.DeadlineExceeded),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			m := &MetadataServer{
				Tables: tt.fields.TableManager,
			}
			got, err := m.Get(context.TODO(), &proto.MetadataRequest{})
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.Equal(tt.want, got)
		})
	}
}
