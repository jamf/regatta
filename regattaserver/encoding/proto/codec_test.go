// Copyright JAMF Software, LLC

package proto

import (
	"testing"

	"github.com/jamf/regatta/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	pb "google.golang.org/protobuf/proto"
)

func TestCodec_Marshal(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Marshal VT proto",
			args: args{
				v: &proto.KeyValue{Key: []byte("key")},
			},
		},
		{
			name: "Marshal V1/V2 proto",
			args: args{
				v: &grpc_reflection_v1alpha.ServerReflectionResponse{ValidHost: "host"},
			},
		},
		{
			name: "Marshal unknown struct",
			args: args{
				v: struct{}{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			co := Codec{}
			got, err := co.Marshal(tt.args.v)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.NotEmpty(got)
		})
	}
}

func TestCodec_Unmarshal(t *testing.T) {
	type args struct {
		v    interface{}
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Unmarshal VT proto",
			args: args{
				v:    &proto.KeyValue{},
				data: mustMarshall(&proto.KeyValue{Key: []byte("key")}),
			},
		},
		{
			name: "Unmarshal V1/V2 proto",
			args: args{
				v:    &grpc_reflection_v1alpha.ServerReflectionResponse{},
				data: mustMarshall(&proto.KeyValue{Key: []byte("key")}),
			},
		},
		{
			name: "Unmarshal unknown struct",
			args: args{
				v: struct{}{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			co := Codec{}
			err := co.Unmarshal(tt.args.data, tt.args.v)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
		})
	}
}

func mustMarshall(v pb.Message) []byte {
	bb, err := pb.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bb
}
