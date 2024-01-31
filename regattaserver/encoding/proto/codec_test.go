// Copyright JAMF Software, LLC

package proto

import (
	"testing"

	"github.com/jamf/regatta/regattapb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
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
				v: &regattapb.KeyValue{Key: []byte("key")},
			},
		},
		{
			name: "Marshal V1/V2 proto",
			args: args{
				v: &grpc_reflection_v1.ServerReflectionResponse{ValidHost: "host"},
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
				v:    &regattapb.KeyValue{},
				data: mustMarshall(&regattapb.KeyValue{Key: []byte("key")}),
			},
		},
		{
			name: "Unmarshal V1/V2 proto",
			args: args{
				v:    &grpc_reflection_v1.ServerReflectionResponse{},
				data: mustMarshall(&regattapb.KeyValue{Key: []byte("key")}),
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

type defaultCodec struct{}

func (d defaultCodec) Marshal(v any) ([]byte, error) {
	return pb.Marshal(v.(pb.Message))
}

func (d defaultCodec) Unmarshal(data []byte, v any) error {
	return pb.Unmarshal(data, v.(pb.Message))
}

func (d defaultCodec) Name() string {
	return Name
}

func BenchmarkCodec_Unmarshal(b *testing.B) {
	benchmarks := []struct {
		name  string
		codec encoding.Codec
	}{
		{
			name:  "unmarshal - default codec",
			codec: defaultCodec{},
		},
		{
			name:  "unmarshal - custom codec",
			codec: Codec{},
		},
	}
	for _, bm := range benchmarks {
		message := mustMarshall(&regattapb.TxnRequest{Success: []*regattapb.RequestOp{
			{
				Request: &regattapb.RequestOp_RequestPut{
					RequestPut: &regattapb.RequestOp_Put{Key: []byte("key1"), Value: []byte("value")},
				},
			},
			{
				Request: &regattapb.RequestOp_RequestPut{
					RequestPut: &regattapb.RequestOp_Put{Key: []byte("key2"), Value: []byte("value")},
				},
			},
			{
				Request: &regattapb.RequestOp_RequestPut{
					RequestPut: &regattapb.RequestOp_Put{Key: []byte("key3"), Value: []byte("value")},
				},
			},
			{
				Request: &regattapb.RequestOp_RequestPut{
					RequestPut: &regattapb.RequestOp_Put{Key: []byte("key4"), Value: []byte("value")},
				},
			},
			{
				Request: &regattapb.RequestOp_RequestPut{
					RequestPut: &regattapb.RequestOp_Put{Key: []byte("key5"), Value: []byte("value")},
				},
			},
		}})
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := regattapb.PutRequest{}
				_ = bm.codec.Unmarshal(message, &m)
			}
		})
	}
}

func BenchmarkCodec_Marshal(b *testing.B) {
	benchmarks := []struct {
		name  string
		codec encoding.Codec
	}{
		{
			name:  "marshal - default codec",
			codec: defaultCodec{},
		},
		{
			name:  "marshal - custom codec",
			codec: Codec{},
		},
	}
	for _, bm := range benchmarks {
		message := &regattapb.RangeResponse{Kvs: []*regattapb.KeyValue{
			{Key: []byte("key1"), Value: []byte("value")},
			{Key: []byte("key2"), Value: []byte("value")},
			{Key: []byte("key3"), Value: []byte("value")},
			{Key: []byte("key4"), Value: []byte("value")},
		}}
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = bm.codec.Marshal(message)
			}
		})
	}
}
