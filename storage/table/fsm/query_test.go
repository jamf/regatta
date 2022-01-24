package fsm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
)

func TestFSM_Lookup(t *testing.T) {
	type fields struct {
		smFactory func() *FSM
	}
	tests := []struct {
		name    string
		fields  fields
		req     *proto.RequestOp_Range
		want    *proto.ResponseOp_Range
		wantErr bool
	}{
		{
			name: "Lookup empty DB",
			fields: fields{
				smFactory: emptySM,
			},
			req: &proto.RequestOp_Range{
				Key: []byte("Hello"),
			},
			wantErr: true,
		},
		{
			name: "Lookup full DB with non-existent key",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key: []byte("Hello"),
			},

			wantErr: true,
		},
		{
			name: "Lookup full DB with existing key",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key: []byte(fmt.Sprintf(testKeyFormat, 0)),
			},

			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
						Value: []byte(testValue),
					},
				},
				Count: 1,
			},
		},
		{
			name: "Lookup full DB with existing key and large value",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
			},

			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
						Value: []byte(largeValues[0]),
					},
				},
				Count: 1,
			},
		},
		{
			name: "Lookup with KeysOnly",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				KeysOnly: true,
			},

			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
				},
				Count: 1,
			},
		},
		{
			name: "Lookup with CountOnly",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				CountOnly: true,
			},

			want: &proto.ResponseOp_Range{
				Count: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestFSM_Lookup_Txn(t *testing.T) {
	type fields struct {
		smFactory func() *FSM
	}
	tests := []struct {
		name    string
		fields  fields
		req     *proto.TxnRequest
		want    *proto.TxnResponse
		wantErr bool
	}{
		{
			name: "Lookup empty DB",
			fields: fields{
				smFactory: emptySM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte("Hello")}),
				},
			},
			wantErr: true,
		},
		{
			name: "Lookup full DB with non-existent key",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte("Hello")}),
				},
			},
			wantErr: true,
		},
		{
			name: "Lookup full DB with existing key",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testKeyFormat, 0))}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: true,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{
						Kvs: []*proto.KeyValue{
							{
								Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
								Value: []byte(testValue),
							},
						},
						Count: 1,
					}),
				},
			},
		},
		{
			name: "Lookup full DB with existing key and large value",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: true,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{
						Kvs: []*proto.KeyValue{
							{
								Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
								Value: []byte(largeValues[0]),
							},
						},
						Count: 1,
					}),
				},
			},
		},
		{
			name: "Lookup with KeysOnly",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)), KeysOnly: true}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: true,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{
						Kvs: []*proto.KeyValue{
							{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
						},
						Count: 1,
					}),
				},
			},
		},
		{
			name: "Lookup with CountOnly",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)), CountOnly: true}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: true,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{Count: 1}),
				},
			},
		},
		{
			name: "Lookup with Compare success",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Compare: []*proto.Compare{
					{
						Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
					},
				},
				Success: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)), CountOnly: true}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: true,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{Count: 1}),
				},
			},
		},
		{
			name: "Lookup with Compare fail",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.TxnRequest{
				Compare: []*proto.Compare{
					{
						Key: []byte("nonsense"),
					},
				},
				Failure: []*proto.RequestOp{
					wrapRequestOp(&proto.RequestOp_Range{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0)), CountOnly: true}),
				},
			},
			want: &proto.TxnResponse{
				Succeeded: false,
				Responses: []*proto.ResponseOp{
					wrapResponseOp(&proto.ResponseOp_Range{Count: 1}),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.req)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)

			gotResponse, ok := got.(*proto.TxnResponse)
			if !ok {
				r.Fail("could not cast the 'got' to '*proto.ResponseOp_Range'")
			}
			r.Equal(tt.want, gotResponse)
		})
	}
}

func TestFSM_Lookup_Range(t *testing.T) {
	type fields struct {
		smFactory func() *FSM
	}

	tests := []struct {
		name    string
		fields  fields
		req     *proto.RequestOp_Range
		want    *proto.ResponseOp_Range
		wantErr bool
	}{
		{
			name: "Range lookup of 2 adjacent keys",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 2)),
				Limit:    0,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
						Value: []byte(largeValues[0]),
					},
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 1)),
						Value: []byte(largeValues[1]),
					},
				},
				Count: 2,
			},
		},
		{
			name: "Range lookup of adjacent keys with limit set to 1",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 9)),
				Limit:    1,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
						Value: []byte(largeValues[0]),
					},
				},
				Count: 1,
				More:  true,
			},
		},
		{
			name: "Range lookup of adjacent short keys with range_end == '\\0'",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testKeyFormat, 0)),
				RangeEnd: []byte{0},
				Limit:    3,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
						Value: []byte(testValue),
					},
					{
						Key:   []byte(fmt.Sprintf(testKeyFormat, 1)),
						Value: []byte(testValue),
					},
					{
						Key:   []byte(fmt.Sprintf(testKeyFormat, 10)),
						Value: []byte(testValue),
					},
				},
				Count: 3,
				More:  true,
			},
		},
		{
			name: "Range lookup of adjacent long keys with range_end == '\\0'",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte{0},
				Limit:    3,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
						Value: []byte(largeValues[0]),
					},
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 1)),
						Value: []byte(largeValues[1]),
					},
					{
						Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 2)),
						Value: []byte(largeValues[2]),
					},
				},
				Count: 3,
				More:  true,
			},
		},
		{
			name: "Range Lookup list all pairs",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte{0},
				RangeEnd: []byte{0},
				Limit:    0,
			},
			want: &proto.ResponseOp_Range{
				Count: smallEntries + largeEntries,
			},
		},
		{
			name: "Range lookup of adjacent keys with KeysOnly, RangeEnd, and Limit (stops on limit)",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 5)),
				KeysOnly: true,
				Limit:    3,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 1))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 2))},
				},
				Count: 3,
				More:  true,
			},
		},
		{
			name: "Range lookup of adjacent keys with KeysOnly, RangeEnd, and Limit (stops on RangeEnd)",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 3)),
				KeysOnly: true,
				Limit:    10,
			},
			want: &proto.ResponseOp_Range{
				Kvs: []*proto.KeyValue{
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 1))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 2))},
				},
				Count: 3,
			},
		},
		{
			name: "Range lookup with CountOnly, RangeEnd, and Limit (stops on limit)",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd:  []byte(fmt.Sprintf(testLargeKeyFormat, 5)),
				CountOnly: true,
				Limit:     3,
			},
			want: &proto.ResponseOp_Range{
				Count: 3,
			},
		},
		{
			name: "Range lookup with CountOnly, RangeEnd, and Limit (stops on RangeEnd)",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd:  []byte(fmt.Sprintf(testLargeKeyFormat, 3)),
				CountOnly: true,
				Limit:     10,
			},
			want: &proto.ResponseOp_Range{
				Count: 3,
			},
		},
		{
			name: "Range prefix lookup with CountOnly",
			fields: fields{
				smFactory: filledSM,
			},
			req: &proto.RequestOp_Range{
				Key:       []byte("testlarge"),
				RangeEnd:  incrementRightmostByte([]byte("testlarge")),
				CountOnly: true,
			},
			want: &proto.ResponseOp_Range{
				Count: 10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.req)
			if tt.wantErr {
				r.Error(err)
				return
			}

			r.NoError(err)

			gotResponse, ok := got.(*proto.ResponseOp_Range)
			if !ok {
				r.Fail("could not cast the 'got' to '*proto.ResponseOp_Range'")
			}

			r.Equal(tt.want.Count, gotResponse.Count)
			if len(tt.want.Kvs) != 0 {
				r.Equal(tt.want, got)
			}
		})
	}
}
