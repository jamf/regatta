package fsm

import (
	"fmt"
	"testing"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
)

func TestSM_Lookup(t *testing.T) {
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
	}
	type args struct {
		key *proto.RangeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Pebble - Lookup empty DB",
			fields: fields{
				smFactory: emptySM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Pebble - Lookup full DB with non-existent key",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Key: []byte("Hello"),
			}},
			wantErr: true,
		},
		{
			name: "Pebble - Lookup full DB with existing key",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table: []byte(testTable),
				Key:   []byte(fmt.Sprintf(testKeyFormat, 0)),
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Lookup full DB with existing key and large value",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table: []byte(testTable),
				Key:   []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Lookup with KeysOnly",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				KeysOnly: true,
			}},
			want: &proto.RangeResponse{
				Kvs: []*proto.KeyValue{
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
				},
				Count: 1,
			},
		},
		{
			name: "Pebble - Lookup with CountOnly",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:     []byte(testTable),
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				CountOnly: true,
			}},
			want: &proto.RangeResponse{
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
			got, err := p.Lookup(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func TestSM_Range(t *testing.T) {
	type fields struct {
		smFactory func() sm.IOnDiskStateMachine
	}
	type args struct {
		key *proto.RangeRequest
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Pebble - Range lookup of 2 adjacent keys",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 2)),
				Limit:    0,
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Range lookup of adjacent keys with limit set to 1",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 9)),
				Limit:    1,
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Range lookup of adjacent short keys with range_end == '\\0'",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testKeyFormat, 0)),
				RangeEnd: []byte{0},
				Limit:    3,
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Range lookup of adjacent long keys with range_end == '\\0'",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte{0},
				Limit:    3,
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Range Lookup list all pairs",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte{0},
				RangeEnd: []byte{0},
				Limit:    0,
			}},
			want: &proto.RangeResponse{
				Count: smallEntries + largeEntries,
			},
		},
		{
			name: "Pebble - Range lookup of adjacent keys with KeysOnly, RangeEnd, and Limit (stops on limit)",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 5)),
				KeysOnly: true,
				Limit:    3,
			}},
			want: &proto.RangeResponse{
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
			name: "Pebble - Range lookup of adjacent keys with KeysOnly, RangeEnd, and Limit (stops on RangeEnd)",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:    []byte(testTable),
				Key:      []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd: []byte(fmt.Sprintf(testLargeKeyFormat, 3)),
				KeysOnly: true,
				Limit:    10,
			}},
			want: &proto.RangeResponse{
				Kvs: []*proto.KeyValue{
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 0))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 1))},
					{Key: []byte(fmt.Sprintf(testLargeKeyFormat, 2))},
				},
				Count: 3,
			},
		},
		{
			name: "Pebble - Range lookup with CountOnly, RangeEnd, and Limit (stops on limit)",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:     []byte(testTable),
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd:  []byte(fmt.Sprintf(testLargeKeyFormat, 5)),
				CountOnly: true,
				Limit:     3,
			}},
			want: &proto.RangeResponse{Count: 3},
		},
		{
			name: "Pebble - Range lookup with CountOnly, RangeEnd, and Limit (stops on RangeEnd)",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:     []byte(testTable),
				Key:       []byte(fmt.Sprintf(testLargeKeyFormat, 0)),
				RangeEnd:  []byte(fmt.Sprintf(testLargeKeyFormat, 3)),
				CountOnly: true,
				Limit:     10,
			}},
			want: &proto.RangeResponse{Count: 3},
		},
		{
			name: "Pebble - Range prefix lookup with CountOnly",
			fields: fields{
				smFactory: filledSM,
			},
			args: args{key: &proto.RangeRequest{
				Table:     []byte(testTable),
				Key:       []byte("testlarge"),
				RangeEnd:  addOne([]byte("testlarge")),
				CountOnly: true,
			}},
			want: &proto.RangeResponse{Count: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			p := tt.fields.smFactory()
			defer func() {
				r.NoError(p.Close())
			}()
			got, err := p.Lookup(tt.args.key)
			if tt.wantErr {
				r.Error(err)
				return
			}

			r.NoError(err)

			wantResponse, ok := tt.want.(*proto.RangeResponse)
			if !ok {
				r.Fail("could not cast the 'tt.want' to '*proto.RangeResponse'")
			}

			gotResponse, ok := got.(*proto.RangeResponse)
			if !ok {
				r.Fail("could not cast the 'got' to '*proto.RangeResponse'")
			}

			r.Equal(wantResponse.Count, gotResponse.Count)
			if len(wantResponse.Kvs) != 0 {
				r.Equal(tt.want, got)
			}
		})
	}
}

// addOne to the leftmost byte in the byte slice.
func addOne(b []byte) []byte {
	tmp := make([]byte, len(b))
	copy(tmp, b)
	tmp[len(tmp)-1] = tmp[len(tmp)-1] + 1
	return tmp
}