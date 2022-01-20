package fsm

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/proto"
)

type errorReader struct{}

func (e errorReader) Get(key []byte) (value []byte, closer io.Closer, err error) {
	return nil, nil, errors.New("error")
}

func (e errorReader) NewIter(o *pebble.IterOptions) *pebble.Iterator {
	return &pebble.Iterator{}
}

func (e errorReader) Close() error {
	return errors.New("error")
}

func Test_txnCompare(t *testing.T) {
	sm := filledSM()
	defer sm.Close()
	loadedPebble := (*pebble.DB)(atomic.LoadPointer(&sm.pebble))

	type args struct {
		reader  pebble.Reader
		compare []*proto.Compare
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "key exist",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key: []byte(fmt.Sprintf(testKeyFormat, 1)),
					},
				},
			},
			want: true,
		},
		{
			name: "key does not exist",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key: []byte("nonsense"),
					},
				},
			},
			want: false,
		},
		{
			name: "non empty range",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:      []byte(fmt.Sprintf(testKeyFormat, 1)),
						RangeEnd: []byte(fmt.Sprintf(testKeyFormat, 5)),
					},
				},
			},
			want: true,
		},
		{
			name: "empty range",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:      []byte("nonsense"),
						RangeEnd: []byte("nonsense2"),
					},
				},
			},
			want: false,
		},
		{
			name: "fail fast",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key: []byte("nonsense"),
					},
					{
						Key: []byte(fmt.Sprintf(testKeyFormat, 1)),
					},
				},
			},
			want: false,
		},
		{
			name: "fail late",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key: []byte(fmt.Sprintf(testKeyFormat, 1)),
					},
					{
						Key: []byte("nonsense"),
					},
				},
			},
			want: false,
		},
		{
			name: "value comparison",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						TargetUnion: &proto.Compare_Value{Value: []byte(testValue)},
					},
				},
			},
			want: true,
		},
		{
			name: "range value comparison",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						RangeEnd:    []byte(fmt.Sprintf(testKeyFormat, 10)),
						TargetUnion: &proto.Compare_Value{Value: []byte(testValue)},
					},
				},
			},
			want: true,
		},
		{
			name: "fail value comparison",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						TargetUnion: &proto.Compare_Value{Value: []byte("nonsense")},
					},
				},
			},
			want: false,
		},
		{
			name: "fail range comparison",
			args: args{
				reader: loadedPebble,
				compare: []*proto.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						RangeEnd:    []byte(fmt.Sprintf(testKeyFormat, 10)),
						TargetUnion: &proto.Compare_Value{Value: []byte("nonsense")},
					},
				},
			},
			want: false,
		},
		{
			name: "fail to get key",
			args: args{
				reader: errorReader{},
				compare: []*proto.Compare{
					{
						Key: []byte(fmt.Sprintf(testKeyFormat, 1)),
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := txnCompare(tt.args.reader, tt.args.compare)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tt.want, got)
		})
	}
}

func Test_txnCompareSingle(t *testing.T) {
	type args struct {
		cmp   *proto.Compare
		value []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "empty compare",
			args: args{
				cmp:   &proto.Compare{},
				value: nil,
			},
			want: true,
		},
		{
			name: "EQUAL - equal value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_EQUAL,
					TargetUnion: &proto.Compare_Value{Value: []byte("test")},
				},
				value: []byte("test"),
			},
			want: true,
		},
		{
			name: "EQUAL - unequal value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_EQUAL,
					TargetUnion: &proto.Compare_Value{Value: []byte("test")},
				},
				value: []byte("testssadasd"),
			},
			want: false,
		},
		{
			name: "NOT EQUAL - equal value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_NOT_EQUAL,
					TargetUnion: &proto.Compare_Value{Value: []byte("test")},
				},
				value: []byte("test"),
			},
			want: false,
		},
		{
			name: "NOT EQUAL - unequal value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_NOT_EQUAL,
					TargetUnion: &proto.Compare_Value{Value: []byte("test")},
				},
				value: []byte("test"),
			},
			want: false,
		},
		{
			name: "GREATER - greater value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_GREATER,
					TargetUnion: &proto.Compare_Value{Value: []byte("testa")},
				},
				value: []byte("testaa"),
			},
			want: true,
		},
		{
			name: "GREATER - lesser value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_GREATER,
					TargetUnion: &proto.Compare_Value{Value: []byte("testa")},
				},
				value: []byte("test"),
			},
			want: false,
		},
		{
			name: "LESS - greater value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_LESS,
					TargetUnion: &proto.Compare_Value{Value: []byte("test")},
				},
				value: []byte("testa"),
			},
			want: false,
		},
		{
			name: "LESS - lesser value",
			args: args{
				cmp: &proto.Compare{
					Result:      proto.Compare_LESS,
					TargetUnion: &proto.Compare_Value{Value: []byte("testa")},
				},
				value: []byte("test"),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, txnCompareSingle(tt.args.cmp, tt.args.value))
		})
	}
}
