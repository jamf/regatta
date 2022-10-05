package fsm

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	rp "github.com/wandera/regatta/pebble"
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
	loadedPebble := sm.pebble.Load()

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

func Test_handleTxn(t *testing.T) {
	r := require.New(t)

	db, err := rp.OpenDB("/", rp.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatalf("could not open pebble db: %v", err)
	}

	c := &updateContext{
		batch: db.NewBatch(),
		db:    db,
		index: 1,
	}
	defer func() { _ = c.Close() }()

	// Make the PUT_BATCH.
	_, err = handlePutBatch(c, []*proto.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// empty transaction
	succ, res, err := handleTxn(c, []*proto.Compare{{Key: []byte("key_1")}}, nil, nil)
	r.True(succ)
	r.NoError(err)
	r.Empty(res)

	// insert key_5 with nil value
	succ, res, err = handleTxn(c, []*proto.Compare{{Key: []byte("key_1")}}, []*proto.RequestOp{{Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{Key: []byte("key_5"), Value: nil}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Equal(1, len(res))
	r.Equal(wrapResponseOp(&proto.ResponseOp_Put{}), res[0])

	// compare key_5 nil value and associate the key with "value"
	succ, res, err = handleTxn(c, []*proto.Compare{{Key: []byte("key_5"), TargetUnion: &proto.Compare_Value{Value: nil}}}, []*proto.RequestOp{{Request: &proto.RequestOp_RequestPut{RequestPut: &proto.RequestOp_Put{Key: []byte("key_5"), Value: []byte("value"), PrevKv: true}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Equal(1, len(res))
	r.Equal(wrapResponseOp(&proto.ResponseOp_Put{PrevKv: &proto.KeyValue{Key: []byte("key_5"), Value: nil}}), res[0])

	// compare key_5 value with "value" and delete keys up to key_4 (non-inclusive)
	succ, res, err = handleTxn(c, []*proto.Compare{{Key: []byte("key_5"), TargetUnion: &proto.Compare_Value{Value: []byte("value")}}}, []*proto.RequestOp{{Request: &proto.RequestOp_RequestDeleteRange{RequestDeleteRange: &proto.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: []byte("key_4"), PrevKv: true}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Equal(1, len(res))
	r.Equal(wrapResponseOp(&proto.ResponseOp_DeleteRange{
		PrevKvs: []*proto.KeyValue{
			{Key: []byte("key_1"), Value: []byte("value")},
			{Key: []byte("key_2"), Value: []byte("value")},
			{Key: []byte("key_3"), Value: []byte("value")},
		},
	}), res[0])

	r.NoError(c.Commit())

	iter := db.NewIter(allUserKeysOpts())
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
		r.Equal("value", string(iter.Value()))
	}
	// just keys key_4 and key_5 should remain
	r.Equal(2, count)

	// Check the system keys.
	index, err := readLocalIndex(db, sysLocalIndex)
	r.NoError(err)
	r.Equal(c.index, index)
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
				value: []byte("testytest"),
			},
			want: true,
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
