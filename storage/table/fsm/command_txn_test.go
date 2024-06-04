// Copyright JAMF Software, LLC

package fsm

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	rp "github.com/jamf/regatta/pebble"
	"github.com/jamf/regatta/regattapb"
	"github.com/stretchr/testify/require"
)

type errorReader struct{}

func (e errorReader) Get(key []byte) (value []byte, closer io.Closer, err error) {
	return nil, nil, errors.New("error")
}

func (e errorReader) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	return &pebble.Iterator{}, nil
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
		compare []*regattapb.Compare
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
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
				compare: []*regattapb.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						TargetUnion: &regattapb.Compare_Value{Value: []byte(testValue)},
					},
				},
			},
			want: true,
		},
		{
			name: "range value comparison",
			args: args{
				reader: loadedPebble,
				compare: []*regattapb.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						RangeEnd:    []byte(fmt.Sprintf(testKeyFormat, 10)),
						TargetUnion: &regattapb.Compare_Value{Value: []byte(testValue)},
					},
				},
			},
			want: true,
		},
		{
			name: "fail value comparison",
			args: args{
				reader: loadedPebble,
				compare: []*regattapb.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						TargetUnion: &regattapb.Compare_Value{Value: []byte("nonsense")},
					},
				},
			},
			want: false,
		},
		{
			name: "fail range comparison",
			args: args{
				reader: loadedPebble,
				compare: []*regattapb.Compare{
					{
						Key:         []byte(fmt.Sprintf(testKeyFormat, 1)),
						RangeEnd:    []byte(fmt.Sprintf(testKeyFormat, 10)),
						TargetUnion: &regattapb.Compare_Value{Value: []byte("nonsense")},
					},
				},
			},
			want: false,
		},
		{
			name: "fail to get key",
			args: args{
				reader: errorReader{},
				compare: []*regattapb.Compare{
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
	_, err = handlePutBatch(c, []*regattapb.RequestOp_Put{
		{Key: []byte("key_1"), Value: []byte("value")},
		{Key: []byte("key_2"), Value: []byte("value")},
		{Key: []byte("key_3"), Value: []byte("value")},
		{Key: []byte("key_4"), Value: []byte("value")},
	})
	r.NoError(err)
	r.NoError(c.Commit())

	c.batch = db.NewBatch()

	// empty transaction
	succ, res, err := handleTxn(c, []*regattapb.Compare{{Key: []byte("key_1")}}, nil, nil)
	r.True(succ)
	r.NoError(err)
	r.Empty(res)

	// insert key_5 with nil value
	succ, res, err = handleTxn(c, []*regattapb.Compare{{Key: []byte("key_1")}}, []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{RequestPut: &regattapb.RequestOp_Put{Key: []byte("key_5"), Value: nil}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Len(res, 1)
	r.Equal(wrapResponseOp(&regattapb.ResponseOp_Put{}), res[0])

	// compare key_5 nil value and associate the key with "value"
	succ, res, err = handleTxn(c, []*regattapb.Compare{{Key: []byte("key_5"), TargetUnion: &regattapb.Compare_Value{Value: nil}}}, []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestPut{RequestPut: &regattapb.RequestOp_Put{Key: []byte("key_5"), Value: []byte("value"), PrevKv: true}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Len(res, 1)
	r.Equal(wrapResponseOp(&regattapb.ResponseOp_Put{PrevKv: &regattapb.KeyValue{Key: []byte("key_5"), Value: nil}}), res[0])

	// compare key_5 value with "value" and delete keys up to key_4 (non-inclusive)
	succ, res, err = handleTxn(c, []*regattapb.Compare{{Key: []byte("key_5"), TargetUnion: &regattapb.Compare_Value{Value: []byte("value")}}}, []*regattapb.RequestOp{{Request: &regattapb.RequestOp_RequestDeleteRange{RequestDeleteRange: &regattapb.RequestOp_DeleteRange{Key: []byte("key_1"), RangeEnd: []byte("key_4"), PrevKv: true}}}}, nil)
	r.True(succ)
	r.NoError(err)
	r.Len(res, 1)
	r.Equal(wrapResponseOp(&regattapb.ResponseOp_DeleteRange{
		Deleted: 3,
		PrevKvs: []*regattapb.KeyValue{
			{Key: []byte("key_1"), Value: []byte("value")},
			{Key: []byte("key_2"), Value: []byte("value")},
			{Key: []byte("key_3"), Value: []byte("value")},
		},
	}), res[0])

	r.NoError(c.Commit())

	iter, err := db.NewIter(allUserKeysOpts())
	r.NoError(err)
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
		cmp   *regattapb.Compare
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
				cmp:   &regattapb.Compare{},
				value: nil,
			},
			want: true,
		},
		{
			name: "EQUAL - equal value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_EQUAL,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("test")},
				},
				value: []byte("test"),
			},
			want: true,
		},
		{
			name: "EQUAL - unequal value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_EQUAL,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("test")},
				},
				value: []byte("testssadasd"),
			},
			want: false,
		},
		{
			name: "NOT EQUAL - equal value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_NOT_EQUAL,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("test")},
				},
				value: []byte("test"),
			},
			want: false,
		},
		{
			name: "NOT EQUAL - unequal value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_NOT_EQUAL,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("test")},
				},
				value: []byte("testytest"),
			},
			want: true,
		},
		{
			name: "GREATER - greater value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_GREATER,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("testa")},
				},
				value: []byte("testaa"),
			},
			want: true,
		},
		{
			name: "GREATER - lesser value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_GREATER,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("testa")},
				},
				value: []byte("test"),
			},
			want: false,
		},
		{
			name: "LESS - greater value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_LESS,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("test")},
				},
				value: []byte("testa"),
			},
			want: false,
		},
		{
			name: "LESS - lesser value",
			args: args{
				cmp: &regattapb.Compare{
					Result:      regattapb.Compare_LESS,
					TargetUnion: &regattapb.Compare_Value{Value: []byte("testa")},
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
