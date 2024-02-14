// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/jamf/regatta/util"
	"github.com/jamf/regatta/util/iter"
	"github.com/stretchr/testify/require"
)

func Test_iterateBasic(t *testing.T) {
	type args struct {
		req  *regattapb.RequestOp_Range
		data iter.Seq[*regattapb.KeyValue]
	}
	tests := []struct {
		name string
		args args
		want iter.Seq[*regattapb.ResponseOp_Range]
	}{
		{
			name: "empty dataset",
			args: args{
				req:  &regattapb.RequestOp_Range{},
				data: iter.From[*regattapb.KeyValue](),
			},
			want: iter.From(&regattapb.ResponseOp_Range{}),
		},
		{
			name: "small dataset query miss",
			args: args{
				req: &regattapb.RequestOp_Range{},
				data: iter.From(&regattapb.KeyValue{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				}),
			},
			want: iter.From(&regattapb.ResponseOp_Range{}),
		},
		{
			name: "small dataset query hit",
			args: args{
				req: &regattapb.RequestOp_Range{
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
				data: iter.From(&regattapb.KeyValue{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				}),
			},
			want: iter.From(&regattapb.ResponseOp_Range{
				Count: 1,
				Kvs: []*regattapb.KeyValue{
					{
						Key:   []byte("foo"),
						Value: []byte("bar"),
					},
				},
			}),
		},
		{
			name: "large dataset no response split",
			args: args{
				req: &regattapb.RequestOp_Range{
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
				data: generateSequence(1000, func(n int) *regattapb.KeyValue {
					return &regattapb.KeyValue{
						Key:   []byte(fmt.Sprintf("key/%d", n)),
						Value: []byte("foo"),
					}
				}),
			},
			want: iter.From(&regattapb.ResponseOp_Range{
				Count: 1000,
				Kvs: func() []*regattapb.KeyValue {
					kvs := iter.Collect(generateSequence(1000, func(n int) *regattapb.KeyValue {
						return &regattapb.KeyValue{
							Key:   []byte(fmt.Sprintf("key/%d", n)),
							Value: []byte("foo"),
						}
					}))
					slices.SortFunc(kvs, func(a, b *regattapb.KeyValue) int {
						return bytes.Compare(a.Key, b.Key)
					})
					return kvs
				}(),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
			require.NoError(t, err)
			iter.Consume(tt.args.data, func(kv *regattapb.KeyValue) {
				kk := mustEncodeKey(key.Key{
					KeyType: key.TypeUser,
					Key:     kv.Key,
				})
				require.NoError(t, db.Set(kk, kv.Value, pebble.Sync))
			})
			i, err := iterate(db, tt.args.req)
			require.Equal(t, iter.Collect(tt.want), iter.Collect(i))
			require.NoError(t, err)
		})
	}
}

func Test_iterateLargeDataset(t *testing.T) {
	type args struct {
		req  *regattapb.RequestOp_Range
		data iter.Seq[*regattapb.KeyValue]
	}
	tests := []struct {
		name   string
		args   args
		assert func(t *testing.T, seq iter.Seq[*regattapb.ResponseOp_Range])
	}{
		{
			name: "large dataset response split",
			args: args{
				req: &regattapb.RequestOp_Range{
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
				data: generateSequence(10, func(n int) *regattapb.KeyValue {
					return &regattapb.KeyValue{
						Key:   []byte(fmt.Sprintf("key/%d", n)),
						Value: []byte(util.RandString(1024 * 512)),
					}
				}),
			},
			assert: func(t *testing.T, seq iter.Seq[*regattapb.ResponseOp_Range]) {
				col := iter.Collect(seq)
				require.Len(t, col, 2, "should generate 2 chunks")
				require.True(t, col[0].More, "first chunk should have More flag set")
				require.False(t, col[1].More, "last chunk should not have More flag set")
				require.Equal(t, int64(10), col[0].Count+col[1].Count, "should return all items")
			},
		},
		{
			name: "large dataset multi response split",
			args: args{
				req: &regattapb.RequestOp_Range{
					Key:      []byte{0},
					RangeEnd: []byte{0},
				},
				data: generateSequence(100, func(n int) *regattapb.KeyValue {
					return &regattapb.KeyValue{
						Key:   []byte(fmt.Sprintf("key/%d", n)),
						Value: []byte(util.RandString(1024 * 512)),
					}
				}),
			},
			assert: func(t *testing.T, seq iter.Seq[*regattapb.ResponseOp_Range]) {
				chunks := 0
				items := int64(0)
				seq(func(r *regattapb.ResponseOp_Range) bool {
					chunks++
					items += r.Count
					return true
				})
				require.Equal(t, 15, chunks)
				require.Equal(t, int64(100), items, "should return all items")
			},
		},
		{
			name: "large dataset multi response split query limit",
			args: args{
				req: &regattapb.RequestOp_Range{
					Key:      []byte{0},
					RangeEnd: []byte{0},
					Limit:    50,
				},
				data: generateSequence(100, func(n int) *regattapb.KeyValue {
					return &regattapb.KeyValue{
						Key:   []byte(fmt.Sprintf("key/%d", n)),
						Value: []byte(util.RandString(1024 * 512)),
					}
				}),
			},
			assert: func(t *testing.T, seq iter.Seq[*regattapb.ResponseOp_Range]) {
				chunks := 0
				items := int64(0)
				seq(func(r *regattapb.ResponseOp_Range) bool {
					chunks++
					items += r.Count
					return true
				})
				require.Equal(t, 8, chunks)
				require.Equal(t, int64(50), items, "should return all items")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
			require.NoError(t, err)
			iter.Consume(tt.args.data, func(kv *regattapb.KeyValue) {
				kk := mustEncodeKey(key.Key{
					KeyType: key.TypeUser,
					Key:     kv.Key,
				})
				require.NoError(t, db.Set(kk, kv.Value, pebble.Sync))
			})
			i, err := iterate(db, tt.args.req)
			require.NoError(t, err)
			tt.assert(t, i)
		})
	}
}

func Test_iterOptionsForBounds(t *testing.T) {
	type args struct {
		low  []byte
		high []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *pebble.IterOptions
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "empty args",
			args: args{},
			want: &pebble.IterOptions{
				LowerBound: mustEncodeKey(key.Key{KeyType: key.TypeUser}),
				UpperBound: mustEncodeKey(key.Key{KeyType: key.TypeUser}),
			},
			wantErr: require.NoError,
		},
		{
			name: "just lower bound set",
			args: args{
				low: []byte("foo"),
			},
			want: &pebble.IterOptions{
				LowerBound: mustEncodeKey(key.Key{KeyType: key.TypeUser, Key: []byte("foo")}),
				UpperBound: mustEncodeKey(key.Key{KeyType: key.TypeUser}),
			},
			wantErr: require.NoError,
		},
		{
			name: "just upper bound set",
			args: args{
				high: []byte("foo"),
			},
			want: &pebble.IterOptions{
				LowerBound: mustEncodeKey(key.Key{KeyType: key.TypeUser}),
				UpperBound: mustEncodeKey(key.Key{KeyType: key.TypeUser, Key: []byte("foo")}),
			},
			wantErr: require.NoError,
		},
		{
			name: "upper bound wildcard",
			args: args{
				high: wildcard,
			},
			want: &pebble.IterOptions{
				LowerBound: mustEncodeKey(key.Key{KeyType: key.TypeUser}),
				UpperBound: incrementRightmostByte(append([]byte{}, maxUserKey...)),
			},
			wantErr: require.NoError,
		},
		{
			name: "upper bound and lower bound wildcard",
			args: args{
				low:  wildcard,
				high: wildcard,
			},
			want: &pebble.IterOptions{
				LowerBound: mustEncodeKey(key.Key{KeyType: key.TypeUser, Key: wildcard}),
				UpperBound: incrementRightmostByte(append([]byte{}, maxUserKey...)),
			},
			wantErr: require.NoError,
		},
	}

	maxUserKeyCpy := append([]byte{}, maxUserKey...)
	defer require.Equal(t, maxUserKey, maxUserKeyCpy, "invariant violated implicit constant maxUserKey changed")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := iterOptionsForBounds(tt.args.low, tt.args.high)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func generateSequence[V any](n int, gen func(n int) V) iter.Seq[V] {
	return func(yield func(V) bool) {
		for i := 0; i < n; i++ {
			if !yield(gen(i)) {
				return
			}
		}
	}
}
