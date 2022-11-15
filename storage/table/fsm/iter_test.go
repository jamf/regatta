// Copyright JAMF Software, LLC

package fsm

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/jamf/regatta/storage/table/key"
	"github.com/stretchr/testify/require"
)

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
