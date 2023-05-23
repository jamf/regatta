// Copyright JAMF Software, LLC

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_split(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "empty",
			args: args{b: make([]byte, 0)},
			want: 0,
		},
		{
			name: "nil",
			args: args{b: nil},
			want: 0,
		},
		{
			name: "key",
			args: args{b: []byte("key")},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, split(tt.args.b))
		})
	}
}

func TestOpenDB(t *testing.T) {
	type args struct {
		dbdir   string
		options []Option
	}
	tests := []struct {
		name    string
		args    args
		want    *pebble.DB
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "default options",
			args: args{
				dbdir:   t.TempDir(),
				options: nil,
			},
			wantErr: require.NoError,
		},
		{
			name: "memfs",
			args: args{
				dbdir:   "/tmp",
				options: []Option{WithFS(vfs.NewMem())},
			},
			wantErr: require.NoError,
		},
		{
			name: "block cache",
			args: args{
				dbdir:   "/tmp",
				options: []Option{WithFS(vfs.NewMem()), WithCache(pebble.NewCache(1024))},
			},
			wantErr: require.NoError,
		},
		{
			name: "table cache",
			args: args{
				dbdir: "/tmp",
				options: func() []Option {
					cache := pebble.NewCache(1024)
					return []Option{WithFS(vfs.NewMem()), WithCache(cache), WithTableCache(pebble.NewTableCache(cache, 16, 1024))}
				}(),
			},
			wantErr: require.NoError,
		},
		{
			name: "logger",
			args: args{
				dbdir:   "/tmp",
				options: []Option{WithFS(vfs.NewMem()), WithLogger(zap.NewNop().Sugar())},
			},
			wantErr: require.NoError,
		},
		{
			name: "events",
			args: args{
				dbdir:   "/tmp",
				options: []Option{WithFS(vfs.NewMem()), WithEventListener(pebble.EventListener{})},
			},
			wantErr: require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := OpenDB(tt.args.dbdir, tt.args.options...)
			tt.wantErr(t, err)
			require.NoError(t, db.Close())
		})
	}
}
