package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncMap_ComputeIfAbsent(t *testing.T) {
	type fields struct {
		m map[string]string
	}
	type args struct {
		key     string
		valFunc func(string) string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "compute missing key",
			args: args{
				key: "key",
				valFunc: func(s string) string {
					return "val"
				},
			},
			want: "val",
		},
		{
			name:   "return existing key",
			fields: fields{m: map[string]string{"key": "old"}},
			args: args{
				key: "key",
				valFunc: func(s string) string {
					return "val"
				},
			},
			want: "old",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SyncMap[string, string]{
				m: tt.fields.m,
			}
			got := s.ComputeIfAbsent(tt.args.key, tt.args.valFunc)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSyncMap_Delete(t *testing.T) {
	type fields struct {
		m map[string]string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		assert func(*testing.T, *SyncMap[string, string])
	}{
		{
			name: "delete non-existent",
			args: args{key: "key"},
			assert: func(t *testing.T, s *SyncMap[string, string]) {
				require.Empty(t, s.m)
			},
		},
		{
			name:   "delete existing key",
			fields: fields{m: map[string]string{"key": "val"}},
			args:   args{key: "key"},
			assert: func(t *testing.T, s *SyncMap[string, string]) {
				require.Empty(t, s.m)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SyncMap[string, string]{
				m: tt.fields.m,
			}
			s.Delete(tt.args.key)
			tt.assert(t, s)
		})
	}
}

func TestSyncMap_Load(t *testing.T) {
	type fields struct {
		m map[string]string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		wantOK bool
	}{
		{
			name: "load empty map",
		},
		{
			name: "load missing key",
			args: args{key: "key"},
		},
		{
			name:   "load existing key",
			fields: fields{m: map[string]string{"key": "val"}},
			args:   args{key: "key"},
			wantOK: true,
			want:   "val",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SyncMap[string, string]{
				m: tt.fields.m,
			}
			got, ok := s.Load(tt.args.key)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestSyncMap_Store(t *testing.T) {
	type fields struct {
		m map[string]string
	}
	type args struct {
		key string
		val string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		assert func(*testing.T, *SyncMap[string, string])
	}{
		{
			name: "store into empty map",
			args: args{
				key: "key",
				val: "value",
			},
			assert: func(t *testing.T, s *SyncMap[string, string]) {
				require.Equal(t, "value", s.m["key"])
			},
		},
		{
			name:   "replace existing key",
			fields: fields{m: map[string]string{"key": "old"}},
			args: args{
				key: "key",
				val: "value",
			},
			assert: func(t *testing.T, s *SyncMap[string, string]) {
				require.Equal(t, "value", s.m["key"])
			},
		},
		{
			name:   "add key",
			fields: fields{m: map[string]string{"key": "old"}},
			args: args{
				key: "key2",
				val: "value",
			},
			assert: func(t *testing.T, s *SyncMap[string, string]) {
				require.Equal(t, "old", s.m["key"])
				require.Equal(t, "value", s.m["key2"])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SyncMap[string, string]{
				m: tt.fields.m,
			}
			s.Store(tt.args.key, tt.args.val)
			tt.assert(t, s)
		})
	}
}
