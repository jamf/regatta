// Copyright JAMF Software, LLC

package cluster

import (
	"testing"

	"github.com/jamf/regatta/raft"
	"github.com/stretchr/testify/assert"
)

func Test_mergeShardInfo(t *testing.T) {
	type args struct {
		current raft.ShardView
		update  raft.ShardView
	}
	tests := []struct {
		name string
		args args
		want raft.ShardView
	}{
		{
			name: "merge empty views",
			args: args{
				current: raft.ShardView{},
				update:  raft.ShardView{},
			},
			want: raft.ShardView{},
		},
		{
			name: "merge with empty view",
			args: args{
				current: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050"},
					ConfigChangeIndex: 5,
					LeaderID:          1,
					Term:              1,
				},
				update: raft.ShardView{},
			},
			want: raft.ShardView{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "address:5050"},
				ConfigChangeIndex: 5,
				LeaderID:          1,
				Term:              1,
			},
		},
		{
			name: "merge with higher config index and term",
			args: args{
				current: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050"},
					ConfigChangeIndex: 5,
					LeaderID:          1,
					Term:              1,
				},
				update: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
					ConfigChangeIndex: 10,
					LeaderID:          2,
					Term:              5,
				},
			},
			want: raft.ShardView{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
				ConfigChangeIndex: 10,
				LeaderID:          2,
				Term:              5,
			},
		},
		{
			name: "skips lower config index update",
			args: args{
				current: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
					ConfigChangeIndex: 10,
					LeaderID:          2,
					Term:              5,
				},
				update: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050"},
					ConfigChangeIndex: 5,
					LeaderID:          1,
					Term:              1,
				},
			},
			want: raft.ShardView{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
				ConfigChangeIndex: 10,
				LeaderID:          2,
				Term:              5,
			},
		},
		{
			name: "skips unknown leader update",
			args: args{
				current: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
					ConfigChangeIndex: 10,
					LeaderID:          2,
					Term:              5,
				},
				update: raft.ShardView{
					ShardID:           1,
					Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
					ConfigChangeIndex: 11,
					LeaderID:          noLeader,
					Term:              10,
				},
			},
			want: raft.ShardView{
				ShardID:           1,
				Replicas:          map[uint64]string{1: "address:5050", 2: "address:5050"},
				ConfigChangeIndex: 11,
				LeaderID:          2,
				Term:              5,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, mergeShardInfo(tt.args.current, tt.args.update), "mergeShardInfo(%v, %v)", tt.args.current, tt.args.update)
		})
	}
}

func Test_shardView_copy(t *testing.T) {
	type fields struct {
		shards map[uint64]raft.ShardView
	}
	tests := []struct {
		name   string
		fields fields
		want   []raft.ShardView
	}{
		{
			name: "copy empty view",
			want: []raft.ShardView{},
		},
		{
			name: "copy single element view",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, Term: 1, LeaderID: 1},
			}},
			want: []raft.ShardView{{ShardID: 1, Term: 1, LeaderID: 1}},
		},
		{
			name: "copy multiple element view",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, Term: 1, LeaderID: 1},
				2: {ShardID: 2, Term: 1, LeaderID: 1},
				3: {ShardID: 3, Term: 1, LeaderID: 1},
			}},
			want: []raft.ShardView{
				{ShardID: 1, Term: 1, LeaderID: 1},
				{ShardID: 2, Term: 1, LeaderID: 1},
				{ShardID: 3, Term: 1, LeaderID: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &shardView{
				shards: tt.fields.shards,
			}
			cp := v.copy()
			assert.Len(t, cp, len(tt.want), "copy()")
			for _, view := range tt.want {
				assert.Containsf(t, cp, view, "copy()")
			}
		})
	}
}

func Test_shardView_shardInfo(t *testing.T) {
	type fields struct {
		shards map[uint64]raft.ShardView
	}
	type args struct {
		id uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   raft.ShardView
	}{
		{
			name: "get from empty view",
			want: raft.ShardView{},
		},
		{
			name: "shard ID miss",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, LeaderID: 1, Term: 1},
			}},
			args: args{id: 50},
			want: raft.ShardView{},
		},
		{
			name: "shard ID hit",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, LeaderID: 1, Term: 1},
				2: {ShardID: 2, LeaderID: 1, Term: 1},
			}},
			args: args{id: 1},
			want: raft.ShardView{ShardID: 1, LeaderID: 1, Term: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &shardView{
				shards: tt.fields.shards,
			}
			assert.Equalf(t, tt.want, v.shardInfo(tt.args.id), "shardInfo(%v)", tt.args.id)
		})
	}
}

func Test_shardView_update(t *testing.T) {
	type fields struct {
		shards map[uint64]raft.ShardView
	}
	type args struct {
		updates []raft.ShardView
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[uint64]raft.ShardView
	}{
		{
			name: "merge empty views",
			args: args{
				updates: []raft.ShardView{},
			},
		},
		{
			name: "merge with empty view",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050"}, ConfigChangeIndex: 5, LeaderID: 1, Term: 1},
			}},
			args: args{
				updates: []raft.ShardView{},
			},
			want: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050"}, ConfigChangeIndex: 5, LeaderID: 1, Term: 1},
			},
		},
		{
			name: "merge with higher config index and term",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050"}, ConfigChangeIndex: 5, LeaderID: 1, Term: 1},
			}},
			args: args{
				updates: []raft.ShardView{{ShardID: 1, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 10, LeaderID: 1, Term: 10}},
			},
			want: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 10, LeaderID: 1, Term: 10},
			},
		},
		{
			name: "merge with multiple updates",
			fields: fields{shards: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 10, LeaderID: 1, Term: 10},
			}},
			args: args{
				updates: []raft.ShardView{
					{ShardID: 1, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 10, LeaderID: 1, Term: 10},
					{ShardID: 2, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 11, LeaderID: 2, Term: 10},
				},
			},
			want: map[uint64]raft.ShardView{
				1: {ShardID: 1, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 10, LeaderID: 1, Term: 10},
				2: {ShardID: 2, Replicas: map[uint64]string{1: "address:5050", 2: "address:5050"}, ConfigChangeIndex: 11, LeaderID: 2, Term: 10},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &shardView{
				shards: tt.fields.shards,
			}
			v.update(tt.args.updates)
			assert.Equalf(t, tt.want, v.shards, "update()")
		})
	}
}
