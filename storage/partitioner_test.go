package storage

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wandera/regatta/util"
)

func TestHashPartitioner_Capacity(t *testing.T) {
	type fields struct {
		capacity uint64
	}
	tests := []struct {
		name      string
		fields    fields
		want      uint64
		wantPanic bool
	}{
		{
			"Zero capacity",
			fields{capacity: 0},
			0,
			true,
		},
		{
			"Some capacity",
			fields{capacity: 64},
			64,
			false,
		},
		{
			"Maximum capacity",
			fields{capacity: math.MaxUint64},
			math.MaxUint64,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			if tt.wantPanic {
				r.Panics(func() { NewHashPartitioner(tt.fields.capacity) })
				return
			}
			p := NewHashPartitioner(tt.fields.capacity)
			r.Equal(tt.want, p.Capacity())
		})
	}
}

func TestHashPartitioner_ClusterID(t *testing.T) {
	type fields struct {
		capacity uint64
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      uint64
		wantPanic bool
	}{
		{
			"Zero capacity",
			fields{capacity: 0},
			args{key: []byte("key")},
			0,
			true,
		},
		{
			"Single shard",
			fields{capacity: 1},
			args{key: []byte("key")},
			1,
			false,
		},
		{
			"Multiple shards",
			fields{capacity: 10},
			args{key: []byte("key")},
			5,
			false,
		},
		{
			"Nil key",
			fields{capacity: 1},
			args{key: nil},
			1,
			false,
		},
		{
			"Prefixed key",
			fields{capacity: 10},
			args{key: []byte("keykey")},
			9,
			false,
		},
		{
			"Large key",
			fields{capacity: 1},
			args{key: []byte(util.RandString(2048))},
			1,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			r := require.New(t)

			if tt.wantPanic {
				r.Panics(func() { NewHashPartitioner(tt.fields.capacity) })
				return
			}
			p := NewHashPartitioner(tt.fields.capacity)
			r.Equal(tt.want, p.ClusterID(tt.args.key))
		})
	}
}

func TestHashPartitioner_Distribution(t *testing.T) {
	type fields struct {
		capacity  uint64
		keyLength int
		keyCount  int
	}
	type want struct {
		maxKeysInShard int
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			"Small capacity - few keys",
			fields{capacity: 10, keyCount: 1000, keyLength: 256},
			want{maxKeysInShard: 1500},
		},
		{
			"Large capacity - few keys",
			fields{capacity: 10_000, keyCount: 1000, keyLength: 256},
			want{maxKeysInShard: 50},
		},
		{
			"Small capacity - lot of keys",
			fields{capacity: 10, keyCount: 1_000_000, keyLength: 256},
			want{maxKeysInShard: 150_000},
		},
		{
			"Large capacity - lot of keys",
			fields{capacity: 10_000, keyCount: 1_000_000, keyLength: 256},
			want{maxKeysInShard: 150},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			p := NewHashPartitioner(tt.fields.capacity)
			keys := util.RandStrings(tt.fields.keyLength, tt.fields.keyCount)
			counts := make(map[uint64]int)
			for _, key := range keys {
				id := p.ClusterID([]byte(key))
				r.Equal(id, p.ClusterID([]byte(key)), "sharding is consistent")

				counts[id] = counts[id] + 1
			}

			c := 0
			for shard, count := range counts {
				c = c + count
				r.LessOrEqual(count, tt.want.maxKeysInShard, "the keys should be evenly spread (Shard: %d Hits: %d)", shard, count)
			}
			r.Equal(tt.fields.keyCount, c, "the number of items should be equal")
		})
	}
}

func BenchmarkHashPartitioner(b *testing.B) {
	b.Run("Small capacity", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		p := NewHashPartitioner(10)
		key := []byte(util.RandString(256))
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			p.ClusterID(key)
		}
	})

	b.Run("Large capacity", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		p := NewHashPartitioner(1000)
		key := []byte(util.RandString(256))
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			p.ClusterID(key)
		}
	})

	b.Run("Large capacity parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		p := NewHashPartitioner(1000)
		key := []byte(util.RandString(256))
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			wg := sync.WaitGroup{}
			wg.Add(100)
			for i := 0; i < 100; i++ {
				go func() {
					p.ClusterID(key)
					wg.Done()
				}()
			}
			wg.Wait()
		}
	})
}
