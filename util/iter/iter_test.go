// Copyright JAMF Software, LLC

package iter

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	type args[S any, R any] struct {
		seq Seq[S]
		fn  func(S) R
	}
	type testCase[S any, R any] struct {
		name string
		args args[S, R]
		want Seq[R]
	}
	tests := []testCase[int, string]{
		{
			name: "empty",
			args: args[int, string]{
				seq: From[int](),
				fn: func(i int) string {
					return strconv.Itoa(i)
				},
			},
			want: From[string](),
		},
		{
			name: "single item",
			args: args[int, string]{
				seq: From(1),
				fn: func(i int) string {
					return strconv.Itoa(i)
				},
			},
			want: From("1"),
		},
		{
			name: "multiple items",
			args: args[int, string]{
				seq: From(1, 2, 3, 4, 5),
				fn: func(i int) string {
					return strconv.Itoa(i)
				},
			},
			want: From("1", "2", "3", "4", "5"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Map(tt.args.seq, tt.args.fn)
			require.Equal(t, Collect(tt.want), Collect(got))
		})
	}
}

func TestPull(t *testing.T) {
	type args[T any] struct {
		seq Seq[T]
	}
	type testCase[T any] struct {
		name string
		args args[T]
		want Seq[T]
	}
	tests := []testCase[int]{
		{
			name: "empty",
			args: args[int]{
				seq: From[int](),
			},
			want: From[int](),
		},
		{
			name: "single item",
			args: args[int]{
				seq: From(1),
			},
			want: From(1),
		},
		{
			name: "multiple items",
			args: args[int]{
				seq: From(1, 2, 3, 4, 5),
			},
			want: From(1, 2, 3, 4, 5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, stop := Pull(tt.args.seq)
			defer stop()
			var items []int
			for {
				i, valid := iter()
				if !valid {
					break
				}
				items = append(items, i)
			}
			require.Equal(t, Collect(tt.want), items)
		})
	}
}
