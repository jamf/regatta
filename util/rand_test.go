// Copyright JAMF Software, LLC

package util

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandString(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "short",
			args: args{
				n: 5,
			},
		},
		{
			name: "long",
			args: args{
				n: 1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r1 := RandString(tt.args.n)
			r2 := RandString(tt.args.n)
			require.NotEqual(t, r1, r2, "should produce different random strings")
		})
	}
}

func TestRandStrings(t *testing.T) {
	type args struct {
		length int
		count  int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "short",
			args: args{
				length: 5,
				count:  2,
			},
		},
		{
			name: "long",
			args: args{
				length: 1024,
				count:  2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run(tt.name, func(t *testing.T) {
				require.False(t, slices.Equal(RandStrings(tt.args.length, tt.args.count), RandStrings(tt.args.length, tt.args.count)), "should produce different random strings")
			})
		})
	}
}
