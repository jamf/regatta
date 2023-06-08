// Copyright JAMF Software, LLC

package dns

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestResolver(t *testing.T) {
	ips := []string{
		"127.0.0.1:19091",
		"127.0.0.2:19092",
		"127.0.0.3:19093",
		"127.0.0.4:19094",
		"127.0.0.5:19095",
	}

	res := NewResolver(zap.NewNop().Sugar())
	res.resolver = &mockResolver{
		res: map[string][]string{
			"a": ips[:2],
			"b": ips[2:4],
			"c": {ips[4]},
		},
	}

	ctx := context.TODO()
	err := res.Resolve(ctx, []string{"any+x"})
	require.NoError(t, err)
	result := res.Addresses()
	sort.Strings(result)
	require.Equal(t, []string(nil), result)

	err = res.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, ips, result)

	err = res.Resolve(ctx, []string{"any+b", "any+c"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, ips[2:], result)

	err = res.Resolve(ctx, []string{"any+x"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, []string(nil), result)

	err = res.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, ips, result)

	err = res.Resolve(ctx, []string{"any+b", "example.com:90", "any+c"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, append(ips[2:], "example.com:90"), result)
	err = res.Resolve(ctx, []string{"any+b", "any+c"})
	require.NoError(t, err)
	result = res.Addresses()
	sort.Strings(result)
	require.Equal(t, ips[2:], result)
}

type mockResolver struct {
	res map[string][]string
	err error
}

func (d *mockResolver) Resolve(_ context.Context, name string, _ QType) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.res[name], nil
}
