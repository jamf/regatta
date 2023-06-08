// Copyright JAMF Software, LLC

package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockHostnameResolver struct {
	resultIPs  map[string][]net.IPAddr
	resultSRVs map[string][]*net.SRV
	err        error
}

func (m mockHostnameResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resultIPs[host], nil
}

func (m mockHostnameResolver) LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
	if m.err != nil {
		return "", nil, m.err
	}
	return "", m.resultSRVs[name], nil
}

func TestIsDynamicNode(t *testing.T) {
	for _, tcase := range []struct {
		node      string
		isDynamic bool
	}{
		{
			node:      "1.2.3.4",
			isDynamic: false,
		},
		{
			node:      "gibberish+1.1.1.1+noa",
			isDynamic: true,
		},
		{
			node:      "",
			isDynamic: false,
		},
		{
			node:      "dns+aaa",
			isDynamic: true,
		},
		{
			node:      "dnssrv+asdasdsa",
			isDynamic: true,
		},
	} {
		isDynamic := IsDynamicNode(tcase.node)
		assert.Equal(t, tcase.isDynamic, isDynamic, "mismatch between results")
	}
}

var errorFromResolver = errors.New("error from resolver")

func TestResolver_Resolve(t *testing.T) {
	type args struct {
		addr  string
		qtype QType
	}
	tests := []struct {
		name     string
		args     args
		want     []string
		wantErr  error
		resolver *mockHostnameResolver
	}{
		{
			name: "single ip from dns lookup of host port",
			args: args{
				addr:  "regatta.io:8080",
				qtype: A,
			},
			want:    []string{"192.168.0.1:8080"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultIPs: map[string][]net.IPAddr{
					"regatta.io": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
				},
			},
		},
		// Scheme is preserved.
		{
			name: "single ip from dns lookup of host port with scheme",
			args: args{
				addr:  "http://regatta.io:8080",
				qtype: A,
			},
			want:    []string{"http://192.168.0.1:8080"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultIPs: map[string][]net.IPAddr{
					"regatta.io": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
				},
			},
		},
		{
			name: "error on dns lookup when no port is specified",
			args: args{
				addr:  "regatta.io",
				qtype: A,
			},
			want:     nil,
			wantErr:  errors.New("missing port in address given for dns lookup: regatta.io"),
			resolver: &mockHostnameResolver{},
		},
		{
			name: "multiple SRV records from SRV lookup",
			args: args{
				addr:  "_test._tcp.regatta.io",
				qtype: SRV,
			},
			want:    []string{"192.168.0.1:8080", "192.168.0.2:8081"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.regatta.io": {
						&net.SRV{Target: "alt1.regatta.io.", Port: 8080},
						&net.SRV{Target: "alt2.regatta.io.", Port: 8081},
					},
				},
				resultIPs: map[string][]net.IPAddr{
					"alt1.regatta.io.": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
					"alt2.regatta.io.": {net.IPAddr{IP: net.ParseIP("192.168.0.2")}},
				},
			},
		},
		{
			name: "multiple SRV records from SRV lookup with specified port",
			args: args{
				addr:  "_test._tcp.regatta.io:8082",
				qtype: SRV,
			},
			want:    []string{"192.168.0.1:8082", "192.168.0.2:8082"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.regatta.io": {
						&net.SRV{Target: "alt1.regatta.io.", Port: 8080},
						&net.SRV{Target: "alt2.regatta.io.", Port: 8081},
					},
				},
				resultIPs: map[string][]net.IPAddr{
					"alt1.regatta.io.": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
					"alt2.regatta.io.": {net.IPAddr{IP: net.ParseIP("192.168.0.2")}},
				},
			},
		},
		{
			name: "error from SRV resolver",
			args: args{
				addr:  "_test._tcp.regatta.io",
				qtype: SRV,
			},
			want:     nil,
			wantErr:  fmt.Errorf("%w lookup SRV records \"_test._tcp.regatta.io\"", errorFromResolver),
			resolver: &mockHostnameResolver{err: errorFromResolver},
		},
		{
			name: "error from SRV resolver not found",
			args: args{
				addr:  "_test._tcp.regatta.io",
				qtype: SRV,
			},
			want:     nil,
			wantErr:  nil,
			resolver: &mockHostnameResolver{err: &net.DNSError{IsNotFound: true}},
		},
		{
			name: "error from A resolver not found",
			args: args{
				addr:  "http://regatta.io:8080",
				qtype: A,
			},
			want:     nil,
			wantErr:  nil,
			resolver: &mockHostnameResolver{err: &net.DNSError{IsNotFound: true}},
		},
		{
			name: "multiple SRV records from SRV no A lookup",
			args: args{
				addr:  "_test._tcp.regatta.io",
				qtype: SRVNoA,
			},
			want:    []string{"192.168.0.1:8080", "192.168.0.2:8081"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.regatta.io": {
						&net.SRV{Target: "192.168.0.1", Port: 8080},
						&net.SRV{Target: "192.168.0.2", Port: 8081},
					},
				},
			},
		},
		{
			name: "multiple SRV records from SRV no A lookup with specified port",
			args: args{
				addr:  "_test._tcp.regatta.io:8082",
				qtype: SRVNoA,
			},
			want:    []string{"192.168.0.1:8082", "192.168.0.2:8082"},
			wantErr: nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.regatta.io": {
						&net.SRV{Target: "192.168.0.1", Port: 8080},
						&net.SRV{Target: "192.168.0.2", Port: 8081},
					},
				},
			},
		},
		{
			name: "error from SRV no A lookup",
			args: args{
				addr:  "_test._tcp.regatta.io",
				qtype: SRV,
			},
			want:     nil,
			wantErr:  fmt.Errorf("%w lookup SRV records \"_test._tcp.regatta.io\"", errorFromResolver),
			resolver: &mockHostnameResolver{err: errorFromResolver},
		},
		{
			name: "error on bad qtype",
			args: args{
				addr:  "regatta.io",
				qtype: "invalid",
			},
			want:     nil,
			wantErr:  errors.New("invalid lookup scheme \"invalid\""),
			resolver: &mockHostnameResolver{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			dnsSD := dnsResolver{tt.resolver, zap.NewNop().Sugar()}
			result, err := dnsSD.Resolve(context.TODO(), tt.args.addr, tt.args.qtype)
			if tt.wantErr != nil {
				r.EqualError(err, tt.wantErr.Error())
			} else {
				r.NoError(err)
			}
			sort.Strings(result)
			r.Equal(tt.want, result)
		})
	}
}
