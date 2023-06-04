// Copyright JAMF Software, LLC

package regattaserver

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRESTServer(t *testing.T) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, l.Close())
	srv := NewRESTServer(l.Addr().String(), 30*time.Second)
	go srv.ListenAndServe()
	srv.Shutdown()
}

func TestRESTServer(t *testing.T) {
	type args struct {
		method  string
		path    string
		headers map[string][]string
	}
	tests := []struct {
		name     string
		args     args
		wantCode int
	}{
		{
			name: "not found",
			args: args{
				method: "GET",
				path:   "/something",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "prometheus metrics",
			args: args{
				method: "GET",
				path:   "/metrics",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "prometheus metrics gzipped",
			args: args{
				method: "GET",
				path:   "/metrics",
				headers: map[string][]string{
					"Accept-Encoding": {"gzip"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "healthcheck",
			args: args{
				method: "GET",
				path:   "/healthz",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown method",
			args: args{
				method: "FOO",
				path:   "/",
			},
			wantCode: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			if len(tt.args.headers) > 0 {
				request.Header = tt.args.headers
			}
			response := httptest.NewRecorder()
			s := NewRESTServer("127.0.0.1:3000", 30*time.Second)
			s.httpServer.Handler.ServeHTTP(response, request)
			require.Equal(t, tt.wantCode, response.Code)
		})
	}
}
