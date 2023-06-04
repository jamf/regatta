// Copyright JAMF Software, LLC

package gzip

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jamf/regatta/util"
	"github.com/stretchr/testify/require"
)

func Test_Compress(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type fields struct {
		handler http.Handler
		level   int
	}
	tests := []struct {
		name   string
		args   args
		fields fields
		assert func(t *testing.T, req *http.Request, res *http.Response, err error)
	}{
		{
			name: "compress response default level",
			args: args{
				req: func() *http.Request {
					r, _ := http.NewRequest(http.MethodGet, "", http.NoBody)
					return r
				}(),
			},
			fields: fields{
				handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.WriteHeader(http.StatusOK)
					writer.Write([]byte(util.RandString(1024)))
				}),
				level: -1000,
			},
			assert: func(t *testing.T, req *http.Request, res *http.Response, err error) {
				require.NoError(t, err)
				require.NoError(t, res.Write(io.Discard))
				require.True(t, res.Uncompressed)
			},
		},
		{
			name: "compress response high level",
			args: args{
				req: func() *http.Request {
					r, _ := http.NewRequest(http.MethodGet, "", http.NoBody)
					return r
				}(),
			},
			fields: fields{
				handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Write([]byte(util.RandString(1024)))
				}),
				level: gzip.BestCompression,
			},
			assert: func(t *testing.T, req *http.Request, res *http.Response, err error) {
				require.NoError(t, err)
				require.NoError(t, res.Write(io.Discard))
				require.True(t, res.Uncompressed)
			},
		},
		{
			name: "compress response high level content type",
			args: args{
				req: func() *http.Request {
					r, _ := http.NewRequest(http.MethodGet, "", http.NoBody)
					return r
				}(),
			},
			fields: fields{
				handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Add("Content-Type", "text/plain")
					writer.Write([]byte(util.RandString(1024)))
				}),
				level: gzip.BestCompression,
			},
			assert: func(t *testing.T, req *http.Request, res *http.Response, err error) {
				require.NoError(t, err)
				require.NoError(t, res.Write(io.Discard))
				require.True(t, res.Uncompressed)
			},
		},
		{
			name: "no compress text stream",
			args: args{
				req: func() *http.Request {
					r, _ := http.NewRequest(http.MethodGet, "", http.NoBody)
					r.Header.Add("Accept", "text/event-stream")
					return r
				}(),
			},
			fields: fields{
				handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Write([]byte(util.RandString(1024)))
				}),
				level: gzip.BestCompression,
			},
			assert: func(t *testing.T, req *http.Request, res *http.Response, err error) {
				require.NoError(t, err)
				require.NoError(t, res.Write(io.Discard))
				require.False(t, res.Uncompressed)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(NewGZIPHandler(tt.fields.handler, tt.fields.level))
			t.Cleanup(func() {
				server.Close()
			})
			tt.args.req.URL, _ = url.Parse(server.URL)
			res, err := server.Client().Do(tt.args.req)
			tt.assert(t, tt.args.req, res, err)
		})
	}
}
