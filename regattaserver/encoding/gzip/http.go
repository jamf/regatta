// Copyright JAMF Software, LLC

package gzip

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

func NewGZIPHandler(fn http.Handler, level int) http.HandlerFunc {
	gzPool := sync.Pool{
		New: func() interface{} {
			gz, err := gzip.NewWriterLevel(io.Discard, level)
			if err != nil {
				gz = gzip.NewWriter(io.Discard)
			}
			return gz
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !shouldCompress(r) {
			fn.ServeHTTP(w, r)
			return
		}
		gz := gzPool.Get().(*gzip.Writer)
		defer gzPool.Put(gz)
		gz.Reset(w)

		w.Header().Add("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		gw := &gzipWriter{w, gz, 0}
		defer func() {
			_ = gz.Close()
			gw.Header().Add("Content-Length", fmt.Sprint(gw.Size()))
		}()
		fn.ServeHTTP(gw, r)
	}
}

type gzipWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
	size   int
}

func (g *gzipWriter) Write(data []byte) (int, error) {
	if "" == g.Header().Get("Content-Type") {
		g.Header().Set("Content-Type", http.DetectContentType(data))
	}
	g.Header().Del("Content-Length")
	nb, err := g.writer.Write(data)
	g.size += nb
	return nb, err
}

func (g *gzipWriter) WriteHeader(code int) {
	g.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipWriter) Size() int {
	return g.size
}

func shouldCompress(req *http.Request) bool {
	if !strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") ||
		strings.Contains(req.Header.Get("Connection"), "Upgrade") ||
		strings.Contains(req.Header.Get("Accept"), "text/event-stream") {
		return false
	}
	return true
}
