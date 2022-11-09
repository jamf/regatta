// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
)

// RESTServer is server exposing debug/healthcheck/metrics services of Regatta.
type RESTServer struct {
	addr       string
	httpServer *http.Server
	log        *zap.SugaredLogger
}

// NewRESTServer returns initialized REST server.
func NewRESTServer(addr string, readTimeout time.Duration) *RESTServer {
	mux := http.NewServeMux()
	// expose the registered metrics at `/metrics` path.
	mux.HandleFunc("/metrics", func(resp http.ResponseWriter, req *http.Request) {
		metrics.WritePrometheus(resp, true)
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		enc := expfmt.NewEncoder(resp, expfmt.FmtText)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	})

	// expose healthcheck on `/healtz` path.
	mux.HandleFunc("/healthz", func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(http.StatusOK)
	})

	// expose pprof
	mux.HandleFunc("/debug/pprof", pprof.Index)
	mux.Handle("/debug/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/block", pprof.Handler("block"))
	mux.Handle("/debug/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/heap", pprof.Handler("heap"))
	mux.Handle("/debug/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/threadcreate", pprof.Handler("threadcreate"))
	mux.HandleFunc("/debug/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/profile", pprof.Profile)
	mux.HandleFunc("/debug/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/trace", pprof.Trace)

	l := zap.S().Named("admin")

	return &RESTServer{
		addr: addr,
		httpServer: &http.Server{
			Addr:        addr,
			Handler:     mux,
			ErrorLog:    zap.NewStdLog(l.Desugar()),
			ReadTimeout: readTimeout,
		},
		log: l,
	}
}

// ListenAndServe starts underlying HTTP server.
func (s *RESTServer) ListenAndServe() error {
	s.log.Infof("listen REST on: %s", s.addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown stops underlying HTTP server.
func (s *RESTServer) Shutdown() {
	s.log.Infof("stopping REST on: %s", s.addr)
	_ = s.httpServer.Shutdown(context.TODO())
	s.log.Infof("stopped REST on: %s", s.addr)
}
