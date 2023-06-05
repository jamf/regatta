// Copyright JAMF Software, LLC

package regattaserver

import (
	gzip2 "compress/gzip"
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/VictoriaMetrics/metrics"
	gometrics "github.com/armon/go-metrics"
	goprometheus "github.com/armon/go-metrics/prometheus"
	"github.com/jamf/regatta/regattaserver/encoding/gzip"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
)

func init() {
	ps, err := goprometheus.NewPrometheusSink()
	if err != nil {
		panic(err)
	}
	if _, err := gometrics.NewGlobal(&gometrics.Config{
		ServiceName:          "regatta", // Use client provided service
		HostName:             "",
		EnableHostname:       false,            // Enable hostname prefix
		EnableRuntimeMetrics: false,            // Enable runtime profiling
		EnableTypePrefix:     false,            // Disable type prefix
		TimerGranularity:     time.Millisecond, // Timers are in milliseconds
		ProfileInterval:      time.Second,      // Poll runtime every second
		FilterDefault:        true,             // Don't filter metrics by default
	}, ps); err != nil {
		panic(err)
	}
}

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

	mux.Handle("/", http.NotFoundHandler())

	l := zap.S().Named("admin")

	return &RESTServer{
		addr: addr,
		httpServer: &http.Server{
			Addr:        addr,
			Handler:     gzip.NewGZIPHandler(mux, gzip2.BestSpeed),
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
