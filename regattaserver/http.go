package regattaserver

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const maxConnectionAge = 60 * time.Second

// RegattaServer is server where grpc/http services can be registered in.
type RegattaServer struct {
	Addr         string
	grpcServer   *grpc.Server
	httpServer   *http.Server
	gwMux        *gwruntime.ServeMux
	gwContext    context.Context
	gwCancelFunc context.CancelFunc
	log          *zap.SugaredLogger
}

// NewServer returns initialized grpc/http server.
func NewServer(addr string, certFilename string, keyFilename string, reflectionAPI bool) *RegattaServer {
	rs := new(RegattaServer)
	rs.Addr = addr
	rs.log = zap.S().Named("server")
	rs.gwContext, rs.gwCancelFunc = context.WithCancel(context.Background())

	var creds credentials.TransportCredentials
	var err error
	if creds, err = credentials.NewServerTLSFromFile(certFilename, keyFilename); err != nil {
		rs.log.Panicf("cannot create server credentials: %v", err)
	}
	grpc_prometheus.EnableHandlingTimeHistogram()
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: maxConnectionAge,
		}),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	}
	rs.grpcServer = grpc.NewServer(opts...)

	if reflectionAPI {
		reflection.Register(rs.grpcServer)
		rs.log.Info("reflection API is active")
	}

	mux := http.NewServeMux()
	rs.gwMux = gwruntime.NewServeMux()

	mux.Handle("/", rs.gwMux)
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
	grpc_prometheus.Register(rs.grpcServer)

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

	cert, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		rs.log.Panicf("failed to parse key pair:", err)
	}

	rs.httpServer = &http.Server{
		Addr:    rs.Addr,
		Handler: grpcHandlerFunc(rs.grpcServer, mux),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		},
		ErrorLog: zap.NewStdLog(rs.log.Desugar()),
	}

	return rs
}

// grpcHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}

// ListenAndServe starts underlying http server.
func (s *RegattaServer) ListenAndServe() error {
	s.log.Infof("listen gRPC/REST on: %s", s.Addr)
	return s.httpServer.ListenAndServeTLS("", "")
}

// Shutdown stops underlying http server.
func (s *RegattaServer) Shutdown(ctx context.Context, d time.Duration) error {
	s.log.Infof("stop gRPC/REST on: %s", s.Addr)
	ctx, cancel := context.WithTimeout(ctx, d)
	defer func() {
		s.gwCancelFunc()
		cancel()
	}()
	return s.httpServer.Shutdown(ctx)
}

// RegisterService implements grpc.ServiceRegistrar interface so internals of this type does not need to be exposed.
func (s *RegattaServer) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	s.grpcServer.RegisterService(desc, impl)
}
