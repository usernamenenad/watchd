// Package ops serves watchd's operational HTTP endpoints - metrics, health,
// and opt-in profiling - on a listener separate from the gRPC API.
package ops

import (
	"net/http"
	"net/http/pprof"
)

// Config configures Handler.
type Config struct {
	// Metrics serves /metrics. When nil, /metrics is not found.
	Metrics http.Handler
	// Ready reports readiness for /readyz: true only while watchd serves
	// its contract, the same state as gRPC health SERVING.
	Ready func() bool
	// Pprof exposes /debug/pprof/*. It reveals internals such as stack
	// traces and command-line flags, so it is off unless enabled.
	Pprof bool
}

// Handler returns the ops endpoint:
//
//   - /metrics: Prometheus exposition.
//   - /healthz: liveness, 200 while the process serves HTTP at all.
//   - /readyz: readiness, 200 only while Ready reports true, else 503.
//   - /debug/pprof/*: Go profiles, only when Pprof is set.
func Handler(cfg Config) http.Handler {
	mux := http.NewServeMux()
	if cfg.Metrics != nil {
		mux.Handle("GET /metrics", cfg.Metrics)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if cfg.Ready != nil && cfg.Ready() {
			writeStatus(w, http.StatusOK, "ready")
			return
		}
		writeStatus(w, http.StatusServiceUnavailable, "not ready")
	})
	if cfg.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

func writeStatus(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
