package ops

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func get(t *testing.T, handler http.Handler, path string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(recorder.Result().Body)
	return recorder.Code, string(body)
}

func TestReadyzFollowsReady(t *testing.T) {
	var ready atomic.Bool
	handler := Handler(Config{Ready: ready.Load})

	if code, _ := get(t, handler, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready = %d, want 503", code)
	}
	ready.Store(true)
	if code, _ := get(t, handler, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz while ready = %d, want 200", code)
	}
	ready.Store(false)
	if code, _ := get(t, handler, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz after ready = %d, want 503", code)
	}
	// Liveness does not depend on readiness.
	if code, _ := get(t, handler, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}
}

func TestMetricsServedWhenConfigured(t *testing.T) {
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("watchd_serving 1\n")) })
	if code, body := get(t, Handler(Config{Metrics: metrics}), "/metrics"); code != http.StatusOK || !strings.Contains(body, "watchd_serving") {
		t.Fatalf("/metrics = %d %q", code, body)
	}
	if code, _ := get(t, Handler(Config{}), "/metrics"); code != http.StatusNotFound {
		t.Fatalf("/metrics without exporter = %d, want 404", code)
	}
}

func TestPprofOffByDefault(t *testing.T) {
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline"} {
		if code, _ := get(t, Handler(Config{}), path); code != http.StatusNotFound {
			t.Errorf("%s with pprof off = %d, want 404", path, code)
		}
	}
	if code, body := get(t, Handler(Config{Pprof: true}), "/debug/pprof/"); code != http.StatusOK || !strings.Contains(body, "goroutine") {
		t.Fatalf("/debug/pprof/ with pprof on = %d", code)
	}
}
