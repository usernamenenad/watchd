package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"

	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/ops"
	"github.com/usernamenenad/watchd/internal/server"
	"github.com/usernamenenad/watchd/internal/telemetry"
	"github.com/usernamenenad/watchd/internal/watch"
)

const meterName = "github.com/usernamenenad/watchd/internal/daemon"

// ErrSource indicates that the PostgreSQL source failed to start or its
// stream ended, as opposed to a configuration or internal error.
var ErrSource = errors.New("daemon: source failed")

// Listeners are the sockets one watchd process serves on.
type Listeners struct {
	// API serves the gRPC Watch API and gRPC health. It is required.
	API net.Listener
	// Ops serves the ops HTTP endpoint (see OpsConfig). Nil disables it.
	Ops net.Listener
}

// Run listens on cfg.ListenAddress, and on cfg.Ops.ListenAddress when set,
// and serves until ctx is cancelled or the source fails. See Serve.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	var listeners Listeners
	var err error
	if listeners.API, err = net.Listen("tcp", cfg.ListenAddress); err != nil {
		return fmt.Errorf("daemon: listen: %w", err)
	}
	if cfg.Ops.ListenAddress != "" {
		if listeners.Ops, err = net.Listen("tcp", cfg.Ops.ListenAddress); err != nil {
			_ = listeners.API.Close()
			return fmt.Errorf("daemon: listen ops: %w", err)
		}
	}
	return Serve(ctx, cfg, listeners, logger)
}

// Serve runs one watchd process on listeners until ctx is cancelled (a
// clean shutdown, returning nil) or the source fails (returning an
// ErrSource error). Serve owns the listeners and closes them.
//
// Startup: the ops endpoint starts first, so liveness answers while the
// source starts - resuming its slot, or waiting for the first scope to
// create one. Then the API starts serving, and only then do gRPC health and
// /readyz report SERVING.
//
// Shutdown: health and /readyz report NOT_SERVING, every watch ends with
// Resync, the server drains within cfg.ShutdownTimeout, and the source
// stream stops. The source acknowledges only transactions the hub accepted,
// so no transaction is acknowledged without being accepted, and after a
// restart the slot resumes where this process left off. The ops endpoint
// stops last, and pending metrics are pushed within the same timeout.
func Serve(ctx context.Context, cfg Config, listeners Listeners, logger *slog.Logger) error {
	closeListeners := func() {
		_ = listeners.API.Close()
		if listeners.Ops != nil {
			_ = listeners.Ops.Close()
		}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	timeout := time.Duration(cfg.ShutdownTimeout)

	tel, err := telemetry.Setup(ctx, telemetry.Options{SourceID: cfg.SourceID, Prometheus: listeners.Ops != nil, Logger: logger})
	if err != nil {
		closeListeners()
		if errors.Is(err, telemetry.ErrInvalidEnvironment) {
			return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		return fmt.Errorf("daemon: telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := tel.Shutdown(flushCtx); err != nil {
			logger.Warn("telemetry did not flush", "error", err)
		}
	}()

	// The hub is the source's sink, and the source is the hub's snapshotter.
	var hub *watch.Hub
	readerConfig := cfg.readerConfig()
	readerConfig.Logger = logger
	readerConfig.Meter = tel.Meter("github.com/usernamenenad/watchd/internal/cdc")
	source, err := cdc.NewSource(readerConfig, func(ctx context.Context, transaction cdc.Transaction) error {
		return hub.Accept(ctx, transaction)
	})
	if err != nil {
		closeListeners()
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	hub, err = watch.New(cfg.hubConfig(), source)
	if err != nil {
		closeListeners()
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	api, err := server.New(server.Config{SourceID: cfg.SourceID, Logger: logger}, hub)
	if err != nil {
		closeListeners()
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	grpcServer := grpc.NewServer()
	api.Register(grpcServer)

	// One serving state backs gRPC health, /readyz, and watchd.serving.
	var serving atomic.Bool
	setServing := func(value bool) {
		serving.Store(value)
		api.SetServing(value)
	}
	if err := registerServingGauge(tel.Meter(meterName), &serving); err != nil {
		closeListeners()
		return fmt.Errorf("daemon: telemetry: %w", err)
	}

	opsServer := startOps(cfg.Ops, listeners.Ops, tel, &serving, logger)
	defer stopOps(ctx, opsServer, timeout, logger)

	// The source outlives ctx: it must keep running while the server drains,
	// and stops only after.
	sourceCtx, stopSource := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSource()
	if err := source.Start(sourceCtx); err != nil {
		_ = listeners.API.Close()
		return fmt.Errorf("%w: start: %w", ErrSource, err)
	}
	logger.Info("source started", "source_id", cfg.SourceID, "slot", cfg.SlotName, "publication", cfg.PublicationName)

	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listeners.API) }()
	setServing(true)
	logger.Info("serving", "address", listeners.API.Addr().String())

	var failure error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case <-source.Done():
		failure = fmt.Errorf("%w: stream ended: %w", ErrSource, source.Err())
		logger.Error("source stream ended; shutting down", "error", source.Err())
	case err := <-serveDone:
		failure = fmt.Errorf("daemon: serve: %w", err)
		logger.Error("server stopped; shutting down", "error", err)
	}

	setServing(false)
	// Ending every watch lets the server drain: clients are told to resync,
	// which is what they must do anyway once this process is gone.
	hub.Stop()
	drained := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(timeout):
		logger.Warn("server did not drain in time; closing remaining streams", "timeout", timeout)
		grpcServer.Stop()
		<-drained
	}

	stopSource()
	select {
	case <-source.Done():
	case <-time.After(timeout):
		logger.Warn("source did not stop in time", "timeout", timeout)
	}
	logger.Info("stopped")
	return failure
}

func registerServingGauge(meter metric.Meter, serving *atomic.Bool) error {
	gauge, err := meter.Int64ObservableGauge("watchd.serving",
		metric.WithDescription("1 while watchd serves its contract (gRPC health SERVING and /readyz ready), else 0."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		value := int64(0)
		if serving.Load() {
			value = 1
		}
		observer.ObserveInt64(gauge, value)
		return nil
	}, gauge)
	return err
}

// startOps serves the ops endpoint on listener, or returns nil when there is
// none. The endpoint failing never stops watchd: it is only logged.
func startOps(cfg OpsConfig, listener net.Listener, tel *telemetry.Telemetry, serving *atomic.Bool, logger *slog.Logger) *http.Server {
	if listener == nil {
		return nil
	}
	if cfg.Pprof {
		runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
		runtime.SetBlockProfileRate(cfg.BlockProfileRate)
	}
	opsServer := &http.Server{
		Handler:           ops.Handler(ops.Config{Metrics: tel.Handler(), Ready: serving.Load, Pprof: cfg.Pprof}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := opsServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("ops endpoint stopped", "error", err)
		}
	}()
	logger.Info("ops endpoint serving", "address", listener.Addr().String(), "pprof", cfg.Pprof)
	return opsServer
}

func stopOps(ctx context.Context, opsServer *http.Server, timeout time.Duration, logger *slog.Logger) {
	if opsServer == nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := opsServer.Shutdown(stopCtx); err != nil {
		logger.Warn("ops endpoint did not stop in time", "error", err)
		_ = opsServer.Close()
	}
}
