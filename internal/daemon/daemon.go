package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/server"
	"github.com/usernamenenad/watchd/internal/watch"
)

// ErrSource indicates that the PostgreSQL source failed to start or its
// stream ended, as opposed to a configuration or internal error.
var ErrSource = errors.New("daemon: source failed")

// Run listens on cfg.ListenAddress and serves until ctx is cancelled or the
// source fails. See Serve.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("daemon: listen: %w", err)
	}
	return Serve(ctx, cfg, listener, logger)
}

// Serve runs one watchd process on listener until ctx is cancelled (a clean
// shutdown, returning nil) or the source fails (returning an ErrSource
// error).
//
// Startup: the source starts first - resuming its slot, or waiting for the
// first scope to create one - then the API starts serving, and only then
// does health report SERVING.
//
// Shutdown: health reports NOT_SERVING, every watch ends with Resync, the
// server drains within cfg.ShutdownTimeout, and the source stream stops
// last. The source acknowledges only transactions the hub accepted, so no
// transaction is acknowledged without being accepted, and after a restart
// the slot resumes where this process left off.
func Serve(ctx context.Context, cfg Config, listener net.Listener, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	timeout := time.Duration(cfg.ShutdownTimeout)

	// The hub is the source's sink, and the source is the hub's snapshotter.
	var hub *watch.Hub
	readerConfig := cfg.readerConfig()
	readerConfig.Logger = logger
	source, err := cdc.NewSource(readerConfig, func(ctx context.Context, transaction cdc.Transaction) error {
		return hub.Accept(ctx, transaction)
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	hub, err = watch.New(cfg.hubConfig(), source)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	api, err := server.New(server.Config{SourceID: cfg.SourceID, Logger: logger}, hub)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	grpcServer := grpc.NewServer()
	api.Register(grpcServer)

	// The source outlives ctx: it must keep running while the server drains,
	// and stops only after.
	sourceCtx, stopSource := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSource()
	if err := source.Start(sourceCtx); err != nil {
		_ = listener.Close()
		return fmt.Errorf("%w: start: %w", ErrSource, err)
	}
	logger.Info("source started", "source_id", cfg.SourceID, "slot", cfg.SlotName, "publication", cfg.PublicationName)

	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	api.SetServing(true)
	logger.Info("serving", "address", listener.Addr().String())

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

	api.SetServing(false)
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
