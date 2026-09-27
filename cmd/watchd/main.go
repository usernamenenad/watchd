// Command watchd serves the Watch API for one PostgreSQL source.
//
//	WATCHD_DATABASE_URL=postgres://... watchd --config watchd.yaml
//
// Exit codes: 0 after a clean shutdown (SIGINT or SIGTERM), 2 for invalid
// configuration, 3 when the PostgreSQL source fails, 1 for anything else.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/usernamenenad/watchd/internal/daemon"
)

const (
	exitOK = iota
	exitInternal
	exitConfig
	exitSource
)

func main() {
	os.Exit(run())
}

func run() int {
	// --config and -c set the same value. The flag package accepts one or
	// two dashes for any flag, so -config works too.
	var configPath string
	flag.StringVar(&configPath, "config", "watchd.json", "")
	flag.StringVar(&configPath, "c", "watchd.json", "")
	flag.Usage = usage
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := daemon.LoadConfig(configPath)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return exitConfig
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch err := daemon.Run(ctx, cfg, logger); {
	case err == nil:
		return exitOK
	case errors.Is(err, daemon.ErrInvalidConfig):
		logger.Error("invalid configuration", "error", err)
		return exitConfig
	case errors.Is(err, daemon.ErrSource):
		logger.Error("source failed", "error", err)
		return exitSource
	default:
		logger.Error("watchd failed", "error", err)
		return exitInternal
	}
}

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `Usage: watchd [-c|--config PATH]

Serves the Watch API for one PostgreSQL source.

Options:
  -c, --config PATH   configuration file: .json, .yaml, or .yml (default "watchd.json")

Environment:
  %s   PostgreSQL URL of the source (required)
`, daemon.DatabaseURLEnv)
}
