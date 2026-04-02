package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/service"
	"github.com/eswar/runq/internal/store"
)

func main() {
	cfg := config.LoadComponent("reaper")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := log.New(os.Stdout, "reaper ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(ctx, logger, "runq-reaper", cfg.TraceEndpoint)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		_ = shutdownTracing(context.Background())
	}()
	jobStore, err := store.Open(cfg.DBConnString)
	if err != nil {
		logger.Fatal(err)
	}
	defer jobStore.Close()

	observability.RunMetricsServer(ctx, logger, cfg.MetricsAddress, metrics)

	reaper := service.NewReaper(logger, jobStore, cfg, metrics)
	if err := reaper.Run(ctx); err != nil && err != context.Canceled {
		logger.Fatal(err)
	}
}
