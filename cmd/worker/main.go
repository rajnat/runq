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
)

func main() {
	cfg := config.LoadWorker()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := log.New(os.Stdout, "worker ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(ctx, logger, "runq-worker", cfg.TraceEndpoint)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		_ = shutdownTracing(context.Background())
	}()
	observability.RunMetricsServer(ctx, logger, cfg.MetricsAddress, metrics)

	worker := service.NewWorkerProcess(logger, cfg, metrics)
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		logger.Fatal(err)
	}
}
