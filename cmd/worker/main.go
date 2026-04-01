package main

import (
	"context"
	"log"
	"os"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/service"
)

func main() {
	cfg := config.LoadWorker()
	logger := log.New(os.Stdout, "worker ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(context.Background(), logger, "runq-worker", cfg.TraceEndpoint)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		_ = shutdownTracing(context.Background())
	}()
	observability.RunMetricsServer(context.Background(), logger, cfg.MetricsAddress, metrics)

	worker := service.NewWorkerProcess(logger, cfg, metrics)
	if err := worker.Run(context.Background()); err != nil {
		logger.Fatal(err)
	}
}
