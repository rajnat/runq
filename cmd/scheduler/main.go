package main

import (
	"context"
	"log"
	"os"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/service"
	"github.com/eswar/runq/internal/store"
)

func main() {
	cfg := config.LoadComponent("scheduler")
	logger := log.New(os.Stdout, "scheduler ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(context.Background(), logger, "runq-scheduler", cfg.TraceEndpoint)
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

	observability.RunMetricsServer(context.Background(), logger, cfg.MetricsAddress, metrics)

	scheduler := service.NewScheduler(logger, jobStore, cfg, metrics)
	if err := scheduler.Run(context.Background()); err != nil {
		logger.Fatal(err)
	}
}
