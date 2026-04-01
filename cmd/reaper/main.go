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
	cfg := config.LoadComponent("reaper")
	logger := log.New(os.Stdout, "reaper ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(context.Background(), logger, "runq-reaper", cfg.TraceEndpoint)
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

	reaper := service.NewReaper(logger, jobStore, cfg, metrics)
	if err := reaper.Run(context.Background()); err != nil {
		logger.Fatal(err)
	}
}
