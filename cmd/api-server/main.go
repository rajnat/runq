package main

import (
	"context"
	"log"
	"os"

	"github.com/eswar/runq/internal/api"
	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/store"
)

func main() {
	cfg := config.LoadAPI()

	logger := log.New(os.Stdout, "api-server ", log.LstdFlags|log.LUTC)
	metrics := observability.NewRegistry()
	shutdownTracing, err := observability.InitTracing(context.Background(), logger, "runq-api-server", cfg.TraceEndpoint)
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

	server, err := api.NewServer(cfg, logger, jobStore, metrics)
	if err != nil {
		logger.Fatal(err)
	}
	if err := server.Run(); err != nil {
		logger.Fatal(err)
	}
}
