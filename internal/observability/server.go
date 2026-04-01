package observability

import (
	"context"
	"log"
	"net/http"
	"time"
)

func RunMetricsServer(ctx context.Context, logger *log.Logger, address string, registry *Registry) {
	if address == "" {
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Printf("starting metrics server on %s", address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("metrics server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}
