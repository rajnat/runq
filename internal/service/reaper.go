package service

import (
	"context"
	"log"
	"time"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/store"
	"go.opentelemetry.io/otel/attribute"
)

type Reaper struct {
	logger  *log.Logger
	store   *store.Store
	cfg     config.ComponentConfig
	metrics *observability.Registry
}

func NewReaper(logger *log.Logger, store *store.Store, cfg config.ComponentConfig, metrics *observability.Registry) *Reaper {
	return &Reaper{
		logger:  logger,
		store:   store,
		cfg:     cfg,
		metrics: metrics,
	}
}

func (r *Reaper) Run(ctx context.Context) error {
	r.logger.Printf("starting reaper loop with interval %s", r.cfg.TickInterval)

	ticker := time.NewTicker(r.cfg.TickInterval)
	defer ticker.Stop()

	for {
		if err := r.tick(ctx); err != nil {
			r.logger.Printf("reaper tick failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Reaper) tick(ctx context.Context) error {
	started := time.Now()
	defer r.metrics.ObserveHistogram("runq_reaper_tick_duration_seconds", time.Since(started).Seconds())
	ctx, span := observability.Tracer("runq/reaper").Start(ctx, "reaper.tick")
	defer span.End()

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	timedOut, err := r.store.RecoverTimedOutRuns(runCtx, r.cfg.ClaimBatchSize)
	if err != nil {
		r.metrics.IncCounter("runq_reaper_timeout_errors_total")
		return err
	}
	if len(timedOut) > 0 {
		r.metrics.AddCounter("runq_reaper_runs_timed_out_total", uint64(len(timedOut)))
		for _, item := range timedOut {
			if item.AvailableAt != nil {
				r.logger.Printf("timed-out run=%s job=%s action=%s available_at=%s", item.RunID, item.JobID, item.Action, item.AvailableAt.Format(time.RFC3339))
			} else {
				r.logger.Printf("timed-out run=%s job=%s action=%s", item.RunID, item.JobID, item.Action)
			}
		}
	}

	recovered, err := r.store.RecoverExpiredRuns(runCtx, r.cfg.ClaimBatchSize)
	if err != nil {
		r.metrics.IncCounter("runq_reaper_tick_errors_total")
		return err
	}

	if len(recovered) == 0 {
		r.metrics.IncCounter("runq_reaper_ticks_empty_total")
		r.logger.Printf("reaper tick: no expired runs recovered")
		span.SetAttributes(
			attribute.Int("runq.timed_out_runs", len(timedOut)),
			attribute.Int("runq.recovered_runs", len(recovered)),
		)
		return nil
	}

	r.metrics.IncCounter("runq_reaper_ticks_recovered_total")
	r.metrics.AddCounter("runq_reaper_runs_recovered_total", uint64(len(recovered)))
	for _, item := range recovered {
		if item.AvailableAt != nil {
			r.logger.Printf("recovered run=%s job=%s action=%s available_at=%s", item.RunID, item.JobID, item.Action, item.AvailableAt.Format(time.RFC3339))
			continue
		}
		r.logger.Printf("recovered run=%s job=%s action=%s", item.RunID, item.JobID, item.Action)
	}
	span.SetAttributes(
		attribute.Int("runq.timed_out_runs", len(timedOut)),
		attribute.Int("runq.recovered_runs", len(recovered)),
	)

	return nil
}
