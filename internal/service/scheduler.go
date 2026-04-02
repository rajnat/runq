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

type Scheduler struct {
	logger  *log.Logger
	store   *store.Store
	cfg     config.ComponentConfig
	metrics *observability.Registry
}

func NewScheduler(logger *log.Logger, store *store.Store, cfg config.ComponentConfig, metrics *observability.Registry) *Scheduler {
	return &Scheduler{
		logger:  logger,
		store:   store,
		cfg:     cfg,
		metrics: metrics,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.logger.Printf("starting scheduler loop with interval %s", s.cfg.TickInterval)

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		if err := s.tick(ctx); err != nil {
			s.logger.Printf("scheduler tick failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) error {
	started := time.Now()
	defer s.metrics.ObserveHistogram("runq_scheduler_tick_duration_seconds", time.Since(started).Seconds())
	ctx, span := observability.Tracer("runq/scheduler").Start(ctx, "scheduler.tick")
	defer span.End()

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	materialized, err := s.store.MaterializeDueRuns(runCtx, s.cfg.ClaimBatchSize)
	if err != nil {
		s.metrics.IncCounter("runq_scheduler_materialize_errors_total")
		return err
	}
	if materialized > 0 {
		s.metrics.AddCounter("runq_scheduler_runs_materialized_total", uint64(materialized))
		s.logger.Printf("materialized due runs count=%d", materialized)
	}

	assignments, summary, err := s.store.ClaimPendingRuns(runCtx, s.cfg.ClaimBatchSize, s.cfg.LeaseDuration, s.cfg.TenantMaxInflight)
	if err != nil {
		s.metrics.IncCounter("runq_scheduler_tick_errors_total")
		return err
	}

	s.metrics.SetGauge("runq_scheduler_candidate_runs", int64(summary.CandidateRuns))
	s.metrics.SetGauge("runq_scheduler_eligible_workers", int64(summary.EligibleWorkers))
	s.metrics.SetGauge("runq_scheduler_saturated_workers", int64(summary.SaturatedWorkers))
	s.metrics.SetGaugeVec("runq_scheduler_skipped_runs_last_tick", map[string]string{"reason": "no_capacity"}, float64(summary.SkippedNoCapacity))
	s.metrics.SetGaugeVec("runq_scheduler_skipped_runs_last_tick", map[string]string{"reason": "no_worker"}, float64(summary.SkippedNoEligibleWorker))
	s.metrics.SetGaugeVec("runq_scheduler_skipped_runs_last_tick", map[string]string{"reason": "tenant_limit"}, float64(summary.SkippedTenantLimit))
	s.metrics.AddCounterVec("runq_scheduler_skipped_runs_total", map[string]string{"reason": "no_capacity"}, float64(summary.SkippedNoCapacity))
	s.metrics.AddCounterVec("runq_scheduler_skipped_runs_total", map[string]string{"reason": "no_worker"}, float64(summary.SkippedNoEligibleWorker))
	s.metrics.AddCounterVec("runq_scheduler_skipped_runs_total", map[string]string{"reason": "tenant_limit"}, float64(summary.SkippedTenantLimit))
	s.metrics.SetGaugeVec("runq_scheduler_tenant_groups_last_tick", map[string]string{"state": "candidate"}, float64(len(summary.TenantCandidates)))
	s.metrics.SetGaugeVec("runq_scheduler_tenant_groups_last_tick", map[string]string{"state": "assigned"}, float64(len(summary.TenantAssigned)))
	s.metrics.SetGaugeVec("runq_scheduler_tenant_groups_last_tick", map[string]string{"state": "skipped"}, float64(len(summary.TenantSkipped)))
	s.metrics.SetGaugeVec("runq_scheduler_tenant_groups_last_tick", map[string]string{"state": "inflight"}, float64(len(summary.TenantInflight)))
	s.metrics.SetGaugeVec("runq_scheduler_tenant_groups_last_tick", map[string]string{"state": "quota_configured"}, float64(len(summary.TenantQuota)))
	s.metrics.SetGaugeVec("runq_scheduler_queue_groups_last_tick", map[string]string{"state": "candidate"}, float64(len(summary.QueueCandidates)))
	s.metrics.SetGaugeVec("runq_scheduler_queue_groups_last_tick", map[string]string{"state": "assigned"}, float64(len(summary.QueueAssigned)))
	s.metrics.SetGaugeVec("runq_scheduler_queue_groups_last_tick", map[string]string{"state": "skipped"}, float64(len(summary.QueueSkipped)))

	if len(assignments) == 0 {
		s.metrics.IncCounter("runq_scheduler_ticks_empty_total")
		s.logger.Printf(
			"scheduler tick: no pending runs claimed candidates=%d eligible_workers=%d skipped_no_worker=%d skipped_no_capacity=%d skipped_tenant_limit=%d",
			summary.CandidateRuns,
			summary.EligibleWorkers,
			summary.SkippedNoEligibleWorker,
			summary.SkippedNoCapacity,
			summary.SkippedTenantLimit,
		)
		span.SetAttributes(
			attribute.Int("runq.candidate_runs", summary.CandidateRuns),
			attribute.Int("runq.assigned_runs", len(assignments)),
		)
		return nil
	}

	s.metrics.IncCounter("runq_scheduler_ticks_claimed_total")
	s.metrics.AddCounter("runq_scheduler_runs_claimed_total", uint64(len(assignments)))
	s.metrics.SetGauge("runq_scheduler_last_claim_batch_size", int64(len(assignments)))
	for _, assignment := range assignments {
		s.logger.Printf(
			"claimed run=%s job=%s worker=%s lease_token=%d lease_expires_at=%s",
			assignment.RunID,
			assignment.JobID,
			assignment.WorkerID,
			assignment.LeaseToken,
			assignment.LeaseExpiry.Format(time.RFC3339),
		)
	}
	span.SetAttributes(
		attribute.Int("runq.candidate_runs", summary.CandidateRuns),
		attribute.Int("runq.assigned_runs", len(assignments)),
	)

	return nil
}
