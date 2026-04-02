package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/eswar/runq/internal/observability"
	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
)

type Store struct {
	db *sql.DB
}

var ErrConflict = errors.New("state conflict")
var ErrAlreadyExists = errors.New("already exists")

type CreateJobInput struct {
	Name                    string
	TenantID                string
	Queue                   string
	Kind                    string
	Payload                 map[string]any
	ScheduleType            string
	CronExpr                string
	Timezone                string
	Priority                int
	MaxRetries              int
	TimeoutSeconds          int
	RetryBackoffBaseSeconds int
	DedupeKey               string
}

type CreateJobResult struct {
	JobID string
	RunID *string
}

type Job struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	TenantID     string     `json:"tenant_id"`
	Queue        string     `json:"queue"`
	Kind         string     `json:"kind"`
	ScheduleType string     `json:"schedule_type"`
	CronExpr     *string    `json:"cron_expr,omitempty"`
	Timezone     *string    `json:"timezone,omitempty"`
	DisabledAt   *time.Time `json:"disabled_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

type JobFilter struct {
	TenantID string
	Queue    string
	Kind     string
	Disabled *bool
}

type RegisterWorkerInput struct {
	Name           string
	Queues         []string
	Capabilities   map[string]any
	MaxConcurrency int
	Metadata       map[string]any
}

type RegisterWorkerResult struct {
	WorkerID string
}

type Worker struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	MaxConcurrency  int       `json:"max_concurrency"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}

type WorkerAssignment struct {
	RunID       string
	JobID       string
	TenantID    string
	Queue       string
	WorkerID    string
	LeaseToken  int64
	AssignedAt  time.Time
	LeaseExpiry time.Time
}

type ClaimSummary struct {
	CandidateRuns           int
	AssignedRuns            int
	EligibleWorkers         int
	SaturatedWorkers        int
	SkippedNoEligibleWorker int
	SkippedNoCapacity       int
	SkippedTenantLimit      int
	TenantCandidates        map[string]int
	TenantAssigned          map[string]int
	TenantSkipped           map[string]int
	TenantInflight          map[string]int
	TenantQuota             map[string]int
	QueueCandidates         map[string]int
	QueueAssigned           map[string]int
	QueueSkipped            map[string]int
}

type TenantQuota struct {
	TenantID    string    `json:"tenant_id"`
	MaxInflight int       `json:"max_inflight"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type AuditEvent struct {
	ID           int64          `json:"id"`
	EventTime    time.Time      `json:"event_time"`
	ActorRole    string         `json:"actor_role"`
	ActorID      *string        `json:"actor_id,omitempty"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	TenantID     *string        `json:"tenant_id,omitempty"`
	Payload      map[string]any `json:"payload"`
}

type AuditEventInput struct {
	ActorRole    string
	ActorID      string
	Action       string
	ResourceType string
	ResourceID   string
	TenantID     string
	Payload      map[string]any
}

type AuditEventFilter struct {
	TenantID     string
	Action       string
	ResourceType string
	Limit        int
}

type eligibleWorker struct {
	ID             string
	Queues         []string
	Capabilities   map[string]bool
	MaxConcurrency int
	Assigned       int
	LastHeartbeat  time.Time
}

type candidateRun struct {
	RunID       string
	JobID       string
	TenantID    string
	Queue       string
	Kind        string
	ScheduledAt time.Time
}

type PolledAssignment struct {
	RunID          string
	JobID          string
	Kind           string
	Payload        map[string]any
	TimeoutSeconds int
	LeaseToken     int64
	LeaseExpiry    time.Time
}

type HeartbeatUpdate struct {
	RunID      string
	LeaseToken int64
	Progress   map[string]any
}

type CompleteRunInput struct {
	WorkerID   string
	RunID      string
	LeaseToken int64
	Result     map[string]any
}

type FailRunInput struct {
	WorkerID     string
	RunID        string
	LeaseToken   int64
	ErrorCode    string
	ErrorMessage string
	Retryable    bool
}

type RecoveredRun struct {
	RunID       string
	JobID       string
	Action      string
	AvailableAt *time.Time
}

type Run struct {
	ID              string         `json:"id"`
	JobID           string         `json:"job_id"`
	TenantID        string         `json:"tenant_id"`
	Status          string         `json:"status"`
	Attempt         int            `json:"attempt"`
	ScheduledAt     time.Time      `json:"scheduled_at"`
	AvailableAt     time.Time      `json:"available_at"`
	StartedAt       *time.Time     `json:"started_at,omitempty"`
	CompletedAt     *time.Time     `json:"completed_at,omitempty"`
	WorkerID        *string        `json:"worker_id,omitempty"`
	LeaseToken      int64          `json:"lease_token"`
	LeaseExpiresAt  *time.Time     `json:"lease_expires_at,omitempty"`
	LastHeartbeatAt *time.Time     `json:"last_heartbeat_at,omitempty"`
	Result          map[string]any `json:"result,omitempty"`
	ErrorCode       *string        `json:"error_code,omitempty"`
	ErrorMessage    *string        `json:"error_message,omitempty"`
}

type RunEvent struct {
	EventType string         `json:"event_type"`
	EventTime time.Time      `json:"event_time"`
	ActorType string         `json:"actor_type"`
	ActorID   *string        `json:"actor_id,omitempty"`
	Payload   map[string]any `json:"payload"`
}

type RunFilter struct {
	TenantID string
	Status   string
	Queue    string
	WorkerID string
	JobID    string
}

type CancelJobResult struct {
	JobID        string
	CanceledRuns int64
	Disabled     bool
}

func Open(databaseURL string) (*Store, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) CreateJob(ctx context.Context, input CreateJobInput) (CreateJobResult, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.create_job")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.tenant_id", defaultTenantID(input.TenantID)),
		attribute.String("runq.queue", input.Queue),
		attribute.String("runq.kind", input.Kind),
		attribute.String("runq.schedule_type", input.ScheduleType),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CreateJobResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	jobID, err := newID("job")
	if err != nil {
		return CreateJobResult{}, err
	}
	tenantID := defaultTenantID(input.TenantID)

	if strings.TrimSpace(input.DedupeKey) != "" {
		var existingJobID string
		err := tx.QueryRowContext(ctx, `
			SELECT id
			FROM jobs
			WHERE tenant_id = $1
			  AND dedupe_key = $2
			  AND disabled_at IS NULL
			LIMIT 1
		`, tenantID, strings.TrimSpace(input.DedupeKey)).Scan(&existingJobID)
		if err == nil {
			return CreateJobResult{}, ErrAlreadyExists
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return CreateJobResult{}, fmt.Errorf("check dedupe key: %w", err)
		}
	}

	payloadJSON, err := json.Marshal(input.Payload)
	if err != nil {
		return CreateJobResult{}, fmt.Errorf("marshal payload: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, name, tenant_id, queue, kind, payload, schedule_type, priority,
			max_retries, timeout_seconds, retry_backoff_base_seconds, dedupe_key,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $13)
	`,
		jobID,
		input.Name,
		tenantID,
		input.Queue,
		input.Kind,
		string(payloadJSON),
		input.ScheduleType,
		input.Priority,
		input.MaxRetries,
		input.TimeoutSeconds,
		input.RetryBackoffBaseSeconds,
		input.DedupeKey,
		now,
	)
	if err != nil {
		if isUniqueViolation(err, "idx_jobs_active_dedupe_key") {
			return CreateJobResult{}, ErrAlreadyExists
		}
		return CreateJobResult{}, fmt.Errorf("insert job: %w", err)
	}

	if input.ScheduleType == "cron" {
		nextRunAt, err := computeNextRun(input.CronExpr, input.Timezone, now)
		if err != nil {
			return CreateJobResult{}, err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_schedules (
				job_id, cron_expr, timezone, next_run_at, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $5)
		`, jobID, input.CronExpr, input.Timezone, nextRunAt, now)
		if err != nil {
			return CreateJobResult{}, fmt.Errorf("insert schedule: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return CreateJobResult{}, fmt.Errorf("commit schedule tx: %w", err)
		}

		return CreateJobResult{JobID: jobID}, nil
	}

	runID, err := newID("run")
	if err != nil {
		return CreateJobResult{}, err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO runs (
			id, job_id, status, attempt, scheduled_at, available_at, created_at, updated_at
		)
		VALUES ($1, $2, 'PENDING', 0, NOW(), NOW(), NOW(), NOW())
	`, runID, jobID)
	if err != nil {
		return CreateJobResult{}, fmt.Errorf("insert run: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO run_events (run_id, event_type, actor_type, payload)
		VALUES ($1, 'RUN_CREATED', 'api', '{}'::jsonb)
	`, runID)
	if err != nil {
		return CreateJobResult{}, fmt.Errorf("insert run event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return CreateJobResult{}, fmt.Errorf("commit tx: %w", err)
	}

	return CreateJobResult{
		JobID: jobID,
		RunID: &runID,
	}, nil
}

func (s *Store) ListJobs(ctx context.Context, filter JobFilter) ([]Job, error) {
	query := `
		SELECT j.id, j.name, j.tenant_id, j.queue, j.kind, j.schedule_type, js.cron_expr, js.timezone, j.disabled_at, j.created_at
		FROM jobs j
		LEFT JOIN job_schedules js ON js.job_id = j.id
		WHERE 1=1
	`
	args := make([]any, 0)
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		query += fmt.Sprintf(" AND j.tenant_id = $%d", len(args))
	}
	if filter.Queue != "" {
		args = append(args, filter.Queue)
		query += fmt.Sprintf(" AND j.queue = $%d", len(args))
	}
	if filter.Kind != "" {
		args = append(args, filter.Kind)
		query += fmt.Sprintf(" AND j.kind = $%d", len(args))
	}
	if filter.Disabled != nil {
		if *filter.Disabled {
			query += " AND j.disabled_at IS NOT NULL"
		} else {
			query += " AND j.disabled_at IS NULL"
		}
	}
	query += " ORDER BY j.created_at DESC LIMIT 100"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		var cronExpr sql.NullString
		var timezone sql.NullString
		var disabledAt sql.NullTime
		if err := rows.Scan(&job.ID, &job.Name, &job.TenantID, &job.Queue, &job.Kind, &job.ScheduleType, &cronExpr, &timezone, &disabledAt, &job.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		if cronExpr.Valid {
			value := cronExpr.String
			job.CronExpr = &value
		}
		if timezone.Valid {
			value := timezone.String
			job.Timezone = &value
		}
		if disabledAt.Valid {
			value := disabledAt.Time
			job.DisabledAt = &value
		}
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return jobs, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT j.id, j.name, j.tenant_id, j.queue, j.kind, j.schedule_type, js.cron_expr, js.timezone, j.disabled_at, j.created_at
		FROM jobs j
		LEFT JOIN job_schedules js ON js.job_id = j.id
		WHERE j.id = $1
	`, jobID)

	var job Job
	var cronExpr sql.NullString
	var timezone sql.NullString
	var disabledAt sql.NullTime
	if err := row.Scan(&job.ID, &job.Name, &job.TenantID, &job.Queue, &job.Kind, &job.ScheduleType, &cronExpr, &timezone, &disabledAt, &job.CreatedAt); err != nil {
		return Job{}, err
	}
	if cronExpr.Valid {
		value := cronExpr.String
		job.CronExpr = &value
	}
	if timezone.Valid {
		value := timezone.String
		job.Timezone = &value
	}
	if disabledAt.Valid {
		value := disabledAt.Time
		job.DisabledAt = &value
	}

	return job, nil
}

func (s *Store) ListRuns(ctx context.Context, filter RunFilter) ([]Run, error) {
	query := `
		SELECT r.id, r.job_id, j.tenant_id, r.status, r.attempt, r.scheduled_at, r.available_at, r.started_at,
		       r.completed_at, r.worker_id, r.lease_token, r.lease_expires_at, r.last_heartbeat_at,
		       r.result, r.error_code, r.error_message
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE 1=1
	`
	args := make([]any, 0)
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		query += fmt.Sprintf(" AND j.tenant_id = $%d", len(args))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		query += fmt.Sprintf(" AND r.status = $%d", len(args))
	}
	if filter.Queue != "" {
		args = append(args, filter.Queue)
		query += fmt.Sprintf(" AND j.queue = $%d", len(args))
	}
	if filter.WorkerID != "" {
		args = append(args, filter.WorkerID)
		query += fmt.Sprintf(" AND r.worker_id = $%d", len(args))
	}
	if filter.JobID != "" {
		args = append(args, filter.JobID)
		query += fmt.Sprintf(" AND r.job_id = $%d", len(args))
	}
	query += " ORDER BY r.created_at DESC LIMIT 100"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query runs: %w", err)
	}
	defer rows.Close()

	runs := make([]Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("runs rows error: %w", err)
	}

	return runs, nil
}

func (s *Store) GetRun(ctx context.Context, runID string) (Run, []RunEvent, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT r.id, r.job_id, j.tenant_id, r.status, r.attempt, r.scheduled_at, r.available_at, r.started_at,
		       completed_at, worker_id, lease_token, lease_expires_at, last_heartbeat_at,
		       result, error_code, error_message
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.id = $1
	`, runID)

	run, err := scanRun(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, nil, sql.ErrNoRows
		}
		return Run{}, nil, err
	}

	eventRows, err := s.db.QueryContext(ctx, `
		SELECT event_type, event_time, actor_type, actor_id, payload
		FROM run_events
		WHERE run_id = $1
		ORDER BY id ASC
	`, runID)
	if err != nil {
		return Run{}, nil, fmt.Errorf("query run events: %w", err)
	}
	defer eventRows.Close()

	events := make([]RunEvent, 0)
	for eventRows.Next() {
		var event RunEvent
		var payloadBytes []byte
		if err := eventRows.Scan(&event.EventType, &event.EventTime, &event.ActorType, &event.ActorID, &payloadBytes); err != nil {
			return Run{}, nil, fmt.Errorf("scan run event: %w", err)
		}
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &event.Payload); err != nil {
				return Run{}, nil, fmt.Errorf("unmarshal run event payload: %w", err)
			}
		}
		if event.Payload == nil {
			event.Payload = map[string]any{}
		}
		events = append(events, event)
	}
	if err := eventRows.Err(); err != nil {
		return Run{}, nil, fmt.Errorf("run events rows error: %w", err)
	}

	return run, events, nil
}

func (s *Store) CancelJob(ctx context.Context, jobID string, reason string, audit *AuditEventInput) (CancelJobResult, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.cancel_job")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.job_id", jobID),
		attribute.String("runq.reason", reason),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CancelJobResult{}, fmt.Errorf("begin cancel job tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET disabled_at = COALESCE(disabled_at, $2),
		    updated_at = $2
		WHERE id = $1
	`, jobID, now)
	if err != nil {
		return CancelJobResult{}, fmt.Errorf("disable job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return CancelJobResult{}, fmt.Errorf("disabled job rows affected: %w", err)
	}
	if affected == 0 {
		return CancelJobResult{}, sql.ErrNoRows
	}

	runResult, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'CANCELED',
		    completed_at = $2,
		    updated_at = $2,
		    cancel_requested_at = $2
		WHERE job_id = $1
		  AND status IN ('PENDING', 'RUNNING')
	`, jobID, now)
	if err != nil {
		return CancelJobResult{}, fmt.Errorf("cancel runs: %w", err)
	}
	canceledRuns, err := runResult.RowsAffected()
	if err != nil {
		return CancelJobResult{}, fmt.Errorf("canceled runs rows affected: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_events (run_id, event_type, actor_type, payload)
		SELECT id, 'RUN_CANCELED', 'api', $2::jsonb
		FROM runs
		WHERE job_id = $1
		  AND cancel_requested_at = $3
	`, jobID, fmt.Sprintf(`{"reason":%q}`, reason), now); err != nil {
		return CancelJobResult{}, fmt.Errorf("insert cancel events: %w", err)
	}

	if err := insertAuditEvent(ctx, tx, now, audit); err != nil {
		return CancelJobResult{}, fmt.Errorf("insert audit event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return CancelJobResult{}, fmt.Errorf("commit cancel job tx: %w", err)
	}

	return CancelJobResult{JobID: jobID, CanceledRuns: canceledRuns, Disabled: true}, nil
}

func (s *Store) RegisterWorker(ctx context.Context, input RegisterWorkerInput) (RegisterWorkerResult, error) {
	capabilitiesJSON, err := json.Marshal(input.Capabilities)
	if err != nil {
		return RegisterWorkerResult{}, fmt.Errorf("marshal capabilities: %w", err)
	}

	metadataJSON, err := json.Marshal(input.Metadata)
	if err != nil {
		return RegisterWorkerResult{}, fmt.Errorf("marshal metadata: %w", err)
	}

	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RegisterWorkerResult{}, fmt.Errorf("begin register worker tx: %w", err)
	}
	defer tx.Rollback()

	var workerID string
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM workers
		WHERE name = $1
		FOR UPDATE
	`, input.Name).Scan(&workerID)
	switch {
	case err == nil:
		if _, err := tx.ExecContext(ctx, `
			UPDATE workers
			SET queues = $2,
			    capabilities = $3::jsonb,
			    max_concurrency = $4,
			    status = 'healthy',
			    last_heartbeat_at = $5,
			    started_at = $5,
			    metadata = $6::jsonb
			WHERE id = $1
		`, workerID, pqArray(input.Queues), string(capabilitiesJSON), input.MaxConcurrency, now, string(metadataJSON)); err != nil {
			return RegisterWorkerResult{}, fmt.Errorf("update worker: %w", err)
		}
	case errors.Is(err, sql.ErrNoRows):
		workerID, err = newID("worker")
		if err != nil {
			return RegisterWorkerResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workers (
				id, name, queues, capabilities, max_concurrency, status,
				last_heartbeat_at, started_at, metadata
			)
			VALUES ($1, $2, $3, $4::jsonb, $5, 'healthy', $6, $6, $7::jsonb)
		`, workerID, input.Name, pqArray(input.Queues), string(capabilitiesJSON), input.MaxConcurrency, now, string(metadataJSON)); err != nil {
			return RegisterWorkerResult{}, fmt.Errorf("insert worker: %w", err)
		}
	default:
		return RegisterWorkerResult{}, fmt.Errorf("lookup worker by name: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return RegisterWorkerResult{}, fmt.Errorf("commit register worker tx: %w", err)
	}

	return RegisterWorkerResult{WorkerID: workerID}, nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, status, max_concurrency, last_heartbeat_at
		FROM workers
		ORDER BY started_at DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, fmt.Errorf("query workers: %w", err)
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		var worker Worker
		if err := rows.Scan(&worker.ID, &worker.Name, &worker.Status, &worker.MaxConcurrency, &worker.LastHeartbeatAt); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workers rows error: %w", err)
	}

	return workers, nil
}

func (s *Store) GetWorker(ctx context.Context, workerID string) (Worker, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, status, max_concurrency, last_heartbeat_at
		FROM workers
		WHERE id = $1
	`, workerID)

	var worker Worker
	if err := row.Scan(&worker.ID, &worker.Name, &worker.Status, &worker.MaxConcurrency, &worker.LastHeartbeatAt); err != nil {
		return Worker{}, err
	}
	return worker, nil
}

func (s *Store) UpsertTenantQuota(ctx context.Context, tenantID string, maxInflight int, audit *AuditEventInput) (TenantQuota, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.upsert_tenant_quota")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.tenant_id", defaultTenantID(tenantID)),
		attribute.Int("runq.max_inflight", maxInflight),
	)

	tenantID = defaultTenantID(tenantID)
	now := time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantQuota{}, fmt.Errorf("begin tenant quota tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO tenant_quotas (tenant_id, max_inflight, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id)
		DO UPDATE SET
			max_inflight = EXCLUDED.max_inflight,
			updated_at = EXCLUDED.updated_at
	`, tenantID, maxInflight, now)
	if err != nil {
		return TenantQuota{}, fmt.Errorf("upsert tenant quota: %w", err)
	}

	if err := insertAuditEvent(ctx, tx, now, audit); err != nil {
		return TenantQuota{}, fmt.Errorf("insert audit event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return TenantQuota{}, fmt.Errorf("commit tenant quota tx: %w", err)
	}

	return TenantQuota{
		TenantID:    tenantID,
		MaxInflight: maxInflight,
		UpdatedAt:   now,
	}, nil
}

func (s *Store) ListTenantQuotas(ctx context.Context) ([]TenantQuota, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT tenant_id, max_inflight, updated_at
		FROM tenant_quotas
		ORDER BY tenant_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query tenant quotas: %w", err)
	}
	defer rows.Close()

	quotas := make([]TenantQuota, 0)
	for rows.Next() {
		var quota TenantQuota
		if err := rows.Scan(&quota.TenantID, &quota.MaxInflight, &quota.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan tenant quota: %w", err)
		}
		quotas = append(quotas, quota)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tenant quota rows error: %w", err)
	}

	return quotas, nil
}

func (s *Store) ListAuditEvents(ctx context.Context, filter AuditEventFilter) ([]AuditEvent, error) {
	query := `
		SELECT id, event_time, actor_role, actor_id, action, resource_type, resource_id, tenant_id, payload
		FROM audit_events
		WHERE 1=1
	`
	args := make([]any, 0, 4)
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	if filter.Action != "" {
		args = append(args, filter.Action)
		query += fmt.Sprintf(" AND action = $%d", len(args))
	}
	if filter.ResourceType != "" {
		args = append(args, filter.ResourceType)
		query += fmt.Sprintf(" AND resource_type = $%d", len(args))
	}

	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()

	events := make([]AuditEvent, 0)
	for rows.Next() {
		var event AuditEvent
		var actorID sql.NullString
		var tenantID sql.NullString
		var payloadBytes []byte
		if err := rows.Scan(&event.ID, &event.EventTime, &event.ActorRole, &actorID, &event.Action, &event.ResourceType, &event.ResourceID, &tenantID, &payloadBytes); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if actorID.Valid {
			value := actorID.String
			event.ActorID = &value
		}
		if tenantID.Valid {
			value := tenantID.String
			event.TenantID = &value
		}
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &event.Payload); err != nil {
				return nil, fmt.Errorf("unmarshal audit event payload: %w", err)
			}
		}
		if event.Payload == nil {
			event.Payload = map[string]any{}
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit events rows error: %w", err)
	}

	return events, nil
}

func (s *Store) ClaimPendingRuns(ctx context.Context, batchSize int, leaseDuration time.Duration, tenantMaxInflight int) ([]WorkerAssignment, ClaimSummary, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.claim_pending_runs")
	defer span.End()
	span.SetAttributes(
		attribute.Int("runq.batch_size", batchSize),
		attribute.Int("runq.tenant_max_inflight", tenantMaxInflight),
		attribute.String("runq.lease_duration", leaseDuration.String()),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("begin claim tx: %w", err)
	}
	defer tx.Rollback()

	summary := ClaimSummary{
		TenantCandidates: make(map[string]int),
		TenantAssigned:   make(map[string]int),
		TenantSkipped:    make(map[string]int),
		TenantInflight:   make(map[string]int),
		TenantQuota:      make(map[string]int),
		QueueCandidates:  make(map[string]int),
		QueueAssigned:    make(map[string]int),
		QueueSkipped:     make(map[string]int),
	}

	candidateLimit := batchSize * 4
	if candidateLimit < batchSize {
		candidateLimit = batchSize
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT r.id, r.job_id, j.tenant_id, j.queue, j.kind, r.scheduled_at
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'PENDING'
		  AND r.available_at <= NOW()
		ORDER BY j.priority ASC, r.scheduled_at ASC
		FOR UPDATE OF r SKIP LOCKED
		LIMIT $1
	`, candidateLimit)
	if err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("select candidate runs: %w", err)
	}

	candidates := make([]candidateRun, 0, batchSize)
	for rows.Next() {
		var item candidateRun
		if err := rows.Scan(&item.RunID, &item.JobID, &item.TenantID, &item.Queue, &item.Kind, &item.ScheduledAt); err != nil {
			rows.Close()
			return nil, ClaimSummary{}, fmt.Errorf("scan candidate run: %w", err)
		}
		candidates = append(candidates, item)
		summary.TenantCandidates[item.TenantID]++
		summary.QueueCandidates[item.Queue]++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, ClaimSummary{}, fmt.Errorf("candidate runs rows error: %w", err)
	}
	rows.Close()
	summary.CandidateRuns = len(candidates)
	candidates = fairOrderCandidates(candidates)

	workerRows, err := tx.QueryContext(ctx, `
		SELECT
			w.id,
			w.queues,
			w.capabilities,
			w.max_concurrency,
			w.last_heartbeat_at,
			COUNT(r.id) FILTER (WHERE r.status = 'RUNNING' AND r.lease_expires_at > NOW()) AS active_runs
		FROM workers w
		LEFT JOIN runs r ON r.worker_id = w.id
		WHERE w.status = 'healthy'
		  AND w.last_heartbeat_at >= NOW() - INTERVAL '30 seconds'
		GROUP BY w.id, w.queues, w.capabilities, w.max_concurrency, w.last_heartbeat_at
	`)
	if err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("select eligible workers: %w", err)
	}
	defer workerRows.Close()

	workers := make([]*eligibleWorker, 0)
	for workerRows.Next() {
		var item eligibleWorker
		var queues []byte
		var capabilitiesBytes []byte
		if err := workerRows.Scan(&item.ID, &queues, &capabilitiesBytes, &item.MaxConcurrency, &item.LastHeartbeat, &item.Assigned); err != nil {
			return nil, ClaimSummary{}, fmt.Errorf("scan eligible worker: %w", err)
		}
		item.Queues = parsePQArray(string(queues))
		item.Capabilities = make(map[string]bool)
		if len(capabilitiesBytes) > 0 {
			raw := make(map[string]any)
			if err := json.Unmarshal(capabilitiesBytes, &raw); err != nil {
				return nil, ClaimSummary{}, fmt.Errorf("unmarshal worker capabilities: %w", err)
			}
			for key, value := range raw {
				enabled, ok := value.(bool)
				if ok && enabled {
					item.Capabilities[key] = true
				}
			}
		}
		workers = append(workers, &item)
	}
	if err := workerRows.Err(); err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("eligible workers rows error: %w", err)
	}
	summary.EligibleWorkers = len(workers)
	for _, worker := range workers {
		if worker.Assigned >= worker.MaxConcurrency {
			summary.SaturatedWorkers++
		}
	}

	inflightRows, err := tx.QueryContext(ctx, `
		SELECT j.tenant_id, COUNT(r.id)
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'RUNNING'
		  AND r.lease_expires_at > NOW()
		GROUP BY j.tenant_id
	`)
	if err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("select tenant inflight: %w", err)
	}
	defer inflightRows.Close()

	tenantInflight := make(map[string]int)
	for inflightRows.Next() {
		var tenantID string
		var count int
		if err := inflightRows.Scan(&tenantID, &count); err != nil {
			return nil, ClaimSummary{}, fmt.Errorf("scan tenant inflight: %w", err)
		}
		tenantID = defaultTenantID(tenantID)
		tenantInflight[tenantID] = count
		summary.TenantInflight[tenantID] = count
	}
	if err := inflightRows.Err(); err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("tenant inflight rows error: %w", err)
	}

	quotaRows, err := tx.QueryContext(ctx, `
		SELECT tenant_id, max_inflight
		FROM tenant_quotas
	`)
	if err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("select tenant quotas: %w", err)
	}
	defer quotaRows.Close()

	tenantQuotas := make(map[string]int)
	for quotaRows.Next() {
		var tenantID string
		var maxInflight int
		if err := quotaRows.Scan(&tenantID, &maxInflight); err != nil {
			return nil, ClaimSummary{}, fmt.Errorf("scan tenant quota: %w", err)
		}
		tenantID = defaultTenantID(tenantID)
		tenantQuotas[tenantID] = maxInflight
		summary.TenantQuota[tenantID] = maxInflight
	}
	if err := quotaRows.Err(); err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("tenant quota rows error: %w", err)
	}

	var assignments []WorkerAssignment
	for _, candidate := range candidates {
		if len(assignments) >= batchSize {
			break
		}
		effectiveQuota := resolveTenantQuota(candidate.TenantID, tenantQuotas, tenantMaxInflight)
		if effectiveQuota > 0 {
			summary.TenantQuota[candidate.TenantID] = effectiveQuota
		}
		if effectiveQuota > 0 && tenantInflight[candidate.TenantID] >= effectiveQuota {
			summary.SkippedTenantLimit++
			summary.TenantSkipped[candidate.TenantID]++
			summary.QueueSkipped[candidate.Queue]++
			continue
		}
		worker, status := selectBestWorker(workers, candidate.Queue, candidate.Kind)
		if worker == nil {
			switch status {
			case workerSelectionNoCapacity:
				summary.SkippedNoCapacity++
			default:
				summary.SkippedNoEligibleWorker++
			}
			summary.TenantSkipped[candidate.TenantID]++
			summary.QueueSkipped[candidate.Queue]++
			continue
		}

		var item WorkerAssignment
		err := tx.QueryRowContext(ctx, `
			UPDATE runs
			SET status = 'RUNNING',
			    worker_id = $2,
			    lease_token = lease_token + 1,
			    lease_expires_at = NOW() + ($3 * INTERVAL '1 second'),
			    started_at = NOW(),
			    updated_at = NOW()
			WHERE id = $1
			RETURNING id, job_id, worker_id, lease_token, updated_at, lease_expires_at
		`, candidate.RunID, worker.ID, int(leaseDuration/time.Second)).Scan(
			&item.RunID,
			&item.JobID,
			&item.WorkerID,
			&item.LeaseToken,
			&item.AssignedAt,
			&item.LeaseExpiry,
		)
		if err != nil {
			return nil, ClaimSummary{}, fmt.Errorf("update claimed run: %w", err)
		}
		item.TenantID = candidate.TenantID
		item.Queue = candidate.Queue
		assignments = append(assignments, item)
		worker.Assigned++
		tenantInflight[candidate.TenantID]++
		summary.TenantInflight[candidate.TenantID] = tenantInflight[candidate.TenantID]
		summary.TenantAssigned[candidate.TenantID]++
		summary.QueueAssigned[candidate.Queue]++
	}

	for _, assignment := range assignments {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, actor_id, payload)
			VALUES ($1, 'LEASE_GRANTED', 'scheduler', $2, $3::jsonb)
		`, assignment.RunID, assignment.WorkerID, fmt.Sprintf(`{"tenant_id":%q,"queue":%q,"lease_token":%d}`, assignment.TenantID, assignment.Queue, assignment.LeaseToken)); err != nil {
			return nil, ClaimSummary{}, fmt.Errorf("insert lease event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, ClaimSummary{}, fmt.Errorf("commit claim tx: %w", err)
	}
	summary.AssignedRuns = len(assignments)
	summary.SaturatedWorkers = 0
	for _, worker := range workers {
		if worker.Assigned >= worker.MaxConcurrency {
			summary.SaturatedWorkers++
		}
	}

	return assignments, summary, nil
}

type workerSelectionStatus string

const (
	workerSelectionAssigned   workerSelectionStatus = "assigned"
	workerSelectionNoWorker   workerSelectionStatus = "no_worker"
	workerSelectionNoCapacity workerSelectionStatus = "no_capacity"
)

func selectBestWorker(workers []*eligibleWorker, queue, kind string) (*eligibleWorker, workerSelectionStatus) {
	eligible := make([]*eligibleWorker, 0)
	hasMatch := false
	for _, worker := range workers {
		if !containsString(worker.Queues, queue) {
			continue
		}
		if !worker.Capabilities[kind] {
			continue
		}
		hasMatch = true
		if worker.Assigned >= worker.MaxConcurrency {
			continue
		}
		eligible = append(eligible, worker)
	}
	if len(eligible) == 0 {
		if hasMatch {
			return nil, workerSelectionNoCapacity
		}
		return nil, workerSelectionNoWorker
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		leftUtil := float64(eligible[i].Assigned) / float64(eligible[i].MaxConcurrency)
		rightUtil := float64(eligible[j].Assigned) / float64(eligible[j].MaxConcurrency)
		if leftUtil != rightUtil {
			return leftUtil < rightUtil
		}
		leftFree := eligible[i].MaxConcurrency - eligible[i].Assigned
		rightFree := eligible[j].MaxConcurrency - eligible[j].Assigned
		if leftFree != rightFree {
			return leftFree > rightFree
		}
		if !eligible[i].LastHeartbeat.Equal(eligible[j].LastHeartbeat) {
			return eligible[i].LastHeartbeat.After(eligible[j].LastHeartbeat)
		}
		return eligible[i].ID < eligible[j].ID
	})

	return eligible[0], workerSelectionAssigned
}

func (s *Store) MaterializeDueRuns(ctx context.Context, batchSize int) (int, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.materialize_due_runs")
	defer span.End()
	span.SetAttributes(attribute.Int("runq.batch_size", batchSize))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin materialize tx: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT js.job_id, js.cron_expr, js.timezone, js.next_run_at
		FROM job_schedules js
		JOIN jobs j ON j.id = js.job_id
		WHERE j.disabled_at IS NULL
		  AND js.next_run_at <= NOW()
		ORDER BY js.next_run_at ASC
		FOR UPDATE OF js SKIP LOCKED
		LIMIT $1
	`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("select due schedules: %w", err)
	}
	defer rows.Close()

	type dueSchedule struct {
		jobID     string
		cronExpr  string
		timezone  string
		nextRunAt time.Time
	}
	due := make([]dueSchedule, 0)
	for rows.Next() {
		var item dueSchedule
		if err := rows.Scan(&item.jobID, &item.cronExpr, &item.timezone, &item.nextRunAt); err != nil {
			return 0, fmt.Errorf("scan due schedule: %w", err)
		}
		due = append(due, item)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("due schedules rows error: %w", err)
	}

	created := 0
	for _, item := range due {
		runID, err := newID("run")
		if err != nil {
			return created, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runs (
				id, job_id, status, attempt, scheduled_at, available_at, created_at, updated_at
			)
			VALUES ($1, $2, 'PENDING', 0, $3, $3, NOW(), NOW())
		`, runID, item.jobID, item.nextRunAt); err != nil {
			return created, fmt.Errorf("insert materialized run: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, payload)
			VALUES ($1, 'RUN_CREATED', 'scheduler', '{"source":"cron"}'::jsonb)
		`, runID); err != nil {
			return created, fmt.Errorf("insert materialized event: %w", err)
		}
		nextRunAt, err := computeNextRun(item.cronExpr, item.timezone, item.nextRunAt)
		if err != nil {
			return created, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE job_schedules
			SET last_run_at = $2,
			    next_run_at = $3,
			    updated_at = NOW()
			WHERE job_id = $1
		`, item.jobID, item.nextRunAt, nextRunAt); err != nil {
			return created, fmt.Errorf("update schedule: %w", err)
		}
		created++
	}

	if err := tx.Commit(); err != nil {
		return created, fmt.Errorf("commit materialize tx: %w", err)
	}

	return created, nil
}

func (s *Store) PollAssignments(ctx context.Context, workerID string, limit int) ([]PolledAssignment, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.poll_assignments")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.worker_id", workerID),
		attribute.Int("runq.limit", limit),
	)

	rows, err := s.db.QueryContext(ctx, `
		SELECT r.id, r.job_id, j.kind, j.payload, j.timeout_seconds, r.lease_token, r.lease_expires_at
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.worker_id = $1
		  AND r.status = 'RUNNING'
		  AND r.lease_expires_at > NOW()
		ORDER BY r.updated_at ASC
		LIMIT $2
	`, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("poll assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]PolledAssignment, 0)
	for rows.Next() {
		var item PolledAssignment
		var payloadBytes []byte
		if err := rows.Scan(&item.RunID, &item.JobID, &item.Kind, &payloadBytes, &item.TimeoutSeconds, &item.LeaseToken, &item.LeaseExpiry); err != nil {
			return nil, fmt.Errorf("scan polled assignment: %w", err)
		}
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &item.Payload); err != nil {
				return nil, fmt.Errorf("unmarshal payload: %w", err)
			}
		}
		assignments = append(assignments, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("polled assignments rows error: %w", err)
	}

	return assignments, nil
}

func (s *Store) HeartbeatWorker(ctx context.Context, workerID string, updates []HeartbeatUpdate, leaseDuration time.Duration) error {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.heartbeat_worker")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.worker_id", workerID),
		attribute.Int("runq.update_count", len(updates)),
		attribute.String("runq.lease_duration", leaseDuration.String()),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin heartbeat tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE workers
		SET last_heartbeat_at = $2, status = 'healthy'
		WHERE id = $1
	`, workerID, now); err != nil {
		return fmt.Errorf("update worker heartbeat: %w", err)
	}

	for _, update := range updates {
		progressJSON, err := json.Marshal(update.Progress)
		if err != nil {
			return fmt.Errorf("marshal progress: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE runs
			SET last_heartbeat_at = $4,
			    lease_expires_at = $4::timestamptz + ($5 * INTERVAL '1 second'),
			    updated_at = $4
			WHERE id = $1
			  AND worker_id = $2
			  AND lease_token = $3
			  AND status = 'RUNNING'
		`, update.RunID, workerID, update.LeaseToken, now, int(leaseDuration/time.Second))
		if err != nil {
			return fmt.Errorf("update run heartbeat: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("run heartbeat rows affected: %w", err)
		}
		if affected == 0 {
			return ErrConflict
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, actor_id, payload)
			VALUES ($1, 'LEASE_RENEWED', 'worker', $2, $3::jsonb)
		`, update.RunID, workerID, string(progressJSON)); err != nil {
			return fmt.Errorf("insert lease renewed event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit heartbeat tx: %w", err)
	}

	return nil
}

func (s *Store) CompleteRun(ctx context.Context, input CompleteRunInput) error {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.complete_run")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.worker_id", input.WorkerID),
		attribute.String("runq.run_id", input.RunID),
	)

	resultJSON, err := json.Marshal(input.Result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin complete tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'SUCCEEDED',
		    completed_at = $4,
		    result = $5::jsonb,
		    updated_at = $4
		WHERE id = $1
		  AND worker_id = $2
		  AND lease_token = $3
		  AND status = 'RUNNING'
	`, input.RunID, input.WorkerID, input.LeaseToken, now, string(resultJSON))
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("complete run rows affected: %w", err)
	}
	if affected == 0 {
		return ErrConflict
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_events (run_id, event_type, actor_type, actor_id, payload)
		VALUES ($1, 'RUN_SUCCEEDED', 'worker', $2, $3::jsonb)
	`, input.RunID, input.WorkerID, string(resultJSON)); err != nil {
		return fmt.Errorf("insert run succeeded event: %w", err)
	}
	if err := disableTerminalOneOffJob(ctx, tx, input.RunID, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit complete tx: %w", err)
	}

	return nil
}

func (s *Store) FailRun(ctx context.Context, input FailRunInput) error {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.fail_run")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.worker_id", input.WorkerID),
		attribute.String("runq.run_id", input.RunID),
		attribute.String("runq.error_code", input.ErrorCode),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fail tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	var attempt int
	var maxRetries int
	var backoffBase int
	err = tx.QueryRowContext(ctx, `
		SELECT r.attempt, j.max_retries, j.retry_backoff_base_seconds
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.id = $1
		  AND r.worker_id = $2
		  AND r.lease_token = $3
		  AND r.status = 'RUNNING'
		FOR UPDATE
	`, input.RunID, input.WorkerID, input.LeaseToken).Scan(&attempt, &maxRetries, &backoffBase)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrConflict
		}
		return fmt.Errorf("load run for fail: %w", err)
	}

	retryable := input.Retryable && attempt < maxRetries
	var result sql.Result
	if retryable {
		nextAvailableAt := now.Add(computeRetryDelay(attempt+1, backoffBase))
		result, err = tx.ExecContext(ctx, `
			UPDATE runs
			SET status = 'PENDING',
			    attempt = attempt + 1,
			    available_at = $4,
			    started_at = NULL,
			    worker_id = NULL,
			    lease_expires_at = NULL,
			    last_heartbeat_at = NULL,
			    error_code = $5,
			    error_message = $6,
			    updated_at = $3
			WHERE id = $1
			  AND worker_id = $2
			  AND lease_token = $7
			  AND status = 'RUNNING'
		`, input.RunID, input.WorkerID, now, nextAvailableAt, input.ErrorCode, input.ErrorMessage, input.LeaseToken)
		if err != nil {
			return fmt.Errorf("requeue failed run: %w", err)
		}
		payload := fmt.Sprintf(`{"error_code":%q,"retryable":true,"available_at":%q}`, input.ErrorCode, nextAvailableAt.Format(time.RFC3339))
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, actor_id, payload)
			VALUES ($1, 'RUN_REQUEUED', 'worker', $2, $3::jsonb)
		`, input.RunID, input.WorkerID, payload); err != nil {
			return fmt.Errorf("insert run requeued event: %w", err)
		}
	} else {
		result, err = tx.ExecContext(ctx, `
			UPDATE runs
			SET status = 'FAILED',
			    completed_at = $5,
			    error_code = $4,
			    error_message = $6,
			    updated_at = $5
			WHERE id = $1
			  AND worker_id = $2
			  AND lease_token = $3
			  AND status = 'RUNNING'
		`, input.RunID, input.WorkerID, input.LeaseToken, input.ErrorCode, now, input.ErrorMessage)
		if err != nil {
			return fmt.Errorf("fail run: %w", err)
		}
		payload := fmt.Sprintf(`{"error_code":%q,"retryable":false}`, input.ErrorCode)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, actor_id, payload)
			VALUES ($1, 'RUN_FAILED', 'worker', $2, $3::jsonb)
		`, input.RunID, input.WorkerID, payload); err != nil {
			return fmt.Errorf("insert run failed event: %w", err)
		}
		if err := disableTerminalOneOffJob(ctx, tx, input.RunID, now); err != nil {
			return err
		}
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("fail run rows affected: %w", err)
	}
	if affected == 0 {
		return ErrConflict
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fail tx: %w", err)
	}

	return nil
}

func (s *Store) RecoverExpiredRuns(ctx context.Context, batchSize int) ([]RecoveredRun, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.recover_expired_runs")
	defer span.End()
	span.SetAttributes(attribute.Int("runq.batch_size", batchSize))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin recover tx: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT r.id, r.job_id, r.attempt, j.max_retries, j.retry_backoff_base_seconds
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'RUNNING'
		  AND r.lease_expires_at IS NOT NULL
		  AND r.lease_expires_at <= NOW()
		ORDER BY r.lease_expires_at ASC
		FOR UPDATE OF r SKIP LOCKED
		LIMIT $1
	`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("select expired runs: %w", err)
	}
	defer rows.Close()

	type expiredRun struct {
		runID       string
		jobID       string
		attempt     int
		maxRetries  int
		backoffBase int
	}
	var expired []expiredRun
	for rows.Next() {
		var item expiredRun
		if err := rows.Scan(&item.runID, &item.jobID, &item.attempt, &item.maxRetries, &item.backoffBase); err != nil {
			return nil, fmt.Errorf("scan expired run: %w", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("expired runs rows error: %w", err)
	}

	now := time.Now().UTC()
	recovered := make([]RecoveredRun, 0, len(expired))
	for _, item := range expired {
		if item.attempt < item.maxRetries {
			nextAvailableAt := now.Add(computeRetryDelay(item.attempt+1, item.backoffBase))
			if _, err := tx.ExecContext(ctx, `
				UPDATE runs
				SET status = 'PENDING',
				    attempt = attempt + 1,
				    available_at = $2,
				    started_at = NULL,
				    worker_id = NULL,
				    lease_expires_at = NULL,
				    last_heartbeat_at = NULL,
				    updated_at = $3
				WHERE id = $1
			`, item.runID, nextAvailableAt, now); err != nil {
				return nil, fmt.Errorf("requeue expired run: %w", err)
			}
			payload := fmt.Sprintf(`{"reason":"lease_expired","available_at":%q}`, nextAvailableAt.Format(time.RFC3339))
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO run_events (run_id, event_type, actor_type, payload)
				VALUES ($1, 'RUN_REQUEUED', 'reaper', $2::jsonb)
			`, item.runID, payload); err != nil {
				return nil, fmt.Errorf("insert reaper requeue event: %w", err)
			}
			availableCopy := nextAvailableAt
			recovered = append(recovered, RecoveredRun{
				RunID:       item.runID,
				JobID:       item.jobID,
				Action:      "requeued",
				AvailableAt: &availableCopy,
			})
			continue
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE runs
			SET status = 'FAILED',
			    completed_at = $2,
			    error_code = 'LEASE_EXPIRED',
			    error_message = 'lease expired and retry budget exhausted',
			    updated_at = $2
			WHERE id = $1
		`, item.runID, now); err != nil {
			return nil, fmt.Errorf("fail expired run: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO run_events (run_id, event_type, actor_type, payload)
			VALUES ($1, 'RUN_FAILED', 'reaper', '{"reason":"lease_expired"}'::jsonb)
		`, item.runID); err != nil {
			return nil, fmt.Errorf("insert reaper failed event: %w", err)
		}
		if err := disableTerminalOneOffJob(ctx, tx, item.runID, now); err != nil {
			return nil, err
		}
		recovered = append(recovered, RecoveredRun{
			RunID:  item.runID,
			JobID:  item.jobID,
			Action: "failed",
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit recover tx: %w", err)
	}

	return recovered, nil
}

func (s *Store) RecoverTimedOutRuns(ctx context.Context, batchSize int) ([]RecoveredRun, error) {
	ctx, span := observability.Tracer("runq/store").Start(ctx, "store.recover_timed_out_runs")
	defer span.End()
	span.SetAttributes(attribute.Int("runq.batch_size", batchSize))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin timeout recover tx: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT r.id, r.job_id, r.attempt, j.max_retries, j.retry_backoff_base_seconds
		FROM runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'RUNNING'
		  AND r.started_at IS NOT NULL
		  AND r.started_at + make_interval(secs => j.timeout_seconds) <= NOW()
		ORDER BY r.started_at ASC
		FOR UPDATE OF r SKIP LOCKED
		LIMIT $1
	`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("select timed out runs: %w", err)
	}
	defer rows.Close()

	type timedOutRun struct {
		runID       string
		jobID       string
		attempt     int
		maxRetries  int
		backoffBase int
	}
	var timedOut []timedOutRun
	for rows.Next() {
		var item timedOutRun
		if err := rows.Scan(&item.runID, &item.jobID, &item.attempt, &item.maxRetries, &item.backoffBase); err != nil {
			return nil, fmt.Errorf("scan timed out run: %w", err)
		}
		timedOut = append(timedOut, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("timed out runs rows error: %w", err)
	}

	now := time.Now().UTC()
	recovered := make([]RecoveredRun, 0, len(timedOut))
	for _, item := range timedOut {
		if item.attempt < item.maxRetries {
			nextAvailableAt := now.Add(computeRetryDelay(item.attempt+1, item.backoffBase))
			if _, err := tx.ExecContext(ctx, `
				UPDATE runs
				SET status = 'PENDING',
				    attempt = attempt + 1,
				    available_at = $2,
				    started_at = NULL,
				    worker_id = NULL,
				    lease_expires_at = NULL,
				    last_heartbeat_at = NULL,
				    error_code = 'TIMED_OUT',
				    error_message = 'run timed out and was requeued',
				    updated_at = $3
				WHERE id = $1
			`, item.runID, nextAvailableAt, now); err != nil {
				return nil, fmt.Errorf("requeue timed out run: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO run_events (run_id, event_type, actor_type, payload)
				VALUES
				  ($1, 'RUN_TIMED_OUT', 'reaper', '{"reason":"timeout"}'::jsonb),
				  ($1, 'RUN_REQUEUED', 'reaper', $2::jsonb)
			`, item.runID, fmt.Sprintf(`{"reason":"timeout","available_at":%q}`, nextAvailableAt.Format(time.RFC3339))); err != nil {
				return nil, fmt.Errorf("insert timeout requeue events: %w", err)
			}
			availableCopy := nextAvailableAt
			recovered = append(recovered, RecoveredRun{
				RunID:       item.runID,
				JobID:       item.jobID,
				Action:      "timed_out_requeued",
				AvailableAt: &availableCopy,
			})
			continue
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE runs
			SET status = 'TIMED_OUT',
			    completed_at = $2,
			    error_code = 'TIMED_OUT',
			    error_message = 'run timed out and retry budget exhausted',
			    updated_at = $2
			WHERE id = $1
		`, item.runID, now); err != nil {
			return nil, fmt.Errorf("mark timed out run terminal: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
				INSERT INTO run_events (run_id, event_type, actor_type, payload)
				VALUES ($1, 'RUN_TIMED_OUT', 'reaper', '{"reason":"timeout"}'::jsonb)
			`, item.runID); err != nil {
			return nil, fmt.Errorf("insert run timed out event: %w", err)
		}
		if err := disableTerminalOneOffJob(ctx, tx, item.runID, now); err != nil {
			return nil, err
		}
		recovered = append(recovered, RecoveredRun{
			RunID:  item.runID,
			JobID:  item.jobID,
			Action: "timed_out",
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit timeout recover tx: %w", err)
	}

	return recovered, nil
}

func pqArray(values []string) string {
	if len(values) == 0 {
		return "{}"
	}

	quoted := make([]string, 0, len(values))
	for _, value := range values {
		escaped := strings.ReplaceAll(value, `"`, `\"`)
		quoted = append(quoted, `"`+escaped+`"`)
	}

	return "{" + strings.Join(quoted, ",") + "}"
}

func disableTerminalOneOffJob(ctx context.Context, tx *sql.Tx, runID string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET disabled_at = COALESCE(disabled_at, $2),
		    updated_at = $2
		WHERE id = (
			SELECT job_id
			FROM runs
			WHERE id = $1
		)
		  AND schedule_type = 'once'
	`, runID, now); err != nil {
		return fmt.Errorf("disable terminal one-off job: %w", err)
	}
	return nil
}

func isUniqueViolation(err error, constraint string) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}
	if pqErr.Code != "23505" {
		return false
	}
	if constraint == "" {
		return true
	}
	return pqErr.Constraint == constraint
}

func parsePQArray(value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "{}" {
		return nil
	}
	trimmed = strings.TrimPrefix(trimmed, "{")
	trimmed = strings.TrimSuffix(trimmed, "}")
	if trimmed == "" {
		return nil
	}

	parts := strings.Split(trimmed, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"`)
		if part != "" {
			items = append(items, part)
		}
	}
	return items
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func defaultTenantID(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "default"
	}
	return trimmed
}

type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertAuditEvent(ctx context.Context, execer execContexter, eventTime time.Time, audit *AuditEventInput) error {
	if audit == nil {
		return nil
	}

	payload := audit.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal audit payload: %w", err)
	}

	var actorID any
	if strings.TrimSpace(audit.ActorID) != "" {
		actorID = strings.TrimSpace(audit.ActorID)
	}
	var tenantID any
	if strings.TrimSpace(audit.TenantID) != "" {
		tenantID = strings.TrimSpace(audit.TenantID)
	}

	_, err = execer.ExecContext(ctx, `
		INSERT INTO audit_events (
			event_time, actor_role, actor_id, action, resource_type, resource_id, tenant_id, payload
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
	`, eventTime, strings.TrimSpace(audit.ActorRole), actorID, strings.TrimSpace(audit.Action), strings.TrimSpace(audit.ResourceType), strings.TrimSpace(audit.ResourceID), tenantID, string(payloadJSON))
	if err != nil {
		return err
	}
	return nil
}

func resolveTenantQuota(tenantID string, overrides map[string]int, fallback int) int {
	tenantID = defaultTenantID(tenantID)
	if value, ok := overrides[tenantID]; ok && value > 0 {
		return value
	}
	if fallback > 0 {
		return fallback
	}
	return 0
}

func fairOrderCandidates(candidates []candidateRun) []candidateRun {
	if len(candidates) <= 1 {
		return candidates
	}

	tenantBuckets := make(map[string][]candidateRun)
	tenantOrder := make([]string, 0)
	for _, candidate := range candidates {
		tenantID := defaultTenantID(candidate.TenantID)
		if _, exists := tenantBuckets[tenantID]; !exists {
			tenantOrder = append(tenantOrder, tenantID)
		}
		tenantBuckets[tenantID] = append(tenantBuckets[tenantID], candidate)
	}

	for tenantID, bucket := range tenantBuckets {
		tenantBuckets[tenantID] = fairOrderTenantQueue(bucket)
	}

	ordered := make([]candidateRun, 0, len(candidates))
	for {
		progressed := false
		for _, tenantID := range tenantOrder {
			bucket := tenantBuckets[tenantID]
			if len(bucket) == 0 {
				continue
			}
			ordered = append(ordered, bucket[0])
			tenantBuckets[tenantID] = bucket[1:]
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return ordered
}

func fairOrderTenantQueue(candidates []candidateRun) []candidateRun {
	queueBuckets := make(map[string][]candidateRun)
	queueOrder := make([]string, 0)
	for _, candidate := range candidates {
		if _, exists := queueBuckets[candidate.Queue]; !exists {
			queueOrder = append(queueOrder, candidate.Queue)
		}
		queueBuckets[candidate.Queue] = append(queueBuckets[candidate.Queue], candidate)
	}

	ordered := make([]candidateRun, 0, len(candidates))
	for {
		progressed := false
		for _, queue := range queueOrder {
			bucket := queueBuckets[queue]
			if len(bucket) == 0 {
				continue
			}
			ordered = append(ordered, bucket[0])
			queueBuckets[queue] = bucket[1:]
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return ordered
}

func computeRetryDelay(attempt, baseSeconds int) time.Duration {
	if baseSeconds <= 0 {
		baseSeconds = 5
	}
	if attempt <= 0 {
		attempt = 1
	}

	multiplier := 1 << (attempt - 1)
	return time.Duration(baseSeconds*multiplier) * time.Second
}

func computeNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}

	location := time.UTC
	if timezone != "" {
		loc, err := time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("load timezone: %w", err)
		}
		location = loc
	}

	next := schedule.Next(from.In(location))
	return next.UTC(), nil
}

type runScanner interface {
	Scan(dest ...any) error
}

func scanRun(scanner runScanner) (Run, error) {
	var run Run
	var startedAt sql.NullTime
	var completedAt sql.NullTime
	var workerID sql.NullString
	var leaseExpiresAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	var resultBytes []byte
	var errorCode sql.NullString
	var errorMessage sql.NullString

	if err := scanner.Scan(
		&run.ID,
		&run.JobID,
		&run.TenantID,
		&run.Status,
		&run.Attempt,
		&run.ScheduledAt,
		&run.AvailableAt,
		&startedAt,
		&completedAt,
		&workerID,
		&run.LeaseToken,
		&leaseExpiresAt,
		&lastHeartbeatAt,
		&resultBytes,
		&errorCode,
		&errorMessage,
	); err != nil {
		return Run{}, err
	}

	if startedAt.Valid {
		value := startedAt.Time
		run.StartedAt = &value
	}
	if completedAt.Valid {
		value := completedAt.Time
		run.CompletedAt = &value
	}
	if workerID.Valid {
		value := workerID.String
		run.WorkerID = &value
	}
	if leaseExpiresAt.Valid {
		value := leaseExpiresAt.Time
		run.LeaseExpiresAt = &value
	}
	if lastHeartbeatAt.Valid {
		value := lastHeartbeatAt.Time
		run.LastHeartbeatAt = &value
	}
	if len(resultBytes) > 0 {
		if err := json.Unmarshal(resultBytes, &run.Result); err != nil {
			return Run{}, fmt.Errorf("unmarshal run result: %w", err)
		}
	}
	if errorCode.Valid {
		value := errorCode.String
		run.ErrorCode = &value
	}
	if errorMessage.Valid {
		value := errorMessage.String
		run.ErrorMessage = &value
	}

	return run, nil
}

func newID(prefix string) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate %s id: %w", prefix, err)
	}

	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(buf[:])), nil
}
