-- Initial schema for runq. This migration captures the core durable entities
-- needed for job definitions, execution runs, workers, and audit events.

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    schedule_type TEXT NOT NULL DEFAULT 'once',
    priority INTEGER NOT NULL DEFAULT 100,
    max_retries INTEGER NOT NULL DEFAULT 3,
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    retry_backoff_base_seconds INTEGER NOT NULL DEFAULT 5,
    dedupe_key TEXT,
    concurrency_key TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disabled_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_schedules (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id),
    cron_expr TEXT NOT NULL,
    timezone TEXT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ,
    misfire_policy TEXT NOT NULL DEFAULT 'catch_up',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    queues TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_concurrency INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id TEXT PRIMARY KEY,
    max_inflight INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_role TEXT NOT NULL,
    actor_id TEXT,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    tenant_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    worker_id TEXT,
    lease_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    result JSONB,
    error_code TEXT,
    error_message TEXT,
    cancel_requested_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue_priority ON jobs(queue, priority);
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_queue_priority ON jobs(tenant_id, queue, priority);
CREATE INDEX IF NOT EXISTS idx_job_schedules_next_run_at ON job_schedules(next_run_at);
CREATE INDEX IF NOT EXISTS idx_runs_status_available_at ON runs(status, available_at);
CREATE INDEX IF NOT EXISTS idx_runs_job_id_scheduled_at ON runs(job_id, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_runs_worker_status ON runs(worker_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_lease_expires_at ON runs(lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_workers_status_last_heartbeat_at ON workers(status, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_event_time ON audit_events(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_time ON audit_events(tenant_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_action_time ON audit_events(action, event_time DESC);
