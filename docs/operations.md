# Operations guide

This guide covers local operation and a simple production-shaped deployment layout.

## Runtime roles

You usually run four processes against one Postgres database:
- API server
- scheduler
- reaper
- one or more workers

## Local development

Bring up supporting infra:

```bash
make up
```

Apply migrations:

```bash
make migrate
```

Start processes in separate terminals:

```bash
RUNQ_API_TOKENS='admin-...-api' make run-api
make run-scheduler
make run-reaper
RUNQ_WORKER_AUTH_TOKEN=*** RUNQ_WORKER_NAME=worker-api make run-worker
```

## Environment variables

See `.env.example` for a sample config set.

Important variables:
- `RUNQ_DATABASE_URL`
- `RUNQ_DB_MAX_IDLE_CONNS`
- `RUNQ_DB_MAX_OPEN_CONNS`
- `RUNQ_DB_CONN_MAX_LIFETIME_SECONDS`
- `RUNQ_API_ADDR`
- `RUNQ_API_TOKENS`
- `RUNQ_API_METRICS_ADDR`
- `RUNQ_API_READ_TIMEOUT_SECONDS`
- `RUNQ_API_WRITE_TIMEOUT_SECONDS`
- `RUNQ_API_IDLE_TIMEOUT_SECONDS`
- `RUNQ_API_READ_HEADER_TIMEOUT_SECONDS`
- `RUNQ_METRICS_READ_TIMEOUT_SECONDS`
- `RUNQ_METRICS_WRITE_TIMEOUT_SECONDS`
- `RUNQ_METRICS_IDLE_TIMEOUT_SECONDS`
- `RUNQ_METRICS_READ_HEADER_TIMEOUT_SECONDS`
- `RUNQ_API_BASE_URL`
- `RUNQ_WORKER_AUTH_TOKEN`
- `RUNQ_WORKER_NAME`
- `RUNQ_WORKER_QUEUES`
- `RUNQ_WORKER_CAPABILITIES`
- `RUNQ_WORKER_MAX_CONCURRENCY`
- `RUNQ_TICK_INTERVAL_SECONDS`
- `RUNQ_LEASE_DURATION_SECONDS`
- `RUNQ_CLAIM_BATCH_SIZE`
- `RUNQ_TENANT_MAX_INFLIGHT`
- `RUNQ_TRACE_OTLP_ENDPOINT`

## Deployment topology

### Small deployment
- 1 API server
- 1 scheduler
- 1 reaper
- N workers
- 1 Postgres instance

### Larger deployment
- multiple API servers behind a load balancer
- multiple workers across queues/capabilities
- scheduler and reaper can be horizontally scaled cautiously against the same DB because lease/claim decisions are persisted centrally
- use managed Postgres and external Prometheus/Grafana/trace backends

## Upgrade flow

1. deploy code compatible with current schema
2. run migrations
3. roll API/scheduler/reaper/workers
4. verify health, metrics, and queue movement

## Backup and rollback

- back up Postgres before schema changes
- keep migration history from `schema_migrations`
- rollback is primarily data restore + binary rollback; no down-migration framework exists today

## Event retention and partitioning plan

For production-scale histories, treat `run_events` and `audit_events` as partitioned operational logs.

Recommended plan:
- partition both tables monthly on `event_time`
- precreate upcoming partitions during routine maintenance
- expire old partitions by dropping whole partitions instead of bulk deleting rows
- keep indexes aligned with the dominant read paths (`run_id,event_time` for `run_events`; tenant/action/resource filters plus `event_time` for `audit_events`)

Suggested retention windows:
- `run_events`: 30-90 days hot in Postgres
- `audit_events`: at least 365 days hot in Postgres

If longer history is required, archive partitions to external storage or a reporting warehouse before dropping them.

Operational safeguards:
- schedule partition creation/drop during off-peak windows
- validate partition-aware backup/restore flows before rollout
- verify CI/test reset helpers still work with partitioned parent tables
- prefer additive partition migrations plus data copy/swap over risky in-place rewrites during busy hours

## Incident checklist

### Jobs not moving
- check scheduler logs and metrics
- confirm workers are `healthy`
- inspect worker filters and capabilities
- inspect tenant quotas and queue assignments
- inspect `/v1/workers` and `/v1/runs`
- if the job was paused, pending runs will not be claimed until resume
- if the job was disabled, pending runs remain stored but are intentionally unclaimable until enable
- if the intent is to stop and clear pending work, use job cancel rather than disable

### Runs stuck in RUNNING
- check worker heartbeat freshness in worker detail
- check reaper logs and metrics
- inspect lease expiry and run events

### High failure rate
- filter runs by `error_code`
- inspect run detail and run events
- review worker logs for repeated execution failures

### API problems
- check `/healthz` and `/readyz`
- confirm DB connectivity
- verify configured auth tokens and tenant scope
