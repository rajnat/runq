# Architecture overview

runq is split into four runtime roles backed by one Postgres database.

## Components

1. API server
- exposes operator and tenant-facing HTTP endpoints
- validates/authenticates requests
- persists job, run, worker, quota, and audit state through the store layer
- exposes metrics and tracing

2. Scheduler
- periodically materializes due delayed/cron runs
- claims pending runs for eligible workers
- enforces queue matching, worker capability matching, worker capacity, tenant inflight limits, and concurrency-key constraints

3. Reaper
- recovers expired leases
- marks timed-out runs and either retries, dead-letters, or makes work available again depending on retry policy

4. Worker
- registers with queues/capabilities/max concurrency
- polls for assignments
- sends heartbeat progress for active leases
- completes or fails runs through the public API

## Data model

Core entities:
- jobs
  - tenant-owned work definitions
  - support `once`, `delayed`, and `cron` schedules
  - contain queue, kind, payload, retry/timeouts, dedupe, and concurrency settings
- job_schedules
  - schedule-specific state like cron expression, timezone, and next run time
- runs
  - concrete executions of jobs
  - move through `PENDING`, `RUNNING`, terminal states, and dead-letter flows
- workers
  - registered worker identities with status, queues, capabilities, and heartbeat metadata
- tenant_quotas
  - limit inflight, pending, and active-job counts per tenant
- audit_events
  - operator and lifecycle audit trail
- run_events
  - execution-history and progress trail for runs

## Control flow

### Job creation
1. client calls `POST /v1/jobs`
2. API validates request and auth scope
3. store inserts job and optionally an initial run / schedule row
4. audit event is written for mutating operations

### Scheduled execution
1. scheduler scans schedules due by now
2. scheduler materializes concrete pending runs
3. scheduler selects healthy eligible workers
4. scheduler assigns leases to workers and marks runs `RUNNING`

### Worker execution
1. worker registers identity and capabilities
2. worker polls for assignments
3. worker executes assignment and sends heartbeats
4. worker completes or fails the run
5. API/store writes run and audit/run-event state

### Failure recovery
1. reaper scans for expired leases / timed-out runs
2. run is retried, dead-lettered, or returned to pending depending on policy
3. operators can inspect and use cancel/requeue/redrive APIs

## Multi-tenant boundaries

- tenant tokens are scoped to a single tenant
- admin tokens can operate across tenants
- worker tokens are tied to worker identity
- list/get/mutation endpoints enforce tenant access checks in the API layer

## Observability

- Prometheus metrics from every runtime
- OTEL tracing hooks for API, store, scheduler, reaper, and worker flows
- Grafana and Jaeger local stack under `deploy/`

## Known architectural limits

Current implementation is intentionally pragmatic:
- API auth still uses static tokens from config
- tenancy is enforced in application logic rather than DB-level isolation
- job lookup natural keys currently focus on tenant + dedupe key
- docs/OpenAPI are maintained in-repo rather than generated from annotations
