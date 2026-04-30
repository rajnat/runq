# API usage guide

This guide focuses on the current operator-facing and tenant-facing HTTP API.

Base URL:
- `http://localhost:8080`

Auth header:
- `Authorization: Bearer <token>`

## Auth model

Configured tokens are static strings with one of these scopes:
- admin
- tenant:<tenant_id>
- worker:<worker_name>

Examples:
- admin token can manage all tenants and workers
- tenant token can only see and mutate that tenant's jobs/runs
- worker token can only operate as the configured worker identity

## Tenancy and quotas

Tenancy is explicit in the API today.

Rules:
- tenant-scoped tokens can only access their own tenant resources
- admin tokens can operate across tenants
- many list endpoints accept `tenant_id`, but the API enforces scope before querying or returning data

Quota APIs:
- `GET /v1/tenants/quotas`
- `PUT /v1/tenants/{tenantID}/quota`

Current quota dimensions:
- `max_inflight`
- `max_pending_runs`
- `max_active_jobs`

Quota effects show up primarily during job admission and scheduler assignment.

## Jobs

### Create a once job

`POST /v1/jobs`

```json
{
  "name": "demo-job",
  "tenant_id": "tenant-api",
  "queue": "default",
  "kind": "http",
  "payload": {"url": "https://example.internal/task"}
}
```

### Create a delayed job

```json
{
  "name": "delayed-job",
  "tenant_id": "tenant-api",
  "queue": "default",
  "kind": "http",
  "payload": {"url": "https://example.internal/task"},
  "schedule": {
    "type": "delayed",
    "run_at": "2026-04-22T12:00:00Z"
  }
}
```

### Create a cron job

```json
{
  "name": "cron-job",
  "tenant_id": "tenant-api",
  "queue": "default",
  "kind": "http",
  "payload": {"url": "https://example.internal/task"},
  "schedule": {
    "type": "cron",
    "cron": "*/5 * * * *",
    "timezone": "UTC"
  }
}
```

### List jobs

`GET /v1/jobs`

Supported filters today:
- `tenant_id`
- `queue`
- `kind`
- `disabled`
- `paused`
- `name`
- `dedupe_key`
- `concurrency_key`
- `created_after`
- `created_before`
- `updated_after`
- `updated_before`
- `limit`
- `offset`
- `cursor`

Examples:
- `/v1/jobs?tenant_id=tenant-api&name=demo-job`
- `/v1/jobs?tenant_id=tenant-api&dedupe_key=my-key`
- `/v1/jobs?tenant_id=tenant-api&created_after=2026-04-21T00:00:00Z`

### Get job by ID

`GET /v1/jobs/{jobID}`

### Lookup job by natural key

`GET /v1/jobs/lookup?tenant_id=<tenant>&dedupe_key=<key>`

### Update job

`PATCH /v1/jobs/{jobID}`

Supports mutable core fields and delayed/cron schedule mutation.

### Job lifecycle endpoints

Single-job endpoints:
- `POST /v1/jobs/{jobID}/disable`
- `POST /v1/jobs/{jobID}/enable`
- `POST /v1/jobs/{jobID}/pause`
- `POST /v1/jobs/{jobID}/resume`
- `POST /v1/jobs/{jobID}/trigger`
- `POST /v1/jobs/{jobID}/cancel`

Bulk endpoints:
- `POST /v1/jobs/disable`
- `POST /v1/jobs/enable`
- `POST /v1/jobs/pause`
- `POST /v1/jobs/resume`

Bulk job lifecycle requests support:
- explicit `job_ids`
- filter selection (`tenant_id`, `queue`, `kind`, `paused`, `disabled`)
- `dry_run`

Bulk lifecycle semantics:
- successful items are returned alongside skipped items
- skipped items carry explicit `error_code` and `error_message`
- dry-run returns `would_change` / `would_skip` without mutation

Current single-job lifecycle semantics:
- `pause` prevents pending runs from being claimed until the job is resumed
- `disable` prevents future pending runs from being claimed
- `disable` does not cancel already running runs
- `disable` does not retroactively cancel existing pending runs; they remain persisted but unclaimable until the job is enabled again
- `cancel` is the lifecycle action that both disables the job and cancels its pending runs

## Runs

### List runs

`GET /v1/runs`

Supported filters today:
- `tenant_id`
- `status` (comma-separated)
- `queue`
- `worker_id`
- `job_id`
- `dead_lettered`
- `error_code`
- `attempt`
- `scheduled_after`
- `scheduled_before`
- `completed_after`
- `completed_before`
- `limit`
- `offset`
- `cursor`

### Get run by ID

`GET /v1/runs/{runID}`

### Run lifecycle endpoints

Single-run endpoints:
- `POST /v1/runs/{runID}/cancel`
- `POST /v1/runs/{runID}/requeue`
- `POST /v1/runs/{runID}/redrive`

Bulk endpoints:
- `POST /v1/runs/cancel`
- `POST /v1/runs/requeue`
- `POST /v1/runs/redrive`

Bulk run requests support `dry_run`.

Bulk run semantics:
- partial success is expected and returned item-by-item
- dry-run returns `would_accept` / `would_cancel` / `would_skip`
- real execution returns accepted/canceled items and explicit skip reasons

## Workers

### List workers

`GET /v1/workers`

Supported filters:
- `status`
- `queue`
- `capability`
- `limit`
- `offset`
- `cursor`

### Get worker

`GET /v1/workers/{workerID}`

Worker detail currently includes:
- queues, capabilities, metadata, status, timestamps
- inflight assignment count
- inflight run summaries
- health summary with heartbeat drift/staleness and capacity

### Worker lifecycle

Admin endpoints:
- `POST /v1/workers/{workerID}/drain`
- `POST /v1/workers/{workerID}/reactivate`
- `POST /v1/workers/{workerID}/decommission`

Worker protocol endpoints:
- `POST /v1/workers/register`
- `POST /v1/workers/{workerID}/poll`
- `POST /v1/workers/{workerID}/heartbeat`
- `POST /v1/workers/{workerID}/complete`
- `POST /v1/workers/{workerID}/fail`

## Quotas

- `GET /v1/tenants/quotas`
- `PUT /v1/tenants/{tenantID}/quota`

## Audit

### List audit events

`GET /v1/audit/events`

Supported filters:
- `tenant_id`
- `action`
- `resource_type`
- `resource_id`
- `actor_id`
- `limit`
- `offset`
- `cursor`

## Pagination

List endpoints support offset and cursor pagination.

Rules:
- use either `offset` or `cursor` for a request, never both
- if a `cursor` is present, omit `offset`
- for stable deep pagination, prefer cursor mode

Response pagination shape:
- `limit`
- `offset`
- `returned`
- `has_more`
- `next_offset`
- `next_cursor`

## Stable error contract

Errors return a stable envelope:

```json
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "..."
  }
}
```

Common codes in the current API include:
- `INVALID_ARGUMENT`
- `INVALID_JSON`
- `FORBIDDEN`
- `NOT_FOUND`
- `CONFLICT`
- `INTERNAL`
- domain-specific codes like `WORKER_REACTIVATE_CONFLICT`, `JOB_DISABLE_CONFLICT`, `RUN_REQUEUE_CONFLICT`, `TENANT_QUOTA_EXCEEDED`

## Example requests and responses

See `docs/api-examples.md` for copy/paste examples across jobs, runs, workers, quotas, and audit endpoints.
