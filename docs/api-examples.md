# API examples

This file provides copy/paste examples for major endpoints.

## Jobs

### Create job

Request:

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Authorization: Bearer ***' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: job-create-001' \
  -d '{
    "name": "example-job",
    "tenant_id": "tenant-api",
    "queue": "default",
    "kind": "http",
    "dedupe_key": "example-job-key",
    "payload": {"url": "https://example.internal/task"}
  }'
```

Example response:

```json
{
  "job_id": "job-123",
  "run_id": "run-123",
  "status": "accepted"
}
```

### List jobs with filters

```bash
curl -s 'http://localhost:8080/v1/jobs?tenant_id=tenant-api&name=example-job&dedupe_key=example-job-key' \
  -H 'Authorization: Bearer ***'
```

Example response:

```json
{
  "jobs": [
    {
      "id": "job-123",
      "name": "example-job",
      "tenant_id": "tenant-api",
      "queue": "default",
      "kind": "http",
      "schedule_type": "once",
      "priority": 100,
      "max_retries": 3,
      "timeout_seconds": 300,
      "retry_backoff_base_seconds": 5,
      "created_at": "2026-04-21T20:00:00Z",
      "updated_at": "2026-04-21T20:00:00Z"
    }
  ],
  "pagination": {
    "limit": 0,
    "offset": 0,
    "returned": 1,
    "has_more": false
  }
}
```

### Lookup by dedupe key

```bash
curl -s 'http://localhost:8080/v1/jobs/lookup?tenant_id=tenant-api&dedupe_key=example-job-key' \
  -H 'Authorization: Bearer ***'
```

## Runs

### List failed runs by error code

```bash
curl -s 'http://localhost:8080/v1/runs?tenant_id=tenant-api&status=FAILED&error_code=TIMEOUT' \
  -H 'Authorization: Bearer ***'
```

Example response:

```json
{
  "runs": [
    {
      "id": "run-123",
      "job_id": "job-123",
      "job_name": "example-job",
      "tenant_id": "tenant-api",
      "queue": "default",
      "kind": "http",
      "schedule_type": "once",
      "job_disabled": false,
      "status": "FAILED",
      "attempt": 2,
      "scheduled_at": "2026-04-21T20:00:00Z",
      "available_at": "2026-04-21T20:00:00Z",
      "completed_at": "2026-04-21T20:05:00Z",
      "lease_token": 1,
      "error_code": "TIMEOUT",
      "error_message": "worker timed out"
    }
  ],
  "pagination": {
    "limit": 0,
    "offset": 0,
    "returned": 1,
    "has_more": false
  }
}
```

### Get run detail with bounded embedded events

```bash
curl -s 'http://localhost:8080/v1/runs/run-123' \
  -H 'Authorization: Bearer ***'
```

### Page through run events

```bash
curl -s 'http://localhost:8080/v1/runs/run-123/events?limit=50&offset=0' \
  -H 'Authorization: Bearer ***'
```

### Bulk requeue dry-run

```bash
curl -s -X POST http://localhost:8080/v1/runs/requeue \
  -H 'Authorization: Bearer ***' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "tenant-api",
    "status": ["FAILED"],
    "dry_run": true
  }'
```

## Workers

### List workers by queue/capability

```bash
curl -s 'http://localhost:8080/v1/workers?queue=default&capability=http' \
  -H 'Authorization: Bearer ***'
```

### Get worker detail

```bash
curl -s 'http://localhost:8080/v1/workers/worker-123' \
  -H 'Authorization: Bearer ***'
```

Example response:

```json
{
  "worker": {
    "id": "worker-123",
    "name": "worker-api",
    "queues": ["default"],
    "capabilities": {"http": true},
    "status": "healthy",
    "max_concurrency": 4,
    "last_heartbeat_at": "2026-04-21T20:00:10Z",
    "started_at": "2026-04-21T19:55:00Z",
    "metadata": {"runtime": "runq-worker"},
    "inflight_assignment_count": 1,
    "inflight_runs": [
      {
        "run_id": "run-123",
        "job_id": "job-123",
        "tenant_id": "tenant-api",
        "queue": "default",
        "status": "RUNNING",
        "attempt": 1,
        "lease_token": 1,
        "lease_expires_at": "2026-04-21T20:00:40Z"
      }
    ],
    "health": {
      "heartbeat_age_seconds": 2,
      "heartbeat_drift_seconds": 0,
      "heartbeat_stale": false,
      "inflight_assignments": 1,
      "available_capacity": 3,
      "at_capacity": false
    }
  }
}
```

### Worker protocol poll

```bash
curl -s -X POST http://localhost:8080/v1/workers/worker-123/poll \
  -H 'Authorization: Bearer ***' \
  -H 'X-Runq-Worker-Session: ws-123' \
  -H 'Content-Type: application/json' \
  -d '{"available_slots": 1}'
```

### Reactivate a drained worker

```bash
curl -s -X POST http://localhost:8080/v1/workers/worker-123/reactivate \
  -H 'Authorization: Bearer ***'
```

## Quotas

### Upsert tenant quota

```bash
curl -s -X PUT http://localhost:8080/v1/tenants/tenant-api/quota \
  -H 'Authorization: Bearer ***' \
  -H 'Content-Type: application/json' \
  -d '{
    "max_inflight": 10,
    "max_pending_runs": 100,
    "max_active_jobs": 50
  }'
```

## Audit

### List audit events by actor or resource

```bash
curl -s 'http://localhost:8080/v1/audit/events?actor_id=admin-user&resource_type=job' \
  -H 'Authorization: Bearer ***'
```

## Stable error envelope

Example error:

```json
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "dedupe_key is required"
  }
}
```
