# Worker protocol and model

This document describes the current worker control plane exposed by runq.

## Worker identity

A worker registers with:
- `name`
- `queues`
- `capabilities`
- `max_concurrency`
- `metadata`

Registration returns:
- `worker_id`
- heartbeat interval hint
- lease renew interval hint

Worker tokens are scoped to a worker name, and the API validates that a worker token can only register/poll/heartbeat/complete/fail for that identity.

## Registration

Endpoint:
- `POST /v1/workers/register`

Request shape:

```json
{
  "name": "worker-api",
  "queues": ["default"],
  "capabilities": {"http": true},
  "max_concurrency": 4,
  "metadata": {"runtime": "runq-worker"}
}
```

Behavior:
- if the worker name already exists, runq reuses that worker identity and updates mutable registration fields
- newly created workers start in `healthy` status

## Assignment model

A worker polls with available slots.

Endpoint:
- `POST /v1/workers/{workerID}/poll`

Request shape:

```json
{
  "available_slots": 2
}
```

Response includes assignment objects with:
- `run_id`
- `job_id`
- `kind`
- `payload`
- `timeout_seconds`
- `lease_token`
- `lease_expires_at`

The scheduler only assigns work to workers that are:
- `healthy`
- subscribed to the target queue
- capable of the target kind
- below max concurrency
- compatible with tenant and concurrency-key constraints

## Heartbeats

Endpoint:
- `POST /v1/workers/{workerID}/heartbeat`

Workers periodically report active runs and progress:

```json
{
  "running": [
    {
      "run_id": "run-123",
      "lease_token": 1,
      "progress": {"percent": 50}
    }
  ]
}
```

Heartbeats refresh lease state and contribute to worker health summaries.

## Completion and failure

Complete endpoint:
- `POST /v1/workers/{workerID}/complete`

```json
{
  "run_id": "run-123",
  "lease_token": 1,
  "status": "SUCCEEDED",
  "result": {"status_code": 200}
}
```

Fail endpoint:
- `POST /v1/workers/{workerID}/fail`

```json
{
  "run_id": "run-123",
  "lease_token": 1,
  "error_code": "HTTP_500",
  "error_message": "upstream returned 500",
  "retryable": true
}
```

## Worker admin lifecycle

Admin endpoints:
- drain
- reactivate
- decommission

Semantics:
- `drained`: worker remains registered but stops receiving new assignments
- `reactivate`: only valid from drained state; returns worker to healthy scheduling eligibility
- `decommissioned`: worker stays visible for control-plane purposes but should not receive new assignments

## Worker detail endpoint

`GET /v1/workers/{workerID}` returns current worker state plus:
- `inflight_assignment_count`
- `inflight_runs`
- `health`

Current health fields:
- `heartbeat_age_seconds`
- `heartbeat_drift_seconds`
- `heartbeat_stale`
- `inflight_assignments`
- `available_capacity`
- `at_capacity`

## Failure and recovery model

- workers hold leased assignments rather than owning work permanently
- if heartbeats stop or leases expire, the reaper can recover work
- timeouts and lease loss can requeue, retry, or dead-letter work depending on run/job state

## Reference implementation

The sample worker in `cmd/worker` and `internal/service/worker.go` is the canonical reference for the current protocol.
