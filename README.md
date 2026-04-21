# runq

runq is a Postgres-backed job queue and worker control plane for scheduled and on-demand work.

It provides:
- a JSON HTTP API for jobs, runs, workers, quotas, and audit events
- delayed, once, and cron scheduling
- tenant-aware quotas and concurrency controls
- worker registration, polling, heartbeats, drain/reactivate, and decommission flows
- operational APIs for cancel, requeue, redrive, bulk lifecycle operations, and audit trails
- Prometheus metrics and OpenTelemetry tracing hooks

## Repository layout

- `cmd/api-server` — HTTP API process
- `cmd/scheduler` — materializes due runs and assigns pending work
- `cmd/reaper` — recovers expired/timed-out work
- `cmd/worker` — sample worker process using the public worker protocol
- `internal/api` — API routing, auth, request parsing, and response types
- `internal/store` — Postgres persistence and scheduling/assignment logic
- `internal/service` — scheduler, reaper, and worker runtime loops
- `internal/observability` — Prometheus and tracing setup
- `deploy/` — local observability stack and dashboards
- `migrations/` — schema migrations

## Quickstart

Prerequisites:
- Go 1.26+
- Docker / Docker Compose
- Postgres 17 if running outside Compose

Start infrastructure:

```bash
make up
```

Run migrations:

```bash
make migrate
```

Start the API server:

```bash
RUNQ_API_TOKENS='admin-token:admin,tenant-token:tenant:tenant-api,worker-token:worker:worker-api' \
make run-api
```

Start the scheduler and reaper:

```bash
make run-scheduler
make run-reaper
```

Start a sample worker:

```bash
RUNQ_WORKER_AUTH_TOKEN=worker-token \
RUNQ_WORKER_NAME=worker-api \
RUNQ_API_BASE_URL=http://localhost:8080 \
make run-worker
```

Run tests:

```bash
go test -p 1 ./...
```

## Minimal API flow

Create a job:

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Authorization: Bearer tenant-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo-job",
    "tenant_id": "tenant-api",
    "queue": "default",
    "kind": "http",
    "payload": {"url": "https://example.internal/task"}
  }'
```

List jobs:

```bash
curl -s 'http://localhost:8080/v1/jobs?tenant_id=tenant-api' \
  -H 'Authorization: Bearer tenant-token'
```

Inspect a job by dedupe key:

```bash
curl -s 'http://localhost:8080/v1/jobs/lookup?tenant_id=tenant-api&dedupe_key=my-key' \
  -H 'Authorization: Bearer tenant-token'
```

## Local observability

`deploy/docker-compose.yml` starts:
- Postgres on `localhost:5432`
- Grafana on `localhost:3000`
- Jaeger on `localhost:16686`
- Prometheus on `localhost:9094`
- OTEL collector on `localhost:4317` / `4318`

## Next docs

See:
- `docs/architecture.md`
- `docs/api-usage.md`
- `docs/worker-protocol.md`
- `docs/operations.md`
- `docs/observability.md`
- `docs/security.md`
