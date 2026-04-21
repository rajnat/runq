# Observability guide

runq ships with Prometheus metrics and OpenTelemetry tracing hooks.

## Local stack

`deploy/docker-compose.yml` starts:
- Prometheus
- Grafana
- Jaeger
- OTEL collector

Ports:
- Grafana: `3000`
- Jaeger UI: `16686`
- Prometheus UI: `9094`
- OTEL collector: `4317` / `4318`

## Metrics endpoints

Default metrics listeners:
- API: `:9090`
- scheduler: `:9091`
- reaper: `:9092`
- worker: `:9093`

Each process exposes `/metrics` on its configured metrics address.

## Trace configuration

Set:
- `RUNQ_TRACE_OTLP_ENDPOINT`

When unset, tracing is disabled by using a never-sample provider.
When set, traces are exported with OTLP/HTTP.

## Metric families in current code

### API
- `runq_api_requests_total`
- `runq_api_request_duration_seconds`
- `runq_api_worker_register_errors_total`
- `runq_api_workers_registered_total`
- `runq_api_run_list_errors_total`
- `runq_api_run_get_errors_total`

### Scheduler
- `runq_scheduler_tick_duration_seconds`
- `runq_scheduler_materialize_errors_total`
- `runq_scheduler_runs_materialized_total`
- `runq_scheduler_tick_errors_total`
- `runq_scheduler_candidate_runs`
- `runq_scheduler_eligible_workers`
- `runq_scheduler_saturated_workers`
- `runq_scheduler_skipped_runs_last_tick`
- `runq_scheduler_skipped_runs_total`
- `runq_scheduler_tenant_groups_last_tick`
- `runq_scheduler_queue_groups_last_tick`
- `runq_scheduler_ticks_empty_total`
- `runq_scheduler_ticks_claimed_total`
- `runq_scheduler_runs_claimed_total`
- `runq_scheduler_last_claim_batch_size`

### Reaper
- `runq_reaper_tick_duration_seconds`
- `runq_reaper_timeout_errors_total`
- `runq_reaper_runs_timed_out_total`
- `runq_reaper_tick_errors_total`
- `runq_reaper_ticks_empty_total`
- `runq_reaper_ticks_recovered_total`
- `runq_reaper_runs_recovered_total`

### Worker
- `runq_worker_registrations_total`
- `runq_worker_running_runs`
- `runq_worker_heartbeat_errors_total`
- `runq_worker_poll_errors_total`
- `runq_worker_poll_duration_seconds`
- `runq_worker_assignments_polled_total`
- `runq_worker_runs_started_total`
- `runq_worker_runs_abandoned_total`

## Recommended dashboards

At minimum chart:
- API request rate / latency / error rate
- scheduler claimed runs and skip reasons
- reaper recoveries and timeouts
- worker running runs and poll/heartbeat errors
- tenant quota pressure

A starter Grafana dashboard is already provisioned under `deploy/grafana/provisioning/dashboards/json/runq-overview.json`.

## Troubleshooting traces

- no traces: confirm `RUNQ_TRACE_OTLP_ENDPOINT`
- API traces only: make sure scheduler/reaper/worker also get the same env var
- traces exported but not visible: confirm collector endpoint and Jaeger pipeline in `deploy/otel/collector.yml`
