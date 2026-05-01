# Security and hardening notes

This document describes the current security posture and near-term hardening priorities.

## Current model

### API auth
- static bearer tokens from `RUNQ_API_TOKENS`
- token scopes are admin, tenant-scoped, or worker-name-scoped
- tenant access is enforced in the API layer

### Worker auth
- worker tokens are tied to a configured worker name
- worker protocol calls validate that the token matches the worker identity being used

### Data isolation
- tenant isolation is enforced in application logic
- the database schema is shared across tenants today

## Current limitations

These are known gaps in the present implementation:
- static tokens are operationally simple but weak for long-term production use
- auth can still be configured in insecure ways if operators choose insecure dev mode deliberately, though it is now restricted to loopback binds
- application-layer tenant isolation is weaker than DB-enforced isolation
- metrics endpoints are convenient locally but should not be internet-exposed by default
- idempotency is currently implemented for job creation, not all mutating endpoints
- rate limiting is enforced, but it is still intentionally simple and local-process only

## Current safe-use guidance

- always set explicit `RUNQ_API_TOKENS`
- use distinct admin, tenant, and worker credentials
- do not expose metrics ports publicly
- run behind TLS termination and a trusted ingress/proxy
- restrict Postgres network access to runtime components
- keep Jaeger/Grafana/Prometheus behind internal-only access in non-dev environments

## Recommended production controls

- move from static tokens to a proper authn/authz system
- add fail-closed startup validation for auth config
- introduce worker session credentials beyond stable worker names
- add API idempotency for mutating requests
- add per-tenant and per-token rate limiting
- evaluate DB-level tenant isolation / RLS
- add panic recovery middleware and abuse controls for large heartbeat payloads

## Tenant isolation strategy

The preferred long-term strategy is defense in depth:
1. keep API-layer authorization checks for clear user-facing errors
2. add tenant ownership columns to all tenant-scoped operational tables
3. execute tenant-scoped API requests with a request-scoped Postgres setting such as `SET LOCAL runq.tenant_id = ...`
4. enable row-level security policies for tenant-facing tables using that setting
5. keep admin, scheduler, reaper, and migration paths on privileged DB access outside tenant RLS policies

Recommended rollout shape:
- add explicit `tenant_id` propagation to `runs`, `run_events`, and `job_schedules`
- add request-scoped DB execution helpers in the store/API layer
- enable RLS policies only after tenant-scoped execution is in place
- keep application-layer checks even after RLS lands

This avoids treating RLS as a replacement for application authorization while still making cross-tenant data leaks much harder.

## Event retention and partitioning plan

`run_events` and `audit_events` should be treated as append-heavy operational history tables.

Recommended plan:
- partition both tables by monthly `event_time`
- keep parent-table indexes aligned with the dominant access paths
- precreate future partitions during regular maintenance
- expire old partitions instead of issuing large delete sweeps

Suggested retention windows:
- `run_events`: keep 30-90 days hot in Postgres, then archive externally if longer history is needed
- `audit_events`: keep at least 365 days hot in Postgres, with longer retention in centralized archives if required by policy

Operational guidance:
- prefer partition drop over bulk delete for old event data
- run retention work during off-peak periods or from a dedicated maintenance job
- back up Postgres before partition conversion work
- verify `TRUNCATE`/test reset flows against partitioned tables in CI after partitioning is introduced

## Auditability

runq already maintains audit events for many mutating actions. In production, retain and ship these events along with logs and traces to your central observability platform.
