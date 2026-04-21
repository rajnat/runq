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
- auth can still be configured in insecure ways if operators leave tokens empty or overly broad
- application-layer tenant isolation is weaker than DB-enforced isolation
- metrics endpoints are convenient locally but should not be internet-exposed by default
- mutating endpoints do not yet use idempotency keys
- rate limiting is not yet enforced

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

## Auditability

runq already maintains audit events for many mutating actions. In production, retain and ship these events along with logs and traces to your central observability platform.
