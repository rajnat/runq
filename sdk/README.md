# SDKs

This directory contains lightweight partial client SDKs for the current runq API surface.

## Go SDK

Location:
- `sdk/go/client.go`

Currently implemented operations:
- auth me
- create job
- list jobs
- get job
- lookup job by dedupe key
- list runs
- list workers
- register worker
- upsert tenant quota

Usage:

```go
client := runqsdk.NewClient("http://localhost:8080", "tenant-token")
resp, err := client.ListJobs(ctx, map[string]string{"tenant_id": "tenant-api"})
```

## TypeScript SDK

Location:
- `sdk/ts/client.ts`

Currently implemented operations:
- list jobs
- create job
- list runs

Usage:

```ts
const client = new RunqClient('http://localhost:8080', 'tenant-token');
const jobs = await client.listJobs({ tenant_id: 'tenant-api' });
```

These SDKs are intentionally small hand-written wrappers around a subset of the current API and are suitable as a starting point for fuller generated clients later.
