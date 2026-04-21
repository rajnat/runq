# SDKs

This directory contains lightweight client SDKs for the current runq API surface.

## Go SDK

Location:
- `sdk/go/client.go`

Usage:

```go
client := runqsdk.NewClient("http://localhost:8080", "tenant-token")
resp, err := client.ListJobs(ctx, map[string]string{"tenant_id": "tenant-api"})
```

## TypeScript SDK

Location:
- `sdk/ts/client.ts`

Usage:

```ts
const client = new RunqClient('http://localhost:8080', 'tenant-token');
const jobs = await client.listJobs({ tenant_id: 'tenant-api' });
```

These SDKs are intentionally small wrappers around the current OpenAPI-documented endpoints and are suitable as a starting point for fuller generated clients later.
