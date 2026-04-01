CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id TEXT PRIMARY KEY,
    max_inflight INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
