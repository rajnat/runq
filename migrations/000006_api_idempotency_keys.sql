CREATE TABLE IF NOT EXISTS api_idempotency_keys (
    tenant_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, operation, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_api_idempotency_keys_created_at
    ON api_idempotency_keys(created_at DESC);
