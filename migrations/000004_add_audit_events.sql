CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_role TEXT NOT NULL,
    actor_id TEXT,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    tenant_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_events_event_time ON audit_events(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_time ON audit_events(tenant_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_action_time ON audit_events(action, event_time DESC);
