ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_queue_priority ON jobs(tenant_id, queue, priority);
