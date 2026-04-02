ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS concurrency_key TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_concurrency_key
ON jobs(tenant_id, concurrency_key)
WHERE concurrency_key IS NOT NULL;
