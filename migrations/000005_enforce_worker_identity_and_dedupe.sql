ALTER TABLE jobs
DROP COLUMN IF EXISTS concurrency_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe_key
ON jobs(tenant_id, dedupe_key)
WHERE dedupe_key IS NOT NULL AND disabled_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_workers_name
ON workers(name);
