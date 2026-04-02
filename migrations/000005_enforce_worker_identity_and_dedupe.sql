ALTER TABLE jobs
DROP COLUMN IF EXISTS concurrency_key;

WITH ranked_duplicate_jobs AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_id, dedupe_key
            ORDER BY created_at DESC, id DESC
        ) AS row_num
    FROM jobs
    WHERE dedupe_key IS NOT NULL
      AND disabled_at IS NULL
)
UPDATE jobs
SET disabled_at = NOW(),
    updated_at = NOW()
WHERE id IN (
    SELECT id
    FROM ranked_duplicate_jobs
    WHERE row_num > 1
);

WITH duplicate_job_ids AS (
    SELECT id
    FROM jobs
    WHERE dedupe_key IS NOT NULL
      AND disabled_at IS NOT NULL
),
affected_runs AS (
    SELECT r.id
    FROM runs r
    JOIN duplicate_job_ids d ON d.id = r.job_id
    WHERE r.status IN ('PENDING', 'RUNNING')
)
UPDATE runs
SET status = 'CANCELED',
    completed_at = NOW(),
    updated_at = NOW(),
    cancel_requested_at = NOW()
WHERE id IN (SELECT id FROM affected_runs);

WITH ranked_workers AS (
    SELECT
        id,
        name,
        FIRST_VALUE(id) OVER (
            PARTITION BY name
            ORDER BY last_heartbeat_at DESC, started_at DESC, id DESC
        ) AS keep_id,
        ROW_NUMBER() OVER (
            PARTITION BY name
            ORDER BY last_heartbeat_at DESC, started_at DESC, id DESC
        ) AS row_num
    FROM workers
),
worker_rewrites AS (
    SELECT id, keep_id
    FROM ranked_workers
    WHERE row_num > 1
)
UPDATE runs
SET worker_id = worker_rewrites.keep_id
FROM worker_rewrites
WHERE runs.worker_id = worker_rewrites.id;

WITH ranked_workers AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY name
            ORDER BY last_heartbeat_at DESC, started_at DESC, id DESC
        ) AS row_num
    FROM workers
)
DELETE FROM workers
WHERE id IN (
    SELECT id
    FROM ranked_workers
    WHERE row_num > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe_key
ON jobs(tenant_id, dedupe_key)
WHERE dedupe_key IS NOT NULL AND disabled_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_workers_name
ON workers(name);
