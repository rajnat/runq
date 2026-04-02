ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ;

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ;

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS dead_letter_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_paused_at ON jobs(paused_at) WHERE paused_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runs_dead_lettered_at ON runs(dead_lettered_at) WHERE dead_lettered_at IS NOT NULL;
