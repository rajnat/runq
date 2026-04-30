CREATE INDEX IF NOT EXISTS idx_jobs_created_at_id ON jobs(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_runs_created_at_id ON runs(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id_id ON run_events(run_id, id);
CREATE INDEX IF NOT EXISTS idx_workers_started_at_id ON workers(started_at DESC, id DESC);
