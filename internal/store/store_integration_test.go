package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const defaultTestDBURL = "postgres://runq:runq@localhost:5432/runq?sslmode=disable"

func TestMaterializeDueRuns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "cron-test",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/1 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}

	_, err = store.db.ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID)
	if err != nil {
		t.Fatalf("set due schedule: %v", err)
	}

	created, err := store.MaterializeDueRuns(ctx, 10)
	if err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected 1 materialized run, got %d", created)
	}

	runs, err := store.ListRuns(ctx, RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].Status != "PENDING" {
		t.Fatalf("expected PENDING run, got %s", runs[0].Status)
	}
}

func TestFailRunRetryableRequeues(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, workerID := createRunningRun(t, store, ctx, 30, 3)

	err := store.FailRun(ctx, FailRunInput{
		WorkerID:     workerID,
		RunID:        runID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_503",
		ErrorMessage: "upstream unavailable",
		Retryable:    true,
	})
	if err != nil {
		t.Fatalf("fail run: %v", err)
	}

	run, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "PENDING" {
		t.Fatalf("expected PENDING after retryable fail, got %s", run.Status)
	}
	if run.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", run.Attempt)
	}
	if run.ErrorCode == nil || *run.ErrorCode != "HTTP_503" {
		t.Fatalf("expected HTTP_503 error code, got %+v", run.ErrorCode)
	}
	if run.StartedAt != nil {
		t.Fatalf("expected started_at to reset on requeue, got %+v", run.StartedAt)
	}
}

func TestCancelJobDisablesAndCancelsPendingRuns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "cancel-test",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	cancelResult, err := store.CancelJob(ctx, result.JobID, "test cancel", nil)
	if err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	if cancelResult.CanceledRuns != 1 {
		t.Fatalf("expected 1 canceled run, got %d", cancelResult.CanceledRuns)
	}

	jobs, err := store.ListJobs(ctx, JobFilter{Disabled: boolPtr(true)})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].DisabledAt == nil {
		t.Fatalf("expected disabled job entry, got %+v", jobs)
	}

	runs, err := store.ListRuns(ctx, RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "CANCELED" {
		t.Fatalf("expected canceled run, got %+v", runs)
	}
}

func TestRecoverTimedOutRunsRequeues(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, _ := createRunningRun(t, store, ctx, 1, 3)
	_, err := store.db.ExecContext(ctx, `
		UPDATE runs
		SET started_at = NOW() - INTERVAL '5 seconds',
		    lease_expires_at = NOW() + INTERVAL '1 hour'
		WHERE id = $1
	`, runID)
	if err != nil {
		t.Fatalf("backdate started_at: %v", err)
	}

	recovered, err := store.RecoverTimedOutRuns(ctx, 10)
	if err != nil {
		t.Fatalf("recover timed out runs: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered timed out run, got %d", len(recovered))
	}

	run, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "PENDING" {
		t.Fatalf("expected PENDING after timeout requeue, got %s", run.Status)
	}
	if run.Attempt != 1 {
		t.Fatalf("expected attempt 1 after timeout, got %d", run.Attempt)
	}
	if run.ErrorCode == nil || *run.ErrorCode != "TIMED_OUT" {
		t.Fatalf("expected TIMED_OUT error code, got %+v", run.ErrorCode)
	}
	if run.StartedAt != nil {
		t.Fatalf("expected started_at to reset after timeout requeue, got %+v", run.StartedAt)
	}
}

func TestClaimPendingRunsResetsStartedAtPerAttempt(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, workerID := createRunningRun(t, store, ctx, 30, 3)

	beforeRetry, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run before retry: %v", err)
	}
	if beforeRetry.StartedAt == nil {
		t.Fatalf("expected started_at before retry")
	}

	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     workerID,
		RunID:        runID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_503",
		ErrorMessage: "upstream unavailable",
		Retryable:    true,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	registered, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "retry-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "retry"},
	})
	if err != nil {
		t.Fatalf("register retry worker: %v", err)
	}

	_, err = store.DB().ExecContext(ctx, `UPDATE runs SET available_at = NOW() WHERE id = $1`, runID)
	if err != nil {
		t.Fatalf("make retried run available: %v", err)
	}

	assignments, _, err := store.ClaimPendingRuns(ctx, 1, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment after retry, got %d", len(assignments))
	}
	if assignments[0].WorkerID != registered.WorkerID {
		t.Fatalf("expected assignment to retry worker, got %+v", assignments[0])
	}

	afterRetry, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run after retry: %v", err)
	}
	if afterRetry.StartedAt == nil {
		t.Fatalf("expected started_at after reclaim")
	}
	if !afterRetry.StartedAt.After(*beforeRetry.StartedAt) {
		t.Fatalf("expected started_at to move forward across attempts, before=%s after=%s", beforeRetry.StartedAt, afterRetry.StartedAt)
	}
}

func TestUpsertAndListTenantQuotas(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	tenantID := "tenant-store-" + time.Now().UTC().Format("150405.000000000")
	quota, err := store.UpsertTenantQuota(ctx, tenantID, 3, 5, 7, nil)
	if err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}
	if quota.TenantID != tenantID || quota.MaxInflight != 3 || quota.MaxPendingRuns != 5 || quota.MaxActiveJobs != 7 {
		t.Fatalf("unexpected upsert result: %+v", quota)
	}

	quotas, err := store.ListTenantQuotas(ctx)
	if err != nil {
		t.Fatalf("list tenant quotas: %v", err)
	}
	found := false
	for _, item := range quotas {
		if item.TenantID == tenantID && item.MaxInflight == 3 && item.MaxPendingRuns == 5 && item.MaxActiveJobs == 7 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("unexpected tenant quotas: %+v", quotas)
	}
}

func TestCreateJobRespectsTenantActiveJobQuota(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	if _, err := store.UpsertTenantQuota(ctx, "tenant-admission", 0, 0, 1, nil); err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "first-job",
		TenantID:     "tenant-admission",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	}); err != nil {
		t.Fatalf("create first job: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "second-job",
		TenantID:     "tenant-admission",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	}); err != ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded for active job admission, got %v", err)
	}
}

func TestCreateJobRespectsTenantPendingRunQuota(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	if _, err := store.UpsertTenantQuota(ctx, "tenant-backlog", 0, 1, 0, nil); err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "first-once",
		TenantID:     "tenant-backlog",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
	}); err != nil {
		t.Fatalf("create first once job: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "second-once",
		TenantID:     "tenant-backlog",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
	}); err != ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded for pending-run admission, got %v", err)
	}
}

func TestTriggerJobRespectsTenantPendingRunQuota(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	if _, err := store.UpsertTenantQuota(ctx, "tenant-trigger-quota", 0, 1, 0, nil); err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "cron-job",
		TenantID:     "tenant-trigger-quota",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}

	if _, err := store.TriggerJob(ctx, result.JobID, nil); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	if _, err := store.TriggerJob(ctx, result.JobID, nil); err != ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded on second trigger, got %v", err)
	}
}

func TestCreateJobRejectsDuplicateDedupeKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	input := CreateJobInput{
		Name:         "dedupe-job",
		TenantID:     "tenant-dedupe",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
		DedupeKey:    "same-key",
	}

	if _, err := store.CreateJob(ctx, input); err != nil {
		t.Fatalf("create first deduped job: %v", err)
	}
	if _, err := store.CreateJob(ctx, input); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists for duplicate dedupe key, got %v", err)
	}
}

func TestCreateJobPersistsConcurrencyKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:           "concurrency-job",
		TenantID:       "tenant-concurrency",
		Queue:          "test",
		Kind:           "http",
		Payload:        map[string]any{"url": "https://example.internal"},
		ScheduleType:   "once",
		ConcurrencyKey: "customer-42",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	job, err := store.GetJob(ctx, result.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.ConcurrencyKey == nil || *job.ConcurrencyKey != "customer-42" {
		t.Fatalf("expected concurrency key to persist, got %+v", job)
	}
}

func TestCreateJobAllowsReusingDedupeKeyAfterCompletion(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "dedupe-complete",
		TenantID:     "tenant-dedupe",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
		DedupeKey:    "complete-key",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "dedupe-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"test": true},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, *result.RunID, worker.WorkerID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	if err := store.CompleteRun(ctx, CompleteRunInput{
		WorkerID:   worker.WorkerID,
		RunID:      *result.RunID,
		LeaseToken: 1,
		Result:     map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "dedupe-complete-reuse",
		TenantID:     "tenant-dedupe",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
		DedupeKey:    "complete-key",
	}); err != nil {
		t.Fatalf("expected dedupe key to be reusable after completion, got %v", err)
	}
}

func TestCreateJobAllowsReusingDedupeKeyAfterTerminalFailure(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, workerID := createRunningRun(t, store, ctx, 30, 0)

	if _, err := store.db.ExecContext(ctx, `
		UPDATE jobs
		SET tenant_id = 'tenant-dedupe',
		    dedupe_key = 'failed-key'
		WHERE id = (SELECT job_id FROM runs WHERE id = $1)
	`, runID); err != nil {
		t.Fatalf("set dedupe key: %v", err)
	}

	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     workerID,
		RunID:        runID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	if _, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "dedupe-failed-reuse",
		TenantID:     "tenant-dedupe",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
		DedupeKey:    "failed-key",
	}); err != nil {
		t.Fatalf("expected dedupe key to be reusable after failure, got %v", err)
	}
}

func TestPauseAndResumeJobControlsClaiming(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "pause-job",
		TenantID:     "tenant-pause",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "pause-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	if _, err := store.PauseJob(ctx, result.JobID, nil); err != nil {
		t.Fatalf("pause job: %v", err)
	}

	assignments, _, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim while paused: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments while paused, got %+v", assignments)
	}

	if _, err := store.ResumeJob(ctx, result.JobID, nil); err != nil {
		t.Fatalf("resume job: %v", err)
	}
	assignments, _, err = store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim after resume: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected one assignment after resume, got %+v", assignments)
	}
}

func TestTriggerJobCreatesAdHocRunForCronJob(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "trigger-job",
		TenantID:     "tenant-trigger",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, err := store.TriggerJob(ctx, result.JobID, nil)
	if err != nil {
		t.Fatalf("trigger job: %v", err)
	}
	run, events, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get triggered run: %v", err)
	}
	if run.Status != "PENDING" || run.ScheduleType != "cron" {
		t.Fatalf("expected ad hoc pending cron run, got %+v", run)
	}
	if !containsEvent(events, "RUN_CREATED") {
		t.Fatalf("expected run created event, got %+v", events)
	}
}

func TestTerminalFailureMarksRunDeadLettered(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, workerID := createRunningRun(t, store, ctx, 30, 0)
	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     workerID,
		RunID:        runID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	run, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.DeadLetteredAt == nil || run.DeadLetterReason == nil || *run.DeadLetterReason != "worker_failed" {
		t.Fatalf("expected dead-letter metadata, got %+v", run)
	}

	deadLettered := true
	runs, err := store.ListRuns(ctx, RunFilter{TenantID: run.TenantID, DeadLettered: &deadLettered})
	if err != nil {
		t.Fatalf("list dead-lettered runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("expected dead-lettered run in filtered list, got %+v", runs)
	}
}

func TestRequeueRunCreatesNewPendingRunForFailedRun(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	runID, workerID := createRunningRun(t, store, ctx, 30, 0)
	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     workerID,
		RunID:        runID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	newRunID, err := store.RequeueRun(ctx, runID, nil)
	if err != nil {
		t.Fatalf("requeue run: %v", err)
	}
	if newRunID == "" || newRunID == runID {
		t.Fatalf("expected a fresh run id, got %q", newRunID)
	}

	original, _, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get original run: %v", err)
	}
	if original.Status != "FAILED" {
		t.Fatalf("expected original run to remain failed, got %+v", original)
	}

	requeued, events, err := store.GetRun(ctx, newRunID)
	if err != nil {
		t.Fatalf("get requeued run: %v", err)
	}
	if requeued.Status != "PENDING" || requeued.Attempt != 0 {
		t.Fatalf("expected new pending attempt 0 run, got %+v", requeued)
	}
	if requeued.JobID != original.JobID {
		t.Fatalf("expected requeued run to keep same job id, got %+v", requeued)
	}
	if len(events) == 0 || events[0].EventType != "RUN_CREATED" {
		t.Fatalf("expected RUN_CREATED event on new run, got %+v", events)
	}
	if events[0].Payload["source_run_id"] != runID {
		t.Fatalf("expected source_run_id on new run event, got %+v", events[0].Payload)
	}
}

func TestRequeueRunCreatesAdHocRunForTimedOutCronJob(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "cron-requeue",
		TenantID:     "tenant-cron",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}
	_, err = store.db.ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID)
	if err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := store.MaterializeDueRuns(ctx, 1); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}

	runs, err := store.ListRuns(ctx, RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 materialized run, got %+v", runs)
	}

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "cron-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "cron"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, runs[0].ID, worker.WorkerID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() + INTERVAL '10 minutes' WHERE job_id = $1`, result.JobID); err != nil {
		t.Fatalf("set future schedule: %v", err)
	}

	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        runs[0].ID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail cron run: %v", err)
	}

	var nextRunAtBefore time.Time
	if err := store.db.QueryRowContext(ctx, `SELECT next_run_at FROM job_schedules WHERE job_id = $1`, result.JobID).Scan(&nextRunAtBefore); err != nil {
		t.Fatalf("load schedule before requeue: %v", err)
	}

	newRunID, err := store.RequeueRun(ctx, runs[0].ID, nil)
	if err != nil {
		t.Fatalf("requeue cron run: %v", err)
	}

	var nextRunAtAfter time.Time
	if err := store.db.QueryRowContext(ctx, `SELECT next_run_at FROM job_schedules WHERE job_id = $1`, result.JobID).Scan(&nextRunAtAfter); err != nil {
		t.Fatalf("load schedule after requeue: %v", err)
	}
	if !nextRunAtAfter.Equal(nextRunAtBefore) {
		t.Fatalf("expected requeue to keep cron schedule unchanged, before=%s after=%s", nextRunAtBefore, nextRunAtAfter)
	}

	requeued, _, err := store.GetRun(ctx, newRunID)
	if err != nil {
		t.Fatalf("get requeued cron run: %v", err)
	}
	if requeued.ScheduleType != "cron" || requeued.Status != "PENDING" {
		t.Fatalf("expected ad hoc pending cron run, got %+v", requeued)
	}
}

func TestRequeueRunRejectsSucceededRun(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "succeeded-requeue",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}
	_, err = store.db.ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID)
	if err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := store.MaterializeDueRuns(ctx, 1); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	runs, err := store.ListRuns(ctx, RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "success-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "success"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, runs[0].ID, worker.WorkerID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}
	if err := store.CompleteRun(ctx, CompleteRunInput{
		WorkerID:   worker.WorkerID,
		RunID:      runs[0].ID,
		LeaseToken: 1,
		Result:     map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	if _, err := store.RequeueRun(ctx, runs[0].ID, nil); err != ErrConflict {
		t.Fatalf("expected ErrConflict requeueing succeeded run, got %v", err)
	}
}

func TestRequeueRunRejectsDisabledJob(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "disabled-requeue",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}
	_, err = store.db.ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID)
	if err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := store.MaterializeDueRuns(ctx, 1); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	runs, err := store.ListRuns(ctx, RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "disabled-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "disabled"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, runs[0].ID, worker.WorkerID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}
	if err := store.FailRun(ctx, FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        runs[0].ID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE jobs SET disabled_at = NOW(), updated_at = NOW() WHERE id = $1`, result.JobID); err != nil {
		t.Fatalf("disable job: %v", err)
	}

	if _, err := store.RequeueRun(ctx, runs[0].ID, nil); err != ErrConflict {
		t.Fatalf("expected ErrConflict requeueing disabled job run, got %v", err)
	}
}

func TestRegisterWorkerReusesExistingName(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	first, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "worker-stable",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"version": 1},
	})
	if err != nil {
		t.Fatalf("register first worker: %v", err)
	}

	second, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "worker-stable",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"version": 2},
	})
	if err != nil {
		t.Fatalf("register second worker: %v", err)
	}
	if first.WorkerID != second.WorkerID {
		t.Fatalf("expected stable worker id, got %s then %s", first.WorkerID, second.WorkerID)
	}

	workers, err := store.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected a single worker row, got %+v", workers)
	}
	if workers[0].MaxConcurrency != 2 {
		t.Fatalf("expected worker update to persist, got %+v", workers[0])
	}
}

func TestClaimPendingRunsRequiresMatchingCapability(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "shell-job",
		Queue:        "test",
		Kind:         "shell",
		Payload:      map[string]any{"command": "echo hello"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "http-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "http"},
	})
	if err != nil {
		t.Fatalf("register http worker: %v", err)
	}

	capableWorker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "shell-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"shell": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "shell"},
	})
	if err != nil {
		t.Fatalf("register shell worker: %v", err)
	}

	assignments, _, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	if assignments[0].WorkerID != capableWorker.WorkerID {
		t.Fatalf("expected shell worker %s, got %s", capableWorker.WorkerID, assignments[0].WorkerID)
	}

	run, _, err := store.GetRun(ctx, *result.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.WorkerID == nil || *run.WorkerID != capableWorker.WorkerID {
		t.Fatalf("expected run assigned to capable worker, got %+v", run)
	}
}

func TestClaimPendingRunsLeavesRunPendingWithoutCapabilityMatch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "shell-job-no-match",
		Queue:        "test",
		Kind:         "shell",
		Payload:      map[string]any{"command": "echo hello"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "http-only-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "http"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	assignments, _, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected 0 assignments, got %d", len(assignments))
	}

	run, _, err := store.GetRun(ctx, *result.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "PENDING" || run.WorkerID != nil {
		t.Fatalf("expected pending unassigned run, got %+v", run)
	}
}

func TestClaimPendingRunsRespectsWorkerMaxConcurrency(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	for i := 0; i < 3; i++ {
		_, err := store.CreateJob(ctx, CreateJobInput{
			Name:         "http-job",
			Queue:        "test",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/task"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create job %d: %v", i, err)
		}
	}

	_, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "small-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "small"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected only 1 assignment due to capacity, got %d", len(assignments))
	}
	if summary.SkippedNoCapacity != 2 {
		t.Fatalf("expected 2 runs skipped for capacity, got %+v", summary)
	}

	runs, err := store.ListRuns(ctx, RunFilter{Queue: "test"})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	runningCount := 0
	pendingCount := 0
	for _, run := range runs {
		switch run.Status {
		case "RUNNING":
			runningCount++
		case "PENDING":
			pendingCount++
		}
	}
	if runningCount != 1 || pendingCount != 2 {
		t.Fatalf("expected 1 RUNNING and 2 PENDING runs, got running=%d pending=%d", runningCount, pendingCount)
	}
}

func TestClaimPendingRunsSkipsConcurrentRunsWithSameConcurrencyKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	for i := 0; i < 2; i++ {
		_, err := store.CreateJob(ctx, CreateJobInput{
			Name:           "serialized-job",
			TenantID:       "tenant-serial",
			Queue:          "test",
			Kind:           "http",
			Payload:        map[string]any{"url": "https://example.internal/task"},
			ScheduleType:   "once",
			ConcurrencyKey: "account-7",
		})
		if err != nil {
			t.Fatalf("create job %d: %v", i, err)
		}
	}

	_, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "serial-worker-a",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"pool": "a"},
	})
	if err != nil {
		t.Fatalf("register first worker: %v", err)
	}
	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "serial-worker-b",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"pool": "b"},
	})
	if err != nil {
		t.Fatalf("register second worker: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected only one assignment for shared concurrency key, got %d", len(assignments))
	}
	if summary.SkippedConcurrencyLimit != 1 {
		t.Fatalf("expected one concurrency skip, got %+v", summary)
	}

	runs, err := store.ListRuns(ctx, RunFilter{TenantID: "tenant-serial"})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	runningCount := 0
	pendingCount := 0
	for _, run := range runs {
		switch run.Status {
		case "RUNNING":
			runningCount++
		case "PENDING":
			pendingCount++
		}
	}
	if runningCount != 1 || pendingCount != 1 {
		t.Fatalf("expected one running and one pending run, got running=%d pending=%d", runningCount, pendingCount)
	}
}

func TestClaimPendingRunsSkipsWhenConcurrencyKeyAlreadyRunning(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	activeRunID, _ := createRunningRun(t, store, ctx, 30, 0)
	if _, err := store.db.ExecContext(ctx, `
		UPDATE jobs
		SET tenant_id = 'tenant-shared',
		    concurrency_key = 'account-9'
		WHERE id = (SELECT job_id FROM runs WHERE id = $1)
	`, activeRunID); err != nil {
		t.Fatalf("set active concurrency key: %v", err)
	}

	_, err := store.CreateJob(ctx, CreateJobInput{
		Name:           "queued-serialized-job",
		TenantID:       "tenant-shared",
		Queue:          "test",
		Kind:           "http",
		Payload:        map[string]any{"url": "https://example.internal/task"},
		ScheduleType:   "once",
		ConcurrencyKey: "account-9",
	})
	if err != nil {
		t.Fatalf("create queued job: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected no new assignments while matching concurrency key is running, got %+v", assignments)
	}
	if summary.SkippedConcurrencyLimit != 1 {
		t.Fatalf("expected one concurrency skip, got %+v", summary)
	}
}

func TestClaimPendingRunsPrefersLessLoadedCapableWorker(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	first, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "busy-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 4,
		Metadata:       map[string]any{"role": "busy"},
	})
	if err != nil {
		t.Fatalf("register first worker: %v", err)
	}

	second, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "free-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 4,
		Metadata:       map[string]any{"role": "free"},
	})
	if err != nil {
		t.Fatalf("register second worker: %v", err)
	}

	busyRunID, _ := createRunningRunForWorker(t, store, ctx, first.WorkerID, 30, 3)
	_ = busyRunID

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "new-http-job",
		Queue:        "test",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	assignments, _, err := store.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	if assignments[0].WorkerID != second.WorkerID {
		t.Fatalf("expected less-loaded worker %s, got %s", second.WorkerID, assignments[0].WorkerID)
	}
}

func TestClaimPendingRunsFairlySpreadsAcrossQueues(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	for i := 0; i < 3; i++ {
		_, err := store.CreateJob(ctx, CreateJobInput{
			Name:         "alpha-job",
			Queue:        "alpha",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/alpha"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create alpha job %d: %v", i, err)
		}
	}
	_, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "beta-job",
		Queue:        "beta",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/beta"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create beta job: %v", err)
	}

	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "alpha-worker",
		Queues:         []string{"alpha"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"queue": "alpha"},
	})
	if err != nil {
		t.Fatalf("register alpha worker: %v", err)
	}
	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "beta-worker",
		Queues:         []string{"beta"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"queue": "beta"},
	})
	if err != nil {
		t.Fatalf("register beta worker: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 2, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(assignments))
	}
	if summary.QueueAssigned["alpha"] != 1 || summary.QueueAssigned["beta"] != 1 {
		t.Fatalf("expected one assignment per queue, got %+v", summary.QueueAssigned)
	}
}

func TestClaimPendingRunsFairlySpreadsAcrossTenants(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	for i := 0; i < 3; i++ {
		_, err := store.CreateJob(ctx, CreateJobInput{
			Name:         "tenant-a-job",
			TenantID:     "tenant-a",
			Queue:        "shared",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/a"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create tenant-a job %d: %v", i, err)
		}
	}
	_, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "tenant-b-job",
		TenantID:     "tenant-b",
		Queue:        "shared",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/b"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create tenant-b job: %v", err)
	}

	_, err = store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "shared-worker",
		Queues:         []string{"shared"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 2,
		Metadata:       map[string]any{"pool": "shared"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 2, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(assignments))
	}
	if summary.TenantAssigned["tenant-a"] != 1 || summary.TenantAssigned["tenant-b"] != 1 {
		t.Fatalf("expected one assignment per tenant, got %+v", summary.TenantAssigned)
	}
}

func TestClaimPendingRunsRespectsTenantMaxInflight(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	resetTables(t, store)

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "shared-worker",
		Queues:         []string{"shared"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 4,
		Metadata:       map[string]any{"pool": "shared"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	tenantRun, _ := createRunningRunForWorker(t, store, ctx, worker.WorkerID, 30, 3)
	_, err = store.db.ExecContext(ctx, `
		UPDATE jobs
		SET tenant_id = 'tenant-a', queue = 'shared'
		WHERE id = (SELECT job_id FROM runs WHERE id = $1)
	`, tenantRun)
	if err != nil {
		t.Fatalf("set running tenant: %v", err)
	}

	_, err = store.CreateJob(ctx, CreateJobInput{
		Name:         "tenant-a-pending",
		TenantID:     "tenant-a",
		Queue:        "shared",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/a"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create tenant-a pending job: %v", err)
	}
	_, err = store.CreateJob(ctx, CreateJobInput{
		Name:         "tenant-b-pending",
		TenantID:     "tenant-b",
		Queue:        "shared",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/b"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create tenant-b pending job: %v", err)
	}

	assignments, summary, err := store.ClaimPendingRuns(ctx, 2, 30*time.Second, 1)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment because tenant-a is at limit, got %d", len(assignments))
	}
	if assignments[0].TenantID != "tenant-b" {
		t.Fatalf("expected assignment for tenant-b, got %+v", assignments[0])
	}
	if summary.SkippedTenantLimit != 1 {
		t.Fatalf("expected one tenant-limit skip, got %+v", summary)
	}
	if summary.TenantInflight["tenant-a"] != 1 || summary.TenantInflight["tenant-b"] != 1 {
		t.Fatalf("expected inflight counts to reflect quota enforcement, got %+v", summary.TenantInflight)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()

	dbURL := os.Getenv("RUNQ_DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultTestDBURL
	}

	store, err := Open(dbURL)
	if err != nil {
		t.Skipf("skipping integration test, db unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Ping(ctx); err != nil {
		t.Skipf("skipping integration test, db ping failed: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func resetTables(t *testing.T, store *Store) {
	t.Helper()

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatalf("begin reset tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(989898)`); err != nil {
		t.Fatalf("acquire reset lock: %v", err)
	}

	_, err = tx.Exec(`
		TRUNCATE TABLE audit_events, run_events, runs, job_schedules, workers, jobs, tenant_quotas RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit reset tx: %v", err)
	}
}

func createRunningRun(t *testing.T, store *Store, ctx context.Context, timeoutSeconds int, maxRetries int) (string, string) {
	t.Helper()

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:                    "running-test",
		Queue:                   "test",
		Kind:                    "http",
		Payload:                 map[string]any{"url": "https://example.internal"},
		ScheduleType:            "once",
		TimeoutSeconds:          timeoutSeconds,
		MaxRetries:              maxRetries,
		RetryBackoffBaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	worker, err := store.RegisterWorker(ctx, RegisterWorkerInput{
		Name:           "test-worker",
		Queues:         []string{"test"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"test": true},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	_, err = store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, *result.RunID, worker.WorkerID)
	if err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	return *result.RunID, worker.WorkerID
}

func createRunningRunForWorker(t *testing.T, store *Store, ctx context.Context, workerID string, timeoutSeconds int, maxRetries int) (string, string) {
	t.Helper()

	result, err := store.CreateJob(ctx, CreateJobInput{
		Name:                    "running-test",
		Queue:                   "test",
		Kind:                    "http",
		Payload:                 map[string]any{"url": "https://example.internal"},
		ScheduleType:            "once",
		TimeoutSeconds:          timeoutSeconds,
		MaxRetries:              maxRetries,
		RetryBackoffBaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	_, err = store.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, *result.RunID, workerID)
	if err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	return *result.RunID, workerID
}

func boolPtr(v bool) *bool {
	return &v
}

func containsEvent(events []RunEvent, eventType string) bool {
	for _, event := range events {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

func TestMain(m *testing.M) {
	dbURL := os.Getenv("RUNQ_DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultTestDBURL
	}

	db, err := sql.Open("postgres", dbURL)
	if err == nil {
		if _, lockErr := db.Exec(`SELECT pg_advisory_lock(999001)`); lockErr == nil {
			code := m.Run()
			_, _ = db.Exec(`SELECT pg_advisory_unlock(999001)`)
			_ = db.Close()
			os.Exit(code)
		}
		_ = db.Close()
	}

	os.Exit(m.Run())
}
