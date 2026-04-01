package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/store"
	_ "github.com/lib/pq"
)

const defaultTestDBURL = "postgres://runq:runq@localhost:5432/runq?sslmode=disable"

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

func TestWorkerProcessCompletesClaimedRun(t *testing.T) {
	jobStore := openTestStore(t)
	release := lockAndResetTables(t, jobStore)
	defer release()
	queue := "worker-complete-" + time.Now().UTC().Format("150405.000000000")

	result, err := jobStore.CreateJob(context.Background(), store.CreateJobInput{
		Name:                    "worker-complete",
		Priority:                1,
		Queue:                   queue,
		Kind:                    "http",
		Payload:                 map[string]any{"url": "https://example.internal/task"},
		ScheduleType:            "once",
		TimeoutSeconds:          30,
		MaxRetries:              2,
		RetryBackoffBaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	httpServer := httptest.NewServer(newWorkerTestMux(jobStore))
	defer httpServer.Close()

	worker := NewWorkerProcess(log.New(io.Discard, "", 0), config.WorkerConfig{
		APIBaseURL:        httpServer.URL,
		Name:              "worker-complete-test",
		Queues:            []string{queue},
		Capabilities:      []string{"http"},
		MaxConcurrency:    1,
		PollInterval:      100 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		ExecutionTime:     time.Second,
	}, observability.NewRegistry())

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		return worker.getWorkerID() != ""
	})

	scheduler := NewScheduler(log.New(io.Discard, "", 0), jobStore, config.ComponentConfig{
		ClaimBatchSize: 1,
		LeaseDuration:  30 * time.Second,
	}, observability.NewRegistry())
	if err := scheduler.tick(ctx); err != nil {
		t.Fatalf("scheduler tick: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		run, _, err := jobStore.GetRun(context.Background(), *result.RunID)
		return err == nil && run.Status == "RUNNING"
	})

	waitForCondition(t, 5*time.Second, func() bool {
		run, _, err := jobStore.GetRun(context.Background(), *result.RunID)
		return err == nil && run.Status == "SUCCEEDED"
	})

	run, events, err := jobStore.GetRun(context.Background(), *result.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED run, got %+v", run)
	}
	if !containsEvent(events, "RUN_SUCCEEDED") {
		t.Fatalf("expected RUN_SUCCEEDED event, got %+v", events)
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("worker exit: %v", err)
	}
}

func TestWorkerProcessAbandonsCanceledRun(t *testing.T) {
	jobStore := openTestStore(t)
	release := lockAndResetTables(t, jobStore)
	defer release()
	queue := "worker-cancel-" + time.Now().UTC().Format("150405.000000000")

	result, err := jobStore.CreateJob(context.Background(), store.CreateJobInput{
		Name:                    "worker-cancel",
		Priority:                1,
		Queue:                   queue,
		Kind:                    "http",
		Payload:                 map[string]any{"url": "https://example.internal/task"},
		ScheduleType:            "once",
		TimeoutSeconds:          30,
		MaxRetries:              2,
		RetryBackoffBaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	httpServer := httptest.NewServer(newWorkerTestMux(jobStore))
	defer httpServer.Close()

	worker := NewWorkerProcess(log.New(io.Discard, "", 0), config.WorkerConfig{
		APIBaseURL:        httpServer.URL,
		Name:              "worker-cancel-test",
		Queues:            []string{queue},
		Capabilities:      []string{"http"},
		MaxConcurrency:    1,
		PollInterval:      100 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		ExecutionTime:     3 * time.Second,
	}, observability.NewRegistry())

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		return worker.getWorkerID() != ""
	})

	scheduler := NewScheduler(log.New(io.Discard, "", 0), jobStore, config.ComponentConfig{
		ClaimBatchSize: 1,
		LeaseDuration:  30 * time.Second,
	}, observability.NewRegistry())
	if err := scheduler.tick(ctx); err != nil {
		t.Fatalf("scheduler tick: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return worker.runningCount() == 1
	})

	_, err = jobStore.CancelJob(context.Background(), result.JobID, "integration test cancel", nil)
	if err != nil {
		t.Fatalf("cancel job: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return worker.runningCount() == 0
	})

	run, events, err := jobStore.GetRun(context.Background(), *result.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "CANCELED" {
		t.Fatalf("expected CANCELED run, got %+v", run)
	}
	if containsEvent(events, "RUN_SUCCEEDED") {
		t.Fatalf("did not expect RUN_SUCCEEDED after cancellation, got %+v", events)
	}
	if !containsEvent(events, "RUN_CANCELED") {
		t.Fatalf("expected RUN_CANCELED event, got %+v", events)
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("worker exit: %v", err)
	}
}

func TestWorkerProcessRegistersConfiguredCapabilities(t *testing.T) {
	var captured registerWorkerRequest
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/workers/register" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, registerWorkerResponse{WorkerID: "worker-test"})
	}))
	defer httpServer.Close()

	worker := NewWorkerProcess(log.New(io.Discard, "", 0), config.WorkerConfig{
		APIBaseURL:        httpServer.URL,
		Name:              "worker-capabilities-test",
		Queues:            []string{"default"},
		Capabilities:      []string{"shell", "http"},
		MaxConcurrency:    1,
		PollInterval:      time.Hour,
		HeartbeatInterval: time.Hour,
		ExecutionTime:     time.Second,
	}, observability.NewRegistry())

	workerID, err := worker.register(context.Background())
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	if workerID != "worker-test" {
		t.Fatalf("expected worker id to round-trip, got %s", workerID)
	}
	shellEnabled, shellOK := captured.Capabilities["shell"].(bool)
	httpEnabled, httpOK := captured.Capabilities["http"].(bool)
	if !shellOK || !httpOK || !shellEnabled || !httpEnabled {
		t.Fatalf("expected configured capabilities to be registered, got %+v", captured.Capabilities)
	}
	if _, exists := captured.Capabilities["queue"]; exists {
		t.Fatalf("did not expect unrelated capability, got %+v", captured.Capabilities)
	}
}

func TestSchedulerEmitsPlacementMetrics(t *testing.T) {
	jobStore := openTestStore(t)
	release := lockAndResetTables(t, jobStore)
	defer release()
	ctx := context.Background()

	_, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "alpha-job",
		TenantID:     "tenant-alpha",
		Queue:        "alpha",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/alpha"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create alpha job: %v", err)
	}
	_, err = jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "beta-job",
		TenantID:     "tenant-beta",
		Queue:        "beta",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/beta"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create beta job: %v", err)
	}

	alphaWorker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "alpha-worker",
		Queues:         []string{"alpha"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"queue": "alpha"},
	})
	if err != nil {
		t.Fatalf("register alpha worker: %v", err)
	}
	_, err = jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "beta-worker",
		Queues:         []string{"beta"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"queue": "beta"},
	})
	if err != nil {
		t.Fatalf("register beta worker: %v", err)
	}
	_, err = createRunningRunForWorker(t, jobStore, ctx, alphaWorker.WorkerID, 30, 3, "alpha")
	if err != nil {
		t.Fatalf("seed running alpha run: %v", err)
	}

	metrics := observability.NewRegistry()
	scheduler := NewScheduler(log.New(io.Discard, "", 0), jobStore, config.ComponentConfig{
		ClaimBatchSize: 2,
		LeaseDuration:  30 * time.Second,
	}, metrics)
	if err := scheduler.tick(ctx); err != nil {
		t.Fatalf("scheduler tick: %v", err)
	}

	rendered := metrics.Render()
	assertMetricContains(t, rendered, "runq_scheduler_candidate_runs 2")
	assertMetricContains(t, rendered, "runq_scheduler_saturated_workers 2")
	assertMetricContains(t, rendered, "runq_scheduler_skipped_runs_total{reason=\"no_capacity\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_tenant_runs{state=\"skipped\",tenant=\"tenant-alpha\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_tenant_runs{state=\"assigned\",tenant=\"tenant-beta\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_queue_runs{queue=\"alpha\",state=\"skipped\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_queue_runs{queue=\"beta\",state=\"assigned\"} 1")
}

func TestSchedulerEmitsTenantLimitMetrics(t *testing.T) {
	jobStore := openTestStore(t)
	release := lockAndResetTables(t, jobStore)
	defer release()
	ctx := context.Background()

	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "shared-worker",
		Queues:         []string{"shared"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 4,
		Metadata:       map[string]any{"pool": "shared"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	_, err = createRunningRunForWorker(t, jobStore, ctx, worker.WorkerID, 30, 3, "shared")
	if err != nil {
		t.Fatalf("seed running run: %v", err)
	}
	_, err = jobStore.DB().ExecContext(ctx, `
		UPDATE jobs
		SET tenant_id = 'tenant-a'
		WHERE id = (SELECT job_id FROM runs WHERE worker_id = $1 LIMIT 1)
	`, worker.WorkerID)
	if err != nil {
		t.Fatalf("set running tenant: %v", err)
	}

	_, err = jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "tenant-a-pending",
		TenantID:     "tenant-a",
		Queue:        "shared",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/a"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create tenant-a pending: %v", err)
	}
	_, err = jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "tenant-b-pending",
		TenantID:     "tenant-b",
		Queue:        "shared",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/b"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create tenant-b pending: %v", err)
	}

	metrics := observability.NewRegistry()
	_, err = jobStore.UpsertTenantQuota(ctx, "tenant-a", 1, nil)
	if err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}
	scheduler := NewScheduler(log.New(io.Discard, "", 0), jobStore, config.ComponentConfig{
		ClaimBatchSize:    2,
		LeaseDuration:     30 * time.Second,
		TenantMaxInflight: 1,
	}, metrics)
	if err := scheduler.tick(ctx); err != nil {
		t.Fatalf("scheduler tick: %v", err)
	}

	rendered := metrics.Render()
	assertMetricContains(t, rendered, "runq_scheduler_skipped_runs_total{reason=\"tenant_limit\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_skipped_runs_last_tick{reason=\"tenant_limit\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_tenant_runs{state=\"inflight\",tenant=\"tenant-a\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_tenant_quota{tenant=\"tenant-a\"} 1")
	assertMetricContains(t, rendered, "runq_scheduler_tenant_runs{state=\"assigned\",tenant=\"tenant-b\"} 1")
}

func newWorkerTestMux(jobStore *store.Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/workers/register", func(w http.ResponseWriter, r *http.Request) {
		var req registerWorkerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := jobStore.RegisterWorker(r.Context(), store.RegisterWorkerInput{
			Name:           req.Name,
			Queues:         req.Queues,
			Capabilities:   req.Capabilities,
			MaxConcurrency: req.MaxConcurrency,
			Metadata:       req.Metadata,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, registerWorkerResponse{WorkerID: resp.WorkerID})
	})

	mux.HandleFunc("POST /v1/workers/{workerID}/poll", func(w http.ResponseWriter, r *http.Request) {
		var req pollWorkerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assignments, err := jobStore.PollAssignments(r.Context(), r.PathValue("workerID"), req.AvailableSlots)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := pollWorkerResponse{Assignments: make([]workerAssignment, 0, len(assignments))}
		for _, assignment := range assignments {
			resp.Assignments = append(resp.Assignments, workerAssignment{
				RunID:          assignment.RunID,
				JobID:          assignment.JobID,
				Kind:           assignment.Kind,
				Payload:        assignment.Payload,
				TimeoutSeconds: assignment.TimeoutSeconds,
				LeaseToken:     assignment.LeaseToken,
				LeaseExpiresAt: assignment.LeaseExpiry.Format(time.RFC3339),
			})
		}
		writeJSON(w, http.StatusOK, resp)
	})

	mux.HandleFunc("POST /v1/workers/{workerID}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var req heartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		updates := make([]store.HeartbeatUpdate, 0, len(req.Running))
		for _, item := range req.Running {
			updates = append(updates, store.HeartbeatUpdate{
				RunID:      item.RunID,
				LeaseToken: item.LeaseToken,
				Progress:   item.Progress,
			})
		}
		err := jobStore.HeartbeatWorker(r.Context(), r.PathValue("workerID"), updates, 30*time.Second)
		if errors.Is(err, store.ErrConflict) {
			http.Error(w, "stale worker ownership", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/workers/{workerID}/complete", func(w http.ResponseWriter, r *http.Request) {
		var req completeRunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := jobStore.CompleteRun(r.Context(), store.CompleteRunInput{
			WorkerID:   r.PathValue("workerID"),
			RunID:      req.RunID,
			LeaseToken: req.LeaseToken,
			Result:     req.Result,
		})
		if errors.Is(err, store.ErrConflict) {
			http.Error(w, "stale worker ownership", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
	})

	mux.HandleFunc("POST /v1/workers/{workerID}/fail", func(w http.ResponseWriter, r *http.Request) {
		var req failRunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := jobStore.FailRun(r.Context(), store.FailRunInput{
			WorkerID:     r.PathValue("workerID"),
			RunID:        req.RunID,
			LeaseToken:   req.LeaseToken,
			ErrorCode:    req.ErrorCode,
			ErrorMessage: req.ErrorMessage,
			Retryable:    req.Retryable,
		})
		if errors.Is(err, store.ErrConflict) {
			http.Error(w, "stale worker ownership", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func assertMetricContains(t *testing.T, rendered, pattern string) {
	t.Helper()
	if !strings.Contains(rendered, pattern) {
		t.Fatalf("expected metrics to contain %q, got:\n%s", pattern, rendered)
	}
}

func containsEvent(events []store.RunEvent, eventType string) bool {
	for _, event := range events {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()

	dbURL := os.Getenv("RUNQ_DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultTestDBURL
	}

	jobStore, err := store.Open(dbURL)
	if err != nil {
		t.Skipf("skipping integration test, db unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := jobStore.Ping(ctx); err != nil {
		t.Skipf("skipping integration test, db ping failed: %v", err)
	}

	t.Cleanup(func() {
		_ = jobStore.Close()
	})
	return jobStore
}

func resetTables(t *testing.T, jobStore *store.Store) {
	t.Helper()

	tx, err := jobStore.DB().Begin()
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

func lockAndResetTables(t *testing.T, jobStore *store.Store) func() {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := jobStore.DB().Conn(ctx)
	cancel()
	if err != nil {
		t.Fatalf("open locked connection: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(989898)`); err != nil {
		cancel()
		_ = conn.Close()
		t.Fatalf("acquire suite lock: %v", err)
	}
	cancel()

	resetTablesOnConn(t, conn)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(989898)`); err != nil {
			t.Fatalf("release suite lock: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close locked connection: %v", err)
		}
	}
}

func resetTablesOnConn(t *testing.T, conn *sql.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, `
		TRUNCATE TABLE audit_events, run_events, runs, job_schedules, workers, jobs, tenant_quotas RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func createRunningRunForWorker(t *testing.T, jobStore *store.Store, ctx context.Context, workerID string, timeoutSeconds int, maxRetries int, queue string) (string, error) {
	t.Helper()

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:                    "running-test",
		Queue:                   queue,
		Kind:                    "http",
		Payload:                 map[string]any{"url": "https://example.internal"},
		ScheduleType:            "once",
		TimeoutSeconds:          timeoutSeconds,
		MaxRetries:              maxRetries,
		RetryBackoffBaseSeconds: 1,
	})
	if err != nil {
		return "", err
	}
	if result.RunID == nil {
		t.Fatalf("expected run id")
	}

	_, err = jobStore.DB().ExecContext(ctx, `
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
		return "", err
	}

	return *result.RunID, nil
}
