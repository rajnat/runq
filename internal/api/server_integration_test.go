package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
)

const defaultTestDBURL = "postgres://runq:runq@localhost:5432/runq?sslmode=disable"

const (
	adminToken  = "admin-token"
	tenantToken = "tenant-token"
	workerToken = "worker-token"
)

func TestCreateJobAndListRunsEndpoints(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-create-" + time.Now().UTC().Format("150405.000000000")
	payload := map[string]any{
		"name":      "api-create-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload": map[string]any{
			"url": "https://example.internal/task",
		},
	}

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", payload, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}
	if createResp.JobID == "" || createResp.RunID == nil || *createResp.RunID == "" {
		t.Fatalf("expected job and run ids, got %+v", createResp)
	}

	var jobsResp struct {
		Jobs []store.Job `json:"jobs"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&queue="+queue, nil, &jobsResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing jobs, got %d", status)
	}
	foundJob := false
	for _, job := range jobsResp.Jobs {
		if job.ID == createResp.JobID {
			foundJob = true
			break
		}
	}
	if !foundJob {
		t.Fatalf("expected created job in list, got %+v", jobsResp.Jobs)
	}
	if jobsResp.Jobs[0].TenantID != "tenant-api" {
		t.Fatalf("expected tenant_id to round-trip, got %+v", jobsResp.Jobs[0])
	}

	var runsResp struct {
		Runs []store.Run `json:"runs"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&job_id="+createResp.JobID, nil, &runsResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing runs, got %d", status)
	}
	if len(runsResp.Runs) != 1 || runsResp.Runs[0].Status != "PENDING" {
		t.Fatalf("expected one pending run, got %+v", runsResp.Runs)
	}
	if runsResp.Runs[0].TenantID != "tenant-api" {
		t.Fatalf("expected run tenant_id to round-trip, got %+v", runsResp.Runs[0])
	}
	if runsResp.Runs[0].JobName != "api-create-test" || runsResp.Runs[0].Queue != queue || runsResp.Runs[0].Kind != "http" {
		t.Fatalf("expected run list to include job metadata, got %+v", runsResp.Runs[0])
	}
	if runsResp.Runs[0].JobDisabled {
		t.Fatalf("expected active job in run listing, got %+v", runsResp.Runs[0])
	}
}

func TestCreateDelayedJobEndpoint(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	runAt := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Second)
	queue := "api-delayed-" + time.Now().UTC().Format("150405.000000000")

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-delayed-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/delayed"},
		"schedule": map[string]any{
			"type":   "delayed",
			"run_at": runAt.Format(time.RFC3339),
		},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}
	if createResp.RunID == nil || *createResp.RunID == "" {
		t.Fatalf("expected delayed job to create a pending run, got %+v", createResp)
	}

	var jobResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs/"+createResp.JobID, nil, &jobResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get job, got %d", status)
	}
	if jobResp.Job.ScheduleType != "delayed" {
		t.Fatalf("expected delayed schedule type, got %+v", jobResp.Job)
	}

	var runResp GetRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs/"+*createResp.RunID, nil, &runResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get run, got %d", status)
	}
	if !runResp.Run.ScheduledAt.Equal(runAt) {
		t.Fatalf("expected scheduled_at %s, got %+v", runAt.Format(time.RFC3339), runResp.Run)
	}
	if !runResp.Run.AvailableAt.Equal(runAt) {
		t.Fatalf("expected available_at %s, got %+v", runAt.Format(time.RFC3339), runResp.Run)
	}
}

func TestGetJobEndpointReturnsCreatedJob(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-get-job-" + time.Now().UTC().Format("150405.000000000")
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-get-job-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/get-job"},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}

	var jobResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs/"+createResp.JobID, nil, &jobResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get job, got %d", status)
	}
	if jobResp.Job.ID != createResp.JobID || jobResp.Job.Name != "api-get-job-test" || jobResp.Job.Queue != queue {
		t.Fatalf("unexpected job response: %+v", jobResp.Job)
	}
}

func TestUpdateJobEndpoint(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-update-" + time.Now().UTC().Format("150405.000000000")
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-update-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/original"},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}

	var updateResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPatch, httpServer.URL+"/v1/jobs/"+createResp.JobID, map[string]any{
		"name":                       "api-update-test-v2",
		"queue":                      queue + "-updated",
		"payload":                    map[string]any{"url": "https://example.internal/updated", "method": "POST"},
		"priority":                   7,
		"max_retries":                8,
		"timeout_seconds":            45,
		"retry_backoff_base_seconds": 9,
		"concurrency_key":            "tenant-api:updated",
	}, &updateResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 patch job, got %d", status)
	}
	if updateResp.Job.Name != "api-update-test-v2" || updateResp.Job.Queue != queue+"-updated" {
		t.Fatalf("unexpected patch response: %+v", updateResp.Job)
	}
	if updateResp.Job.Priority != 7 || updateResp.Job.MaxRetries != 8 || updateResp.Job.TimeoutSeconds != 45 || updateResp.Job.RetryBackoffBaseSeconds != 9 {
		t.Fatalf("expected numeric updates to round-trip, got %+v", updateResp.Job)
	}
	if updateResp.Job.ConcurrencyKey == nil || *updateResp.Job.ConcurrencyKey != "tenant-api:updated" {
		t.Fatalf("expected concurrency key update, got %+v", updateResp.Job)
	}
	if updateResp.Job.Payload["method"] != "POST" {
		t.Fatalf("expected payload update, got %+v", updateResp.Job.Payload)
	}
}

func TestUpdateDelayedJobSchedule(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	initialRunAt := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Second)
	updatedRunAt := initialRunAt.Add(45 * time.Minute)
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-update-delayed",
		"tenant_id": "tenant-api",
		"queue":     "api-update-delayed",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/delayed"},
		"schedule": map[string]any{
			"type":   "delayed",
			"run_at": initialRunAt.Format(time.RFC3339),
		},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 create delayed job, got %d", status)
	}

	var updateResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPatch, httpServer.URL+"/v1/jobs/"+createResp.JobID, map[string]any{
		"schedule": map[string]any{
			"type":   "delayed",
			"run_at": updatedRunAt.Format(time.RFC3339),
		},
	}, &updateResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 patch delayed job, got %d", status)
	}
	if updateResp.Job.ScheduleType != "delayed" {
		t.Fatalf("expected delayed schedule type, got %+v", updateResp.Job)
	}

	var runResp GetRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs/"+*createResp.RunID, nil, &runResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get delayed run, got %d", status)
	}
	if !runResp.Run.ScheduledAt.Equal(updatedRunAt) || !runResp.Run.AvailableAt.Equal(updatedRunAt) {
		t.Fatalf("expected delayed run timestamps to update to %s, got %+v", updatedRunAt.Format(time.RFC3339), runResp.Run)
	}
}

func TestUpdateCronJobSchedule(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-update-cron",
		"tenant_id": "tenant-api",
		"queue":     "api-update-cron",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/cron"},
		"schedule": map[string]any{
			"type":     "cron",
			"cron":     "*/5 * * * *",
			"timezone": "UTC",
		},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 create cron job, got %d", status)
	}

	var beforeCronExpr string
	var beforeTimezone string
	var beforeNextRunAt time.Time
	if err := jobStore.DB().QueryRowContext(context.Background(), `SELECT cron_expr, timezone, next_run_at FROM job_schedules WHERE job_id = $1`, createResp.JobID).Scan(&beforeCronExpr, &beforeTimezone, &beforeNextRunAt); err != nil {
		t.Fatalf("load cron schedule before update: %v", err)
	}

	var updateResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPatch, httpServer.URL+"/v1/jobs/"+createResp.JobID, map[string]any{
		"schedule": map[string]any{
			"type":     "cron",
			"cron":     "0 * * * *",
			"timezone": "America/New_York",
		},
	}, &updateResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 patch cron job, got %d", status)
	}
	if updateResp.Job.ScheduleType != "cron" {
		t.Fatalf("expected cron schedule type, got %+v", updateResp.Job)
	}
	if updateResp.Job.CronExpr == nil || *updateResp.Job.CronExpr != "0 * * * *" {
		t.Fatalf("expected updated cron expression, got %+v", updateResp.Job)
	}
	if updateResp.Job.Timezone == nil || *updateResp.Job.Timezone != "America/New_York" {
		t.Fatalf("expected updated timezone, got %+v", updateResp.Job)
	}

	var afterCronExpr string
	var afterTimezone string
	var afterNextRunAt time.Time
	if err := jobStore.DB().QueryRowContext(context.Background(), `SELECT cron_expr, timezone, next_run_at FROM job_schedules WHERE job_id = $1`, createResp.JobID).Scan(&afterCronExpr, &afterTimezone, &afterNextRunAt); err != nil {
		t.Fatalf("load cron schedule after update: %v", err)
	}
	if afterCronExpr != "0 * * * *" || afterTimezone != "America/New_York" {
		t.Fatalf("expected persisted cron schedule update, got cron=%q timezone=%q", afterCronExpr, afterTimezone)
	}
	if afterNextRunAt.Before(beforeNextRunAt) {
		t.Fatalf("expected recomputed next_run_at to stay current, before=%s after=%s", beforeNextRunAt, afterNextRunAt)
	}
}

func TestListJobsSupportsLimitAndOffset(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createdJobIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      "api-pagination-job-" + string(rune('a'+i)),
			"tenant_id": "tenant-api",
			"queue":     "api-pagination-jobs",
			"kind":      "http",
			"payload":   map[string]any{"index": i},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %d, got %d", i, status)
		}
		createdJobIDs = append([]string{createResp.JobID}, createdJobIDs...)
		time.Sleep(10 * time.Millisecond)
	}

	var jobsResp struct {
		Jobs       []store.Job    `json:"jobs"`
		Pagination PaginationMeta `json:"pagination"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=2&offset=1", nil, &jobsResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing jobs, got %d", status)
	}
	if len(jobsResp.Jobs) != 2 {
		t.Fatalf("expected 2 jobs after pagination, got %+v", jobsResp.Jobs)
	}
	if jobsResp.Jobs[0].ID != createdJobIDs[1] || jobsResp.Jobs[1].ID != createdJobIDs[2] {
		t.Fatalf("unexpected paginated jobs: %+v expected ids=%+v", jobsResp.Jobs, createdJobIDs)
	}
	if jobsResp.Pagination.Limit != 2 || jobsResp.Pagination.Offset != 1 || jobsResp.Pagination.Returned != 2 || jobsResp.Pagination.HasMore {
		t.Fatalf("unexpected jobs pagination metadata: %+v", jobsResp.Pagination)
	}
	if jobsResp.Pagination.NextOffset != nil {
		t.Fatalf("expected no next_offset, got %+v", jobsResp.Pagination)
	}
}

func TestListJobsSupportsNewFilters(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createJob := func(name string) string {
		t.Helper()
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      name,
			"tenant_id": "tenant-api",
			"queue":     "api-job-filters",
			"kind":      "http",
			"payload":   map[string]any{"name": name},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %s, got %d", name, status)
		}
		return createResp.JobID
	}

	alphaID := createJob("filter-job-alpha")
	time.Sleep(10 * time.Millisecond)
	betaID := createJob("filter-job-beta")
	time.Sleep(10 * time.Millisecond)
	gammaID := createJob("filter-job-gamma")

	alphaCreated := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Second)
	alphaUpdated := alphaCreated.Add(30 * time.Minute)
	betaCreated := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	betaUpdated := betaCreated.Add(20 * time.Minute)
	gammaCreated := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	gammaUpdated := gammaCreated.Add(10 * time.Minute)
	for _, update := range []struct {
		id             string
		dedupeKey      string
		concurrencyKey string
		createdAt      time.Time
		updatedAt      time.Time
	}{
		{alphaID, "dedupe-alpha", "account-a", alphaCreated, alphaUpdated},
		{betaID, "dedupe-beta", "account-b", betaCreated, betaUpdated},
		{gammaID, "dedupe-shared", "account-shared", gammaCreated, gammaUpdated},
	} {
		if _, err := jobStore.DB().ExecContext(ctx, `
			UPDATE jobs
			SET dedupe_key = $2,
			    concurrency_key = $3,
			    created_at = $4,
			    updated_at = $5
			WHERE id = $1
		`, update.id, update.dedupeKey, update.concurrencyKey, update.createdAt, update.updatedAt); err != nil {
			t.Fatalf("seed job filter fields for %s: %v", update.id, err)
		}
	}

	assertJobIDs := func(url string, expected ...string) {
		t.Helper()
		var jobsResp struct {
			Jobs []store.Job `json:"jobs"`
		}
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, url, nil, &jobsResp)
		if status != http.StatusOK {
			t.Fatalf("expected 200 listing jobs for %s, got %d", url, status)
		}
		if len(jobsResp.Jobs) != len(expected) {
			t.Fatalf("expected %d jobs for %s, got %+v", len(expected), url, jobsResp.Jobs)
		}
		for i, jobID := range expected {
			if jobsResp.Jobs[i].ID != jobID {
				t.Fatalf("unexpected jobs for %s: got %+v expected ids=%+v", url, jobsResp.Jobs, expected)
			}
		}
	}

	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&name=filter-job-beta", betaID)
	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&dedupe_key=dedupe-shared", gammaID)
	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&concurrency_key=account-a", alphaID)
	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&created_after="+betaCreated.Add(-1*time.Minute).Format(time.RFC3339)+"&created_before="+betaCreated.Add(1*time.Minute).Format(time.RFC3339), betaID)
	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&updated_after="+gammaUpdated.Add(-1*time.Minute).Format(time.RFC3339)+"&updated_before="+gammaUpdated.Add(1*time.Minute).Format(time.RFC3339), gammaID)
	assertJobIDs(httpServer.URL+"/v1/jobs?tenant_id=tenant-api&dedupe_key=dedupe-beta&created_after="+betaCreated.Add(-1*time.Minute).Format(time.RFC3339)+"&updated_before="+betaUpdated.Add(1*time.Minute).Format(time.RFC3339), betaID)
}

func TestListRunsSupportsLimitAndOffset(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createdRunIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      "api-pagination-run-" + string(rune('a'+i)),
			"tenant_id": "tenant-api",
			"queue":     "api-pagination-runs",
			"kind":      "http",
			"payload":   map[string]any{"index": i},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %d, got %d", i, status)
		}
		createdRunIDs = append([]string{*createResp.RunID}, createdRunIDs...)
		time.Sleep(10 * time.Millisecond)
	}

	var runsResp struct {
		Runs       []store.Run    `json:"runs"`
		Pagination PaginationMeta `json:"pagination"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=2&offset=1", nil, &runsResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing runs, got %d", status)
	}
	if len(runsResp.Runs) != 2 {
		t.Fatalf("expected 2 runs after pagination, got %+v", runsResp.Runs)
	}
	if runsResp.Runs[0].ID != createdRunIDs[1] || runsResp.Runs[1].ID != createdRunIDs[2] {
		t.Fatalf("unexpected paginated runs: %+v expected ids=%+v", runsResp.Runs, createdRunIDs)
	}
	if runsResp.Pagination.Limit != 2 || runsResp.Pagination.Offset != 1 || runsResp.Pagination.Returned != 2 || runsResp.Pagination.HasMore {
		t.Fatalf("unexpected runs pagination metadata: %+v", runsResp.Pagination)
	}
	if runsResp.Pagination.NextOffset != nil {
		t.Fatalf("expected no next_offset, got %+v", runsResp.Pagination)
	}
}

func TestListRunsSupportsNewFilters(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createRun := func(name string) string {
		t.Helper()
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      name,
			"tenant_id": "tenant-api",
			"queue":     "api-run-filters",
			"kind":      "http",
			"payload":   map[string]any{"name": name},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %s, got %d", name, status)
		}
		return *createResp.RunID
	}

	alphaRunID := createRun("run-filter-alpha")
	time.Sleep(10 * time.Millisecond)
	betaRunID := createRun("run-filter-beta")
	time.Sleep(10 * time.Millisecond)
	gammaRunID := createRun("run-filter-gamma")

	alphaScheduled := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Second)
	alphaCompleted := alphaScheduled.Add(5 * time.Minute)
	betaScheduled := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	betaCompleted := betaScheduled.Add(7 * time.Minute)
	gammaScheduled := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	alphaErrorCode := "HTTP_500"
	betaErrorCode := "TIMEOUT"
	for _, update := range []struct {
		id          string
		status      string
		attempt     int
		errorCode   *string
		scheduledAt time.Time
		completedAt *time.Time
	}{
		{alphaRunID, "FAILED", 2, &alphaErrorCode, alphaScheduled, &alphaCompleted},
		{betaRunID, "FAILED", 3, &betaErrorCode, betaScheduled, &betaCompleted},
		{gammaRunID, "PENDING", 1, nil, gammaScheduled, nil},
	} {
		if _, err := jobStore.DB().ExecContext(ctx, `
			UPDATE runs
			SET status = $2,
			    attempt = $3,
			    error_code = $4,
			    scheduled_at = $5,
			    available_at = $5,
			    completed_at = $6,
			    updated_at = NOW()
			WHERE id = $1
		`, update.id, update.status, update.attempt, update.errorCode, update.scheduledAt, update.completedAt); err != nil {
			t.Fatalf("seed run filter fields for %s: %v", update.id, err)
		}
	}

	assertRunIDs := func(url string, expected ...string) {
		t.Helper()
		var runsResp struct {
			Runs []store.Run `json:"runs"`
		}
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, url, nil, &runsResp)
		if status != http.StatusOK {
			t.Fatalf("expected 200 listing runs for %s, got %d", url, status)
		}
		if len(runsResp.Runs) != len(expected) {
			t.Fatalf("expected %d runs for %s, got %+v", len(expected), url, runsResp.Runs)
		}
		for i, runID := range expected {
			if runsResp.Runs[i].ID != runID {
				t.Fatalf("unexpected runs for %s: got %+v expected ids=%+v", url, runsResp.Runs, expected)
			}
		}
	}

	assertRunIDs(httpServer.URL+"/v1/runs?tenant_id=tenant-api&error_code=TIMEOUT", betaRunID)
	assertRunIDs(httpServer.URL+"/v1/runs?tenant_id=tenant-api&attempt=2", alphaRunID)
	assertRunIDs(httpServer.URL+"/v1/runs?tenant_id=tenant-api&scheduled_after="+betaScheduled.Add(-1*time.Minute).Format(time.RFC3339)+"&scheduled_before="+betaScheduled.Add(1*time.Minute).Format(time.RFC3339), betaRunID)
	assertRunIDs(httpServer.URL+"/v1/runs?tenant_id=tenant-api&completed_after="+alphaCompleted.Add(-1*time.Minute).Format(time.RFC3339)+"&completed_before="+alphaCompleted.Add(1*time.Minute).Format(time.RFC3339), alphaRunID)
	assertRunIDs(httpServer.URL+"/v1/runs?tenant_id=tenant-api&error_code=HTTP_500&attempt=2&completed_before="+alphaCompleted.Add(1*time.Minute).Format(time.RFC3339), alphaRunID)
}

func TestListJobsSupportsCursorPagination(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createdJobIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      "api-cursor-job-" + string(rune('a'+i)),
			"tenant_id": "tenant-api",
			"queue":     "api-cursor-jobs",
			"kind":      "http",
			"payload":   map[string]any{"index": i},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %d, got %d", i, status)
		}
		createdJobIDs = append([]string{createResp.JobID}, createdJobIDs...)
		time.Sleep(10 * time.Millisecond)
	}

	firstPage := ListJobsResponse{}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=2", nil, &firstPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing first jobs page, got %d", status)
	}
	if len(firstPage.Jobs) != 2 || !firstPage.Pagination.HasMore || firstPage.Pagination.NextCursor == nil {
		t.Fatalf("expected first jobs page with next cursor, got %+v", firstPage)
	}
	if firstPage.Jobs[0].ID != createdJobIDs[0] || firstPage.Jobs[1].ID != createdJobIDs[1] {
		t.Fatalf("unexpected first jobs page: %+v expected ids=%+v", firstPage.Jobs, createdJobIDs)
	}

	secondPage := ListJobsResponse{}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=2&cursor="+*firstPage.Pagination.NextCursor, nil, &secondPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing second jobs page, got %d", status)
	}
	if len(secondPage.Jobs) != 1 || secondPage.Jobs[0].ID != createdJobIDs[2] {
		t.Fatalf("unexpected second jobs page: %+v expected ids=%+v", secondPage.Jobs, createdJobIDs)
	}
	if secondPage.Pagination.HasMore || secondPage.Pagination.NextCursor != nil {
		t.Fatalf("expected final jobs page without next cursor, got %+v", secondPage.Pagination)
	}
}

func TestListRunsSupportsCursorPagination(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createdRunIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      "api-cursor-run-" + string(rune('a'+i)),
			"tenant_id": "tenant-api",
			"queue":     "api-cursor-runs",
			"kind":      "http",
			"payload":   map[string]any{"index": i},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating run source job %d, got %d", i, status)
		}
		createdRunIDs = append([]string{*createResp.RunID}, createdRunIDs...)
		time.Sleep(10 * time.Millisecond)
	}

	firstPage := ListRunsResponse{}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=2", nil, &firstPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing first runs page, got %d", status)
	}
	if len(firstPage.Runs) != 2 || !firstPage.Pagination.HasMore || firstPage.Pagination.NextCursor == nil {
		t.Fatalf("expected first runs page with next cursor, got %+v", firstPage)
	}
	if firstPage.Runs[0].ID != createdRunIDs[0] || firstPage.Runs[1].ID != createdRunIDs[1] {
		t.Fatalf("unexpected first runs page: %+v expected ids=%+v", firstPage.Runs, createdRunIDs)
	}

	secondPage := ListRunsResponse{}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=2&cursor="+*firstPage.Pagination.NextCursor, nil, &secondPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing second runs page, got %d", status)
	}
	if len(secondPage.Runs) != 1 || secondPage.Runs[0].ID != createdRunIDs[2] {
		t.Fatalf("unexpected second runs page: %+v expected ids=%+v", secondPage.Runs, createdRunIDs)
	}
	if secondPage.Pagination.HasMore || secondPage.Pagination.NextCursor != nil {
		t.Fatalf("expected final runs page without next cursor, got %+v", secondPage.Pagination)
	}
}

func TestListWorkersSupportsCursorPagination(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	createdWorkerIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		var resp RegisterWorkerResponse
		status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
			"name":            "cursor-worker-" + string(rune('a'+i)),
			"queues":          []string{"default"},
			"capabilities":    map[string]any{"http": true},
			"max_concurrency": 1,
		}, &resp)
		if status != http.StatusCreated {
			t.Fatalf("expected 201 creating worker %d, got %d", i, status)
		}
		createdWorkerIDs = append([]string{resp.WorkerID}, createdWorkerIDs...)
		time.Sleep(10 * time.Millisecond)
	}

	var firstPage struct {
		Workers    []store.Worker `json:"workers"`
		Pagination PaginationMeta `json:"pagination"`
	}
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?limit=2", nil, &firstPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing first workers page, got %d", status)
	}
	if len(firstPage.Workers) != 2 || !firstPage.Pagination.HasMore || firstPage.Pagination.NextCursor == nil {
		t.Fatalf("expected first workers page with next cursor, got %+v", firstPage)
	}
	if firstPage.Workers[0].ID != createdWorkerIDs[0] || firstPage.Workers[1].ID != createdWorkerIDs[1] {
		t.Fatalf("unexpected first workers page: %+v expected ids=%+v", firstPage.Workers, createdWorkerIDs)
	}

	var secondPage struct {
		Workers    []store.Worker `json:"workers"`
		Pagination PaginationMeta `json:"pagination"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?limit=2&cursor="+*firstPage.Pagination.NextCursor, nil, &secondPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing second workers page, got %d", status)
	}
	if len(secondPage.Workers) != 1 || secondPage.Workers[0].ID != createdWorkerIDs[2] {
		t.Fatalf("unexpected second workers page: %+v expected ids=%+v", secondPage.Workers, createdWorkerIDs)
	}
	if secondPage.Pagination.HasMore || secondPage.Pagination.NextCursor != nil {
		t.Fatalf("expected final workers page without next cursor, got %+v", secondPage.Pagination)
	}
}

func TestListWorkersSupportsStatusQueueAndCapabilityFilters(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	register := func(name string, queues []string, capabilities map[string]any) string {
		t.Helper()
		var resp RegisterWorkerResponse
		status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
			"name":            name,
			"queues":          queues,
			"capabilities":    capabilities,
			"max_concurrency": 1,
		}, &resp)
		if status != http.StatusCreated {
			t.Fatalf("expected 201 registering %s, got %d", name, status)
		}
		return resp.WorkerID
	}

	alphaID := register("filter-worker-alpha", []string{"default"}, map[string]any{"http": true})
	time.Sleep(10 * time.Millisecond)
	betaID := register("filter-worker-beta", []string{"priority"}, map[string]any{"shell": true})
	time.Sleep(10 * time.Millisecond)
	gammaID := register("filter-worker-gamma", []string{"default", "priority"}, map[string]any{"http": true, "shell": true})

	var lifecycleResp struct {
		WorkerID string `json:"worker_id"`
		Status   string `json:"status"`
	}
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+betaID+"/drain", nil, &lifecycleResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 draining beta worker, got %d", status)
	}

	var drainedResp struct {
		Workers []store.Worker `json:"workers"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?status=drained", nil, &drainedResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing drained workers, got %d", status)
	}
	if len(drainedResp.Workers) != 1 || drainedResp.Workers[0].ID != betaID {
		t.Fatalf("expected only drained beta worker, got %+v", drainedResp.Workers)
	}

	var priorityResp struct {
		Workers []store.Worker `json:"workers"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?queue=priority", nil, &priorityResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing priority workers, got %d", status)
	}
	if len(priorityResp.Workers) != 2 || priorityResp.Workers[0].ID != gammaID || priorityResp.Workers[1].ID != betaID {
		t.Fatalf("expected priority workers gamma then beta, got %+v", priorityResp.Workers)
	}

	var shellResp struct {
		Workers []store.Worker `json:"workers"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?capability=shell", nil, &shellResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing shell workers, got %d", status)
	}
	if len(shellResp.Workers) != 2 || shellResp.Workers[0].ID != gammaID || shellResp.Workers[1].ID != betaID {
		t.Fatalf("expected shell workers gamma then beta, got %+v", shellResp.Workers)
	}

	var combinedResp struct {
		Workers []store.Worker `json:"workers"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?status=healthy&queue=default&capability=http", nil, &combinedResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing combined worker filters, got %d", status)
	}
	if len(combinedResp.Workers) != 2 || combinedResp.Workers[0].ID != gammaID || combinedResp.Workers[1].ID != alphaID {
		t.Fatalf("expected healthy default http workers gamma then alpha, got %+v", combinedResp.Workers)
	}
}

func TestWorkerDetailAndLifecycleControls(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var registerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "detail-worker",
		"queues":          []string{"default", "priority"},
		"capabilities":    map[string]any{"http": true, "shell": true},
		"max_concurrency": 2,
		"metadata":        map[string]any{"zone": "us-east-1"},
	}, &registerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	var detailResp struct {
		Worker store.Worker `json:"worker"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers/"+registerResp.WorkerID, nil, &detailResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 worker detail, got %d", status)
	}
	if detailResp.Worker.Name != "detail-worker" || detailResp.Worker.Status != "healthy" {
		t.Fatalf("unexpected worker detail: %+v", detailResp.Worker)
	}
	if len(detailResp.Worker.Queues) != 2 || !detailResp.Worker.Capabilities["http"].(bool) {
		t.Fatalf("expected queues/capabilities in worker detail, got %+v", detailResp.Worker)
	}
	if detailResp.Worker.Metadata["zone"] != "us-east-1" {
		t.Fatalf("expected metadata in worker detail, got %+v", detailResp.Worker)
	}

	var lifecycleResp struct {
		WorkerID string `json:"worker_id"`
		Status   string `json:"status"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/drain", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "drained" {
		t.Fatalf("expected drained worker response, got status=%d body=%+v", status, lifecycleResp)
	}

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "drain-check-job",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	assignments, _, err := jobStore.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected drained worker to receive no assignments, got %+v", assignments)
	}
	if _, _, err := jobStore.GetRun(ctx, *result.RunID); err != nil {
		t.Fatalf("get pending run: %v", err)
	}

	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/decommission", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "decommissioned" {
		t.Fatalf("expected decommissioned worker response, got status=%d body=%+v", status, lifecycleResp)
	}

	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers/"+registerResp.WorkerID, nil, &detailResp)
	if status != http.StatusOK || detailResp.Worker.Status != "decommissioned" {
		t.Fatalf("expected decommissioned worker detail, got status=%d body=%+v", status, detailResp)
	}
}

func TestReactivateWorkerRestoresHealthyStatusAndAssignments(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var registerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "reactivate-worker",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &registerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	var lifecycleResp struct {
		WorkerID string `json:"worker_id"`
		Status   string `json:"status"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/drain", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "drained" {
		t.Fatalf("expected drained worker response, got status=%d body=%+v", status, lifecycleResp)
	}

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "reactivate-check-job",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	assignments, _, err := jobStore.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs while drained: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected drained worker to receive no assignments, got %+v", assignments)
	}

	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/reactivate", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "healthy" {
		t.Fatalf("expected healthy worker response, got status=%d body=%+v", status, lifecycleResp)
	}

	var detailResp struct {
		Worker store.Worker `json:"worker"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers/"+registerResp.WorkerID, nil, &detailResp)
	if status != http.StatusOK || detailResp.Worker.Status != "healthy" {
		t.Fatalf("expected healthy worker detail, got status=%d body=%+v", status, detailResp)
	}

	assignments, _, err = jobStore.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs after reactivate: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected one assignment after reactivate, got %+v", assignments)
	}
	if assignments[0].WorkerID != registerResp.WorkerID || assignments[0].RunID != *result.RunID {
		t.Fatalf("expected reactivated worker to receive pending run, got %+v", assignments[0])
	}
}

func TestReactivateWorkerRejectsDecommissionedState(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var registerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "decommissioned-worker",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &registerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	var lifecycleResp struct {
		WorkerID string `json:"worker_id"`
		Status   string `json:"status"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/decommission", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "decommissioned" {
		t.Fatalf("expected decommissioned worker response, got status=%d body=%+v", status, lifecycleResp)
	}

	var errResp errorEnvelope
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/"+registerResp.WorkerID+"/reactivate", nil, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 reactivating decommissioned worker, got %d with %+v", status, errResp)
	}
	if errResp.Error.Code != "WORKER_REACTIVATE_CONFLICT" {
		t.Fatalf("expected worker reactivate conflict code, got %+v", errResp)
	}

	var detailResp struct {
		Worker store.Worker `json:"worker"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers/"+registerResp.WorkerID, nil, &detailResp)
	if status != http.StatusOK || detailResp.Worker.Status != "decommissioned" {
		t.Fatalf("expected decommissioned worker detail after failed reactivate, got status=%d body=%+v", status, detailResp)
	}
}

func TestWorkerDetailIncludesInflightAssignmentsAndHealthSummary(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var registerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "detail-health-worker",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 2,
	}, &registerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "detail-health-job",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	assignments, _, err := jobStore.ClaimPendingRuns(ctx, 10, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected one assignment, got %+v", assignments)
	}

	staleAt := time.Now().UTC().Add(-20 * time.Second)
	if _, err := jobStore.DB().ExecContext(ctx, `UPDATE workers SET last_heartbeat_at = $2 WHERE id = $1`, registerResp.WorkerID, staleAt); err != nil {
		t.Fatalf("set stale heartbeat: %v", err)
	}

	var detailResp struct {
		Worker struct {
			ID                      string `json:"id"`
			Status                  string `json:"status"`
			MaxConcurrency          int    `json:"max_concurrency"`
			InflightAssignmentCount int    `json:"inflight_assignment_count"`
			InflightRuns            []struct {
				RunID      string `json:"run_id"`
				JobID      string `json:"job_id"`
				TenantID   string `json:"tenant_id"`
				Queue      string `json:"queue"`
				Status     string `json:"status"`
				LeaseToken int64  `json:"lease_token"`
			} `json:"inflight_runs"`
			Health struct {
				HeartbeatAgeSeconds   int64 `json:"heartbeat_age_seconds"`
				HeartbeatDriftSeconds int64 `json:"heartbeat_drift_seconds"`
				HeartbeatStale        bool  `json:"heartbeat_stale"`
				InflightAssignments   int   `json:"inflight_assignments"`
				AvailableCapacity     int   `json:"available_capacity"`
				AtCapacity            bool  `json:"at_capacity"`
			} `json:"health"`
		} `json:"worker"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers/"+registerResp.WorkerID, nil, &detailResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 worker detail, got %d", status)
	}
	if detailResp.Worker.InflightAssignmentCount != 1 {
		t.Fatalf("expected one inflight assignment, got %+v", detailResp.Worker)
	}
	if len(detailResp.Worker.InflightRuns) != 1 {
		t.Fatalf("expected one inflight run summary, got %+v", detailResp.Worker.InflightRuns)
	}
	if detailResp.Worker.InflightRuns[0].RunID != *result.RunID || detailResp.Worker.InflightRuns[0].JobID != result.JobID || detailResp.Worker.InflightRuns[0].TenantID != "tenant-api" || detailResp.Worker.InflightRuns[0].Queue != "default" || detailResp.Worker.InflightRuns[0].Status != "RUNNING" {
		t.Fatalf("unexpected inflight run summary: %+v", detailResp.Worker.InflightRuns[0])
	}
	if detailResp.Worker.InflightRuns[0].LeaseToken == 0 {
		t.Fatalf("expected lease token in inflight run summary, got %+v", detailResp.Worker.InflightRuns[0])
	}
	if !detailResp.Worker.Health.HeartbeatStale || detailResp.Worker.Health.HeartbeatDriftSeconds <= 0 {
		t.Fatalf("expected stale health summary, got %+v", detailResp.Worker.Health)
	}
	if detailResp.Worker.Health.InflightAssignments != 1 || detailResp.Worker.Health.AvailableCapacity != 1 || detailResp.Worker.Health.AtCapacity {
		t.Fatalf("unexpected capacity summary: %+v", detailResp.Worker.Health)
	}
	if detailResp.Worker.Health.HeartbeatAgeSeconds <= 0 {
		t.Fatalf("expected positive heartbeat age, got %+v", detailResp.Worker.Health)
	}
}

func TestListAuditEventsSupportsCursorPagination(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	for i := 0; i < 3; i++ {
		status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-cursor/quota", map[string]any{
			"max_inflight":     i + 1,
			"max_pending_runs": i + 2,
			"max_active_jobs":  i + 3,
		}, &TenantQuotaResponse{})
		if status != http.StatusOK {
			t.Fatalf("expected 200 upserting quota %d, got %d", i, status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	firstPage := ListAuditEventsResponse{}
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?action=TENANT_QUOTA_UPSERT&limit=2", nil, &firstPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing first audit page, got %d", status)
	}
	if len(firstPage.Events) != 2 || !firstPage.Pagination.HasMore || firstPage.Pagination.NextCursor == nil {
		t.Fatalf("expected first audit page with next cursor, got %+v", firstPage)
	}

	secondPage := ListAuditEventsResponse{}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?action=TENANT_QUOTA_UPSERT&limit=2&cursor="+*firstPage.Pagination.NextCursor, nil, &secondPage)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing second audit page, got %d", status)
	}
	if len(secondPage.Events) != 1 {
		t.Fatalf("expected final audit page with one event, got %+v", secondPage.Events)
	}
	if secondPage.Pagination.HasMore || secondPage.Pagination.NextCursor != nil {
		t.Fatalf("expected final audit page without next cursor, got %+v", secondPage.Pagination)
	}
}

func TestListEndpointsCapRequestedPageSize(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-limit-job",
		"tenant_id": "tenant-api",
		"queue":     "api-limit-jobs",
		"kind":      "http",
		"payload":   map[string]any{"index": 0},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 creating job, got %d", status)
	}

	var workerResp RegisterWorkerResponse
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-limit-test",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &workerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-limit/quota", map[string]any{
		"max_inflight": 1,
	}, &TenantQuotaResponse{})
	if status != http.StatusOK {
		t.Fatalf("expected 200 upserting quota, got %d", status)
	}

	jobsResp := ListJobsResponse{}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=500", nil, &jobsResp)
	if status != http.StatusOK || jobsResp.Pagination.Limit != 200 {
		t.Fatalf("expected jobs limit cap 200, got status=%d resp=%+v", status, jobsResp.Pagination)
	}

	runsResp := ListRunsResponse{}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=500", nil, &runsResp)
	if status != http.StatusOK || runsResp.Pagination.Limit != 200 {
		t.Fatalf("expected runs limit cap 200, got status=%d resp=%+v", status, runsResp.Pagination)
	}

	workersResp := ListWorkersResponse{}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?limit=500", nil, &workersResp)
	if status != http.StatusOK || workersResp.Pagination.Limit != 200 {
		t.Fatalf("expected workers limit cap 200, got status=%d resp=%+v", status, workersResp.Pagination)
	}

	auditResp := ListAuditEventsResponse{}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?limit=500", nil, &auditResp)
	if status != http.StatusOK || auditResp.Pagination.Limit != 200 {
		t.Fatalf("expected audit limit cap 200, got status=%d resp=%+v", status, auditResp.Pagination)
	}
}

func TestListEndpointsRejectOffsetAndCursorTogether(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	for i := 0; i < 2; i++ {
		var createResp CreateJobResponse
		status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
			"name":      fmt.Sprintf("api-cursor-mix-job-%d", i),
			"tenant_id": "tenant-api",
			"queue":     "default",
			"kind":      "http",
			"payload":   map[string]any{"index": i},
		}, &createResp)
		if status != http.StatusAccepted {
			t.Fatalf("expected 202 creating job %d, got %d", i, status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	firstJobs := ListJobsResponse{}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=1", nil, &firstJobs)
	if status != http.StatusOK || firstJobs.Pagination.NextCursor == nil {
		t.Fatalf("expected first jobs page with cursor, got status=%d resp=%+v", status, firstJobs)
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&limit=1&offset=1&cursor="+*firstJobs.Pagination.NextCursor, nil, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 mixing jobs offset and cursor, got %d", status)
	}

	firstRuns := ListRunsResponse{}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=1", nil, &firstRuns)
	if status != http.StatusOK || firstRuns.Pagination.NextCursor == nil {
		t.Fatalf("expected first runs page with cursor, got status=%d resp=%+v", status, firstRuns)
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&limit=1&offset=1&cursor="+*firstRuns.Pagination.NextCursor, nil, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 mixing runs offset and cursor, got %d", status)
	}

	for i := 0; i < 2; i++ {
		var workerResp RegisterWorkerResponse
		status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
			"name":            fmt.Sprintf("worker-cursor-mix-%d", i),
			"queues":          []string{"default"},
			"capabilities":    map[string]any{"http": true},
			"max_concurrency": 1,
		}, &workerResp)
		if status != http.StatusCreated {
			t.Fatalf("expected 201 registering worker %d, got %d", i, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	firstWorkers := ListWorkersResponse{}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?limit=1", nil, &firstWorkers)
	if status != http.StatusOK || firstWorkers.Pagination.NextCursor == nil {
		t.Fatalf("expected first workers page with cursor, got status=%d resp=%+v", status, firstWorkers)
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/workers?limit=1&offset=1&cursor="+*firstWorkers.Pagination.NextCursor, nil, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 mixing workers offset and cursor, got %d", status)
	}

	for i := 0; i < 2; i++ {
		status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+fmt.Sprintf("/v1/tenants/tenant-cursor-mix-%d/quota", i), map[string]any{
			"max_inflight": i + 1,
		}, &TenantQuotaResponse{})
		if status != http.StatusOK {
			t.Fatalf("expected 200 upserting quota %d, got %d", i, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	firstAudit := ListAuditEventsResponse{}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?limit=1", nil, &firstAudit)
	if status != http.StatusOK || firstAudit.Pagination.NextCursor == nil {
		t.Fatalf("expected first audit page with cursor, got status=%d resp=%+v", status, firstAudit)
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?limit=1&offset=1&cursor="+*firstAudit.Pagination.NextCursor, nil, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 mixing audit offset and cursor, got %d", status)
	}
}

func TestListAuditEventsSupportsResourceAndActorFilters(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	now := time.Now().UTC()
	for _, event := range []struct {
		eventTime    time.Time
		actorID      string
		action       string
		resourceType string
		resourceID   string
		tenantID     string
	}{
		{now.Add(-3 * time.Minute), "admin-user", "JOB_UPDATE", "job", "job-1", "tenant-a"},
		{now.Add(-2 * time.Minute), "service-user", "JOB_UPDATE", "job", "job-2", "tenant-a"},
		{now.Add(-1 * time.Minute), "admin-user", "WORKER_DRAIN", "worker", "worker-1", "tenant-a"},
	} {
		if _, err := jobStore.DB().ExecContext(ctx, `
			INSERT INTO audit_events (event_time, actor_role, actor_id, action, resource_type, resource_id, tenant_id, payload)
			VALUES ($1, 'admin', $2, $3, $4, $5, $6, '{}'::jsonb)
		`, event.eventTime, event.actorID, event.action, event.resourceType, event.resourceID, event.tenantID); err != nil {
			t.Fatalf("insert audit event %s: %v", event.resourceID, err)
		}
	}

	assertAuditIDs := func(url string, expected ...string) {
		t.Helper()
		resp := ListAuditEventsResponse{}
		status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, url, nil, &resp)
		if status != http.StatusOK {
			t.Fatalf("expected 200 listing audit events for %s, got %d", url, status)
		}
		if len(resp.Events) != len(expected) {
			t.Fatalf("expected %d events for %s, got %+v", len(expected), url, resp.Events)
		}
		for i, resourceID := range expected {
			if resp.Events[i].ResourceID != resourceID {
				t.Fatalf("unexpected audit events for %s: got %+v expected resource_ids=%+v", url, resp.Events, expected)
			}
		}
	}

	assertAuditIDs(httpServer.URL+"/v1/audit/events?resource_id=job-2", "job-2")
	assertAuditIDs(httpServer.URL+"/v1/audit/events?actor_id=admin-user", "worker-1", "job-1")
	assertAuditIDs(httpServer.URL+"/v1/audit/events?actor_id=admin-user&resource_id=worker-1", "worker-1")
}

func TestLookupJobByDedupeKey(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":       "lookup-job",
		"tenant_id":  "tenant-api",
		"queue":      "api-lookup-jobs",
		"kind":       "http",
		"dedupe_key": "lookup-dedupe-key",
		"payload":    map[string]any{"url": "https://example.internal/task"},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 creating job, got %d", status)
	}

	var lookupResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs/lookup?tenant_id=tenant-api&dedupe_key=lookup-dedupe-key", nil, &lookupResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 looking up job by dedupe key, got %d", status)
	}
	if lookupResp.Job.ID != createResp.JobID || lookupResp.Job.Name != "lookup-job" {
		t.Fatalf("unexpected lookup job response: %+v", lookupResp.Job)
	}
}

func TestDisableAndEnableJobEndpoints(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-disable-" + time.Now().UTC().Format("150405.000000000")
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-disable-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload": map[string]any{
			"url": "https://example.internal/task",
		},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}

	var disableResp JobLifecycleResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/disable", nil, &disableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 disable response, got %d", status)
	}
	if disableResp.Status != "disabled" {
		t.Fatalf("expected disabled lifecycle response, got %+v", disableResp)
	}

	var jobResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs/"+createResp.JobID, nil, &jobResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get job, got %d", status)
	}
	if jobResp.Job.DisabledAt == nil {
		t.Fatalf("expected job to be disabled, got %+v", jobResp.Job)
	}

	var disabledJobs struct {
		Jobs []store.Job `json:"jobs"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&disabled=true", nil, &disabledJobs)
	if status != http.StatusOK || len(disabledJobs.Jobs) != 1 || disabledJobs.Jobs[0].ID != createResp.JobID {
		t.Fatalf("expected disabled job in filtered list, got status=%d jobs=%+v", status, disabledJobs.Jobs)
	}

	var enableResp JobLifecycleResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/enable", nil, &enableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 enable response, got %d", status)
	}
	if enableResp.Status != "active" {
		t.Fatalf("expected active lifecycle response after enable, got %+v", enableResp)
	}

	var enabledJobResp struct {
		Job store.Job `json:"job"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs/"+createResp.JobID, nil, &enabledJobResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get job after enable, got %d", status)
	}
	if enabledJobResp.Job.DisabledAt != nil {
		t.Fatalf("expected job to be enabled, got %+v", enabledJobResp.Job)
	}
}

func TestTenantQuotaEndpoints(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var upsertResp TenantQuotaResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-api/quota", map[string]any{
		"max_inflight":     2,
		"max_pending_runs": 4,
		"max_active_jobs":  6,
	}, &upsertResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 upsert quota, got %d", status)
	}
	if upsertResp.TenantID != "tenant-api" || upsertResp.MaxInflight != 2 || upsertResp.MaxPendingRuns != 4 || upsertResp.MaxActiveJobs != 6 {
		t.Fatalf("unexpected upsert response: %+v", upsertResp)
	}

	var listResp struct {
		TenantQuotas []TenantQuotaResponse `json:"tenant_quotas"`
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/tenants/quotas", nil, &listResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 list quotas, got %d", status)
	}
	found := false
	for _, quota := range listResp.TenantQuotas {
		if quota.TenantID == "tenant-api" && quota.MaxInflight == 2 && quota.MaxPendingRuns == 4 && quota.MaxActiveJobs == 6 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected tenant-api quota in response, got %+v", listResp.TenantQuotas)
	}

	var auditResp ListAuditEventsResponse
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?action=TENANT_QUOTA_UPSERT&resource_type=tenant_quota", nil, &auditResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 list audit events, got %d", status)
	}
	if len(auditResp.Events) == 0 {
		t.Fatalf("expected audit events after quota upsert")
	}
	found = false
	for _, event := range auditResp.Events {
		if event.Action == "TENANT_QUOTA_UPSERT" && event.ResourceID == "tenant-api" {
			found = true
			if event.ActorRole != "admin" {
				t.Fatalf("expected admin actor role, got %+v", event)
			}
			if event.Payload["max_inflight"] != float64(2) {
				t.Fatalf("expected max_inflight payload, got %+v", event.Payload)
			}
			if event.Payload["max_pending_runs"] != float64(4) || event.Payload["max_active_jobs"] != float64(6) {
				t.Fatalf("expected expanded quota payload, got %+v", event.Payload)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected quota audit event in response, got %+v", auditResp.Events)
	}
}

func TestUnauthorizedRequestsAreRejected(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/jobs", nil, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got %d", status)
	}
}

func TestTenantCannotCrossTenantBoundaries(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=other-tenant", nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant list, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-api/quota", map[string]any{"max_inflight": 2}, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for tenant quota mutation, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodGet, httpServer.URL+"/v1/jobs", nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for worker listing jobs, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/audit/events", nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for tenant audit listing, got %d", status)
	}
}

func TestAdminCancelJobWritesAuditEvent(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-admin-cancel-" + time.Now().UTC().Format("150405.000000000")
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-admin-cancel-test",
		"tenant_id": "tenant-api",
		"queue":     queue,
		"kind":      "http",
		"payload": map[string]any{
			"url": "https://example.internal/task",
		},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", status)
	}

	var cancelResp CancelJobResponse
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/cancel", nil, &cancelResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 cancel response, got %d", status)
	}

	var auditResp ListAuditEventsResponse
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/audit/events?action=JOB_CANCEL&tenant_id=tenant-api", nil, &auditResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 list audit events, got %d", status)
	}
	found := false
	for _, event := range auditResp.Events {
		if event.Action == "JOB_CANCEL" && event.ResourceID == createResp.JobID {
			found = true
			if event.ActorRole != "admin" {
				t.Fatalf("expected admin actor role, got %+v", event)
			}
			if event.TenantID == nil || *event.TenantID != "tenant-api" {
				t.Fatalf("expected tenant id on audit event, got %+v", event)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected job cancel audit event in response, got %+v", auditResp.Events)
	}
}

func TestCreateJobRejectsUnknownScheduleType(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-invalid-schedule",
		"tenant_id": "tenant-api",
		"queue":     "default",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/task"},
		"schedule": map[string]any{
			"type": "delayed",
		},
	}, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown schedule type, got %d", status)
	}
}

func TestCreateJobRejectsUnknownFields(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":       "api-unknown-field",
		"tenant_id":  "tenant-api",
		"queue":      "default",
		"kind":       "http",
		"payload":    map[string]any{"url": "https://example.internal/task"},
		"queue_typo": "unexpected",
	}, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown field, got %d", status)
	}
}
func TestCreateJobReturnsConflictForDuplicateDedupeKey(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	payload := map[string]any{
		"name":       "api-dedupe",
		"tenant_id":  "tenant-api",
		"queue":      "default",
		"kind":       "http",
		"dedupe_key": "dedupe-123",
		"payload":    map[string]any{"url": "https://example.internal/task"},
	}

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", payload, &map[string]any{})
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 creating deduped job, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", payload, &map[string]any{})
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate dedupe key, got %d", status)
	}
}

func TestCreateJobReplaysIdempotencyKey(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	payload := map[string]any{
		"name":      "api-idempotent",
		"tenant_id": "tenant-api",
		"queue":     "default",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/task"},
	}
	headers := map[string]string{"Idempotency-Key": "idem-create-1"}
	var first CreateJobResponse
	status := doJSONRequestWithHeaders(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", headers, payload, &first)
	if status != http.StatusAccepted {
		t.Fatalf("expected first 202 create, got %d", status)
	}
	var second CreateJobResponse
	status = doJSONRequestWithHeaders(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", headers, payload, &second)
	if status != http.StatusAccepted {
		t.Fatalf("expected replayed 202 create, got %d", status)
	}
	if first.JobID != second.JobID {
		t.Fatalf("expected same job id on replay, got %+v and %+v", first, second)
	}
	if (first.RunID == nil) != (second.RunID == nil) || (first.RunID != nil && *first.RunID != *second.RunID) {
		t.Fatalf("expected same run id on replay, got %+v and %+v", first, second)
	}
	jobs, err := jobStore.ListJobs(context.Background(), store.JobFilter{TenantID: "tenant-api"})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one persisted job, got %+v", jobs)
	}
}

func TestCreateJobRejectsIdempotencyKeyReuseWithDifferentRequest(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	headers := map[string]string{"Idempotency-Key": "idem-create-2"}
	payload := map[string]any{
		"name":      "api-idempotent-a",
		"tenant_id": "tenant-api",
		"queue":     "default",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/task"},
	}
	status := doJSONRequestWithHeaders(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", headers, payload, &CreateJobResponse{})
	if status != http.StatusAccepted {
		t.Fatalf("expected first 202 create, got %d", status)
	}
	payload["name"] = "api-idempotent-b"
	var errResp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", headers, payload, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for idempotency conflict, got %d", status)
	}
	if errResp.Error.Code != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("expected IDEMPOTENCY_KEY_REUSED, got %+v", errResp)
	}
}

func TestCreateJobReturnsTooManyRequestsWhenTenantPendingQuotaExceeded(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	if _, err := jobStore.UpsertTenantQuota(ctx, "tenant-api", 0, 1, 0, nil); err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	payload := map[string]any{
		"name":      "quota-first",
		"tenant_id": "tenant-api",
		"queue":     "default",
		"kind":      "http",
		"payload":   map[string]any{"url": "https://example.internal/task"},
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", payload, &map[string]any{})
	if status != http.StatusAccepted {
		t.Fatalf("expected first create accepted, got %d", status)
	}

	payload["name"] = "quota-second"
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", payload, &map[string]any{})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on pending quota exceed, got %d", status)
	}
}

func TestTenantQuotaEndpointAllowsResetToUnlimited(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var resp TenantQuotaResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-api/quota", map[string]any{
		"max_inflight":     0,
		"max_pending_runs": 0,
		"max_active_jobs":  0,
	}, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 resetting quota to unlimited, got %d", status)
	}
	if resp.MaxInflight != 0 || resp.MaxPendingRuns != 0 || resp.MaxActiveJobs != 0 {
		t.Fatalf("expected zeroed unlimited quota response, got %+v", resp)
	}
}

func TestCreateJobAndListJobsRoundTripConcurrencyKey(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":            "api-concurrency-job",
		"tenant_id":       "tenant-api",
		"queue":           "default",
		"kind":            "http",
		"concurrency_key": "customer-42",
		"payload":         map[string]any{"url": "https://example.internal/task"},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 create response, got %d", status)
	}

	var jobsResp struct {
		Jobs []store.Job `json:"jobs"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api", nil, &jobsResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing jobs, got %d", status)
	}
	found := false
	for _, job := range jobsResp.Jobs {
		if job.ID == createResp.JobID {
			found = true
			if job.ConcurrencyKey == nil || *job.ConcurrencyKey != "customer-42" {
				t.Fatalf("expected concurrency key to round-trip, got %+v", job)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected created job in list, got %+v", jobsResp.Jobs)
	}
}

func TestPauseResumeAndTriggerJobEndpoints(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)
	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-pause-job",
		"tenant_id": "tenant-api",
		"queue":     "default",
		"kind":      "http",
		"schedule": map[string]any{
			"type": "cron",
			"cron": "*/5 * * * *",
		},
		"payload": map[string]any{"url": "https://example.internal/task"},
	}, &createResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 create response, got %d", status)
	}

	var lifecycleResp JobLifecycleResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/pause", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "paused" {
		t.Fatalf("expected paused lifecycle response, got status=%d body=%+v", status, lifecycleResp)
	}

	var jobsResp struct {
		Jobs []store.Job `json:"jobs"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/jobs?tenant_id=tenant-api&paused=true", nil, &jobsResp)
	if status != http.StatusOK || len(jobsResp.Jobs) != 1 || jobsResp.Jobs[0].PausedAt == nil {
		t.Fatalf("expected paused job in list, got status=%d jobs=%+v", status, jobsResp.Jobs)
	}

	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/resume", nil, &lifecycleResp)
	if status != http.StatusOK || lifecycleResp.Status != "active" {
		t.Fatalf("expected active lifecycle response, got status=%d body=%+v", status, lifecycleResp)
	}

	var triggerResp TriggerJobResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/trigger", nil, &triggerResp)
	if status != http.StatusAccepted || triggerResp.RunID == "" {
		t.Fatalf("expected trigger response with run id, got status=%d body=%+v", status, triggerResp)
	}
}

func TestAuthMeEndpointReturnsPrincipalScope(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var tenantResp AuthMeResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &tenantResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 for tenant auth me, got %d", status)
	}
	if tenantResp.Role != "tenant" || tenantResp.TenantID == nil || *tenantResp.TenantID != "tenant-api" {
		t.Fatalf("unexpected tenant auth me response: %+v", tenantResp)
	}

	var workerResp AuthMeResponse
	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &workerResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 for worker auth me, got %d", status)
	}
	if workerResp.Role != "worker" || workerResp.WorkerName == nil || *workerResp.WorkerName != "worker-api" {
		t.Fatalf("unexpected worker auth me response: %+v", workerResp)
	}
}

func TestNewServerReturnsConfigError(t *testing.T) {
	server, err := NewServer(config.APIConfig{
		AuthTokens: "bad-entry",
	}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err == nil {
		t.Fatalf("expected constructor error, got server=%v", server)
	}
}

func TestEmptyAuthConfigDoesNotGrantImplicitAdminAccess(t *testing.T) {
	server, err := NewServer(config.APIConfig{}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err == nil {
		t.Fatalf("expected config error, got server=%v", server)
	}
}

func TestExplicitInsecureDevModeAllowsAuthBypass(t *testing.T) {
	server, err := NewServer(config.APIConfig{InsecureDevMode: true, Address: "127.0.0.1:8080"}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var resp AuthMeResponse
	status := doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 in explicit insecure dev mode, got %d", status)
	}
	if resp.Role != "admin" {
		t.Fatalf("expected admin role in insecure dev mode, got %+v", resp)
	}
}

func TestRequestsAreRateLimitedBeforeAuthentication(t *testing.T) {
	server, err := NewServer(config.APIConfig{
		Address:                   "127.0.0.1:8080",
		AuthTokens:                adminToken + ":admin",
		TokenRateLimitPerSecond:   100,
		TokenRateLimitBurst:       100,
		TenantRateLimitPerSecond:  100,
		TenantRateLimitBurst:      100,
		PreAuthRateLimitPerSecond: 1,
		PreAuthRateLimitBurst:     1,
	}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected first unauthenticated request to be unauthorized, got %d", status)
	}
	status = doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected second unauthenticated request to be rate limited, got %d", status)
	}
}

func TestAuthenticationFailuresIncrementMetrics(t *testing.T) {
	registry := observability.NewRegistry()
	server, err := NewServer(config.APIConfig{
		Address:                   "127.0.0.1:8080",
		AuthTokens:                adminToken + ":admin",
		TokenRateLimitPerSecond:   100,
		TokenRateLimitBurst:       100,
		TenantRateLimitPerSecond:  100,
		TenantRateLimitBurst:      100,
		PreAuthRateLimitPerSecond: 100,
		PreAuthRateLimitBurst:     100,
	}, log.New(io.Discard, "", 0), nil, registry)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected missing token request to be unauthorized, got %d", status)
	}
	status = doJSONRequest(t, httpServer.Client(), "not-a-real-token", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected invalid token request to be unauthorized, got %d", status)
	}

	metrics := registry.Render()
	for _, want := range []string{
		`runq_api_auth_failures_total{reason="missing_bearer_token"} 1`,
		`runq_api_auth_failures_total{reason="invalid_bearer_token"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("expected metrics to contain %q, got:\n%s", want, metrics)
		}
	}
}

func TestRequestsAreRateLimitedPerToken(t *testing.T) {
	server, err := NewServer(config.APIConfig{
		AuthTokens:               adminToken + ":admin",
		TokenRateLimitPerSecond:  1,
		TokenRateLimitBurst:      1,
		TenantRateLimitPerSecond: 100,
		TenantRateLimitBurst:     100,
	}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &AuthMeResponse{})
	if status != http.StatusOK {
		t.Fatalf("expected first request to pass, got %d", status)
	}
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate limited, got %d", status)
	}
}

func TestRequestsAreRateLimitedPerTenantAcrossTokens(t *testing.T) {
	server, err := NewServer(config.APIConfig{
		AuthTokens:               "tenant-a:tenant:tenant-api,tenant-b:tenant:tenant-api",
		TokenRateLimitPerSecond:  100,
		TokenRateLimitBurst:      100,
		TenantRateLimitPerSecond: 1,
		TenantRateLimitBurst:     1,
	}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), "tenant-a", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &AuthMeResponse{})
	if status != http.StatusOK {
		t.Fatalf("expected first tenant request to pass, got %d", status)
	}
	status = doJSONRequest(t, httpServer.Client(), "tenant-b", http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected second tenant request to be rate limited, got %d", status)
	}
}

func TestRecoveredPanicReturnsStructuredInternalError(t *testing.T) {
	server, err := NewServer(config.APIConfig{AuthTokens: adminToken + ":admin"}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.handle("GET /panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var errResp map[string]any
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/panic", nil, &errResp)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500 from recovered panic, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodGet, httpServer.URL+"/v1/auth/me", nil, &map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("expected server to keep serving after recovered panic, got %d", status)
	}
}

func TestMetricsAreNotExposedOnPublicAPIMux(t *testing.T) {
	server, err := NewServer(config.APIConfig{AuthTokens: adminToken + ":admin"}, log.New(io.Discard, "", 0), nil, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	resp, err := httpServer.Client().Get(httpServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected public metrics path to be unavailable, got %d", resp.StatusCode)
	}
}

func TestHeartbeatRejectsDuplicateRunIDs(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var workerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 2,
	}, &workerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+workerResp.WorkerID+"/heartbeat", map[string]string{workerSessionHeader: workerResp.WorkerSessionToken}, map[string]any{
		"running": []map[string]any{
			{"run_id": "run-1", "lease_token": 1},
			{"run_id": "run-1", "lease_token": 2},
		},
	}, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for duplicate heartbeat run ids, got %d", status)
	}
}

func TestHeartbeatRejectsOversizedRunningSet(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var workerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 200,
	}, &workerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	running := make([]map[string]any, 0, 101)
	for i := 0; i < 101; i++ {
		running = append(running, map[string]any{"run_id": fmt.Sprintf("run-%03d", i), "lease_token": i + 1})
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+workerResp.WorkerID+"/heartbeat", map[string]string{workerSessionHeader: workerResp.WorkerSessionToken}, map[string]any{
		"running": running,
	}, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized heartbeat running set, got %d", status)
	}
}

func TestRegisterWorkerReturnsSessionTokenAndRotatesIt(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var first RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &first)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}
	if first.WorkerSessionToken == "" {
		t.Fatal("expected worker session token")
	}

	var second RegisterWorkerResponse
	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &second)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 re-registering worker, got %d", status)
	}
	if second.WorkerSessionToken == "" || second.WorkerSessionToken == first.WorkerSessionToken {
		t.Fatalf("expected rotated worker session token, got first=%q second=%q", first.WorkerSessionToken, second.WorkerSessionToken)
	}
}

func TestWorkerProtocolRequiresValidSessionToken(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var workerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &workerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+workerResp.WorkerID+"/poll", map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 without session token, got %d", status)
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+workerResp.WorkerID+"/poll", map[string]string{workerSessionHeader: "bad-token"}, map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 with invalid session token, got %d", status)
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+workerResp.WorkerID+"/poll", map[string]string{workerSessionHeader: workerResp.WorkerSessionToken}, map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("expected 200 with valid session token, got %d", status)
	}
}

func TestAdminWorkerProtocolReturnsNotFoundForUnknownWorker(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequestWithHeaders(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/worker-missing/poll", map[string]string{workerSessionHeader: "unused"}, map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusNotFound {
		t.Fatalf("expected 404 polling unknown worker, got %d", status)
	}

	status = doJSONRequestWithHeaders(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/worker-missing/heartbeat", map[string]string{workerSessionHeader: "unused"}, map[string]any{"running": []map[string]any{}}, &map[string]any{})
	if status != http.StatusNotFound {
		t.Fatalf("expected 404 heartbeating unknown worker, got %d", status)
	}
}

func TestOldWorkerSessionIsInvalidAfterReregistration(t *testing.T) {
	jobStore := openTestStore(t)
	resetTablesForAPI(t, jobStore)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var first RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &first)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}
	var second RegisterWorkerResponse
	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &second)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 re-registering worker, got %d", status)
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+first.WorkerID+"/poll", map[string]string{workerSessionHeader: first.WorkerSessionToken}, map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 using rotated-out session token, got %d", status)
	}
	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+second.WorkerID+"/poll", map[string]string{workerSessionHeader: second.WorkerSessionToken}, map[string]any{"available_slots": 1}, &map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("expected 200 using current session token, got %d", status)
	}
}

func TestWorkerTokenCannotRegisterDifferentWorkerName(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "someone-else",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched worker name, got %d", status)
	}
}

func TestRegisterWorkerRejectsUnknownFields(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
		"extra":           true,
	}, &map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown worker register field, got %d", status)
	}
}

func TestWorkerTokenCannotOperateOnAnotherWorkerID(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var workerResp RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &workerResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker principal, got %d", status)
	}

	var otherResp RegisterWorkerResponse
	status = doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-other",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &otherResp)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering other worker, got %d", status)
	}

	status = doJSONRequestWithHeaders(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+otherResp.WorkerID+"/poll", map[string]string{workerSessionHeader: workerResp.WorkerSessionToken}, map[string]any{
		"available_slots": 1,
	}, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 when worker token uses another worker id, got %d", status)
	}
}

func TestTenantCannotRequeueAnotherTenantsRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "other-tenant-failed",
		TenantID:     "other-tenant",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-requeue-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.FailRun(ctx, store.FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        *result.RunID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/requeue", nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant requeue, got %d", status)
	}
}

func TestWorkerCannotAccessRunConsoleEndpoints(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "worker-console-forbid",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodGet, httpServer.URL+"/v1/runs/"+*result.RunID, nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for worker run detail access, got %d", status)
	}

	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/requeue", nil, &map[string]any{})
	if status != http.StatusForbidden {
		t.Fatalf("expected 403 for worker requeue access, got %d", status)
	}
}

func TestCancelRunEndpointCancelsPendingRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-cancel-run",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var resp struct {
		FromRun string `json:"from_run"`
		Status  string `json:"status"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/cancel", nil, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 cancel run response, got %d", status)
	}
	if resp.FromRun != *result.RunID || resp.Status != "canceled" {
		t.Fatalf("unexpected cancel run response: %+v", resp)
	}

	var runResp GetRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs/"+*result.RunID, nil, &runResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get canceled run, got %d", status)
	}
	if runResp.Run.Status != "CANCELED" {
		t.Fatalf("expected canceled run, got %+v", runResp.Run)
	}
}

func TestCancelRunEndpointRejectsTerminalRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-cancel-run-terminal",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-cancel-run-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING', worker_id = $2, lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds',
		    started_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`, *result.RunID, worker.WorkerID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}
	if err := jobStore.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID: worker.WorkerID, RunID: *result.RunID, LeaseToken: 1,
		Result: map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/cancel", nil, &map[string]any{})
	if status != http.StatusConflict {
		t.Fatalf("expected 409 canceling terminal run, got %d", status)
	}
}

func TestRequeueRunEndpointCreatesFreshPendingRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-requeue",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-requeue-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.FailRun(ctx, store.FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        *result.RunID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var requeueResp RequeueRunResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/requeue", nil, &requeueResp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 requeue response, got %d", status)
	}
	if requeueResp.RunID == "" || requeueResp.RunID == *result.RunID {
		t.Fatalf("expected fresh run id, got %+v", requeueResp)
	}

	var runResp GetRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs/"+requeueResp.RunID, nil, &runResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get requeued run, got %d", status)
	}
	if runResp.Run.Status != "PENDING" || runResp.Run.JobName != "api-requeue" || runResp.Run.Queue != "default" || runResp.Run.Kind != "http" {
		t.Fatalf("expected enriched pending requeued run, got %+v", runResp.Run)
	}
}

func TestRequeueRunEndpointRejectsSucceededRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-requeue-succeeded",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID); err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := jobStore.MaterializeDueRuns(ctx, 1); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	runs, err := jobStore.ListRuns(ctx, store.RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-success-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID:   worker.WorkerID,
		RunID:      runs[0].ID,
		LeaseToken: 1,
		Result:     map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+runs[0].ID+"/requeue", nil, &map[string]any{})
	if status != http.StatusConflict {
		t.Fatalf("expected 409 requeueing succeeded run, got %d", status)
	}
}

func TestRequeueRunEndpointPrefersConflictOverQuotaExceededForSucceededRun(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-requeue-succeeded-quota",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID); err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := jobStore.MaterializeDueRuns(ctx, 1); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	runs, err := jobStore.ListRuns(ctx, store.RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-success-quota-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID:   worker.WorkerID,
		RunID:      runs[0].ID,
		LeaseToken: 1,
		Result:     map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	if _, err := jobStore.UpsertTenantQuota(ctx, "tenant-api", 0, 0, 0, nil); err != nil {
		t.Fatalf("reset tenant quota: %v", err)
	}
	if _, err := jobStore.UpsertTenantQuota(ctx, "tenant-api", 0, 1, 0, nil); err != nil {
		t.Fatalf("upsert tenant quota: %v", err)
	}
	if _, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "quota-blocker",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	}); err != nil {
		t.Fatalf("create quota blocker: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+runs[0].ID+"/requeue", nil, &map[string]any{})
	if status != http.StatusConflict {
		t.Fatalf("expected 409 for succeeded run even when quota is full, got %d", status)
	}
}

func TestDeadLetterRunCanBeListedAndRedriven(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-dead-letter",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-dead-letter-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.FailRun(ctx, store.FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        *result.RunID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var runsResp struct {
		Runs []store.Run `json:"runs"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs?tenant_id=tenant-api&dead_lettered=true", nil, &runsResp)
	if status != http.StatusOK || len(runsResp.Runs) != 1 || runsResp.Runs[0].DeadLetteredAt == nil {
		t.Fatalf("expected dead-lettered run in list, got status=%d runs=%+v", status, runsResp.Runs)
	}

	var redriveResp RequeueRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/"+*result.RunID+"/redrive", nil, &redriveResp)
	if status != http.StatusAccepted || redriveResp.RunID == "" {
		t.Fatalf("expected redrive response, got status=%d body=%+v", status, redriveResp)
	}
}

func TestBulkRequeueRunsByID(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	failedRunIDs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
			Name:         "api-bulk-requeue-" + string(rune('a'+i)),
			TenantID:     "tenant-api",
			Queue:        "default",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/task"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create job: %v", err)
		}
		worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
			Name:           "api-bulk-requeue-worker-" + string(rune('a'+i)),
			Queues:         []string{"default"},
			Capabilities:   map[string]any{"http": true},
			MaxConcurrency: 1,
			Metadata:       map[string]any{"role": "api"},
		})
		if err != nil {
			t.Fatalf("register worker: %v", err)
		}
		if _, err := jobStore.DB().ExecContext(ctx, `
			UPDATE runs
			SET status = 'RUNNING', worker_id = $2, lease_token = 1,
			    lease_expires_at = NOW() + INTERVAL '30 seconds', started_at = NOW(), updated_at = NOW()
			WHERE id = $1
		`, *result.RunID, worker.WorkerID); err != nil {
			t.Fatalf("mark run running: %v", err)
		}
		if err := jobStore.FailRun(ctx, store.FailRunInput{
			WorkerID: worker.WorkerID, RunID: *result.RunID, LeaseToken: 1,
			ErrorCode: "HTTP_500", ErrorMessage: "terminal failure", Retryable: false,
		}); err != nil {
			t.Fatalf("fail run: %v", err)
		}
		failedRunIDs = append(failedRunIDs, *result.RunID)
	}

	succeeded, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-requeue-succeeded",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create succeeded job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-bulk-requeue-worker-success",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register success worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING', worker_id = $2, lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds', started_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`, *succeeded.RunID, worker.WorkerID); err != nil {
		t.Fatalf("mark succeeded run running: %v", err)
	}
	if err := jobStore.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID: worker.WorkerID, RunID: *succeeded.RunID, LeaseToken: 1,
		Result: map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var dryRunResp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			RunID        string `json:"run_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/requeue", map[string]any{
		"run_ids": append(failedRunIDs, *succeeded.RunID),
		"dry_run": true,
	}, &dryRunResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk requeue dry-run response, got %d", status)
	}
	for _, item := range dryRunResp.Results {
		if item.RunID != "" {
			t.Fatalf("expected no run ids during dry-run, got %+v", item)
		}
		if item.Status != "would_accept" && item.Status != "would_skip" {
			t.Fatalf("expected would_accept/would_skip statuses during dry-run, got %+v", item)
		}
	}
	countRuns := func() int {
		rows, err := jobStore.ListRuns(ctx, store.RunFilter{TenantID: "tenant-api", Limit: 100})
		if err != nil {
			t.Fatalf("list runs after dry-run: %v", err)
		}
		return len(rows)
	}
	beforeCount := countRuns()
	afterDryRun := countRuns()
	if afterDryRun != beforeCount {
		t.Fatalf("expected dry-run to leave runs unchanged, before=%d after=%d", beforeCount, afterDryRun)
	}

	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			RunID        string `json:"run_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/requeue", map[string]any{
		"run_ids": append(failedRunIDs, *succeeded.RunID),
	}, &resp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 bulk requeue response, got %d", status)
	}
	if resp.Count != 3 || len(resp.Results) != 3 {
		t.Fatalf("expected three bulk requeue results, got %+v", resp)
	}
	accepted := 0
	skipped := 0
	for _, item := range resp.Results {
		switch item.Status {
		case "accepted":
			accepted++
			if item.RunID == "" || item.RunID == item.FromRun {
				t.Fatalf("unexpected accepted bulk requeue item: %+v", item)
			}
		case "skipped":
			skipped++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk requeue item: %+v", item)
		}
	}
	if accepted != 2 || skipped != 1 {
		t.Fatalf("expected 2 accepted and 1 skipped result, got %+v", resp.Results)
	}
}

func TestBulkRequeueDeduplicatesRunIDs(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-requeue-dedupe",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "once",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-bulk-requeue-dedupe-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
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
	if err := jobStore.FailRun(ctx, store.FailRunInput{
		WorkerID:     worker.WorkerID,
		RunID:        *result.RunID,
		LeaseToken:   1,
		ErrorCode:    "HTTP_500",
		ErrorMessage: "terminal failure",
		Retryable:    false,
	}); err != nil {
		t.Fatalf("fail run: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun string `json:"from_run"`
			RunID   string `json:"run_id"`
			Status  string `json:"status"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/requeue", map[string]any{
		"run_ids": []string{*result.RunID, *result.RunID},
	}, &resp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 bulk requeue response, got %d", status)
	}
	if resp.Count != 1 || len(resp.Results) != 1 {
		t.Fatalf("expected duplicate run ids to collapse to one result, got %+v", resp)
	}
	if resp.Results[0].Status != "accepted" || resp.Results[0].RunID == "" {
		t.Fatalf("expected one accepted requeue result, got %+v", resp.Results[0])
	}
}

func TestBulkRedriveRunsByFilter(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	for i := 0; i < 2; i++ {
		result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
			Name:         "api-bulk-redrive-" + string(rune('a'+i)),
			TenantID:     "tenant-api",
			Queue:        "default",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/task"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create job: %v", err)
		}
		worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
			Name:           "api-bulk-redrive-worker-" + string(rune('a'+i)),
			Queues:         []string{"default"},
			Capabilities:   map[string]any{"http": true},
			MaxConcurrency: 1,
			Metadata:       map[string]any{"role": "api"},
		})
		if err != nil {
			t.Fatalf("register worker: %v", err)
		}
		if _, err := jobStore.DB().ExecContext(ctx, `
			UPDATE runs
			SET status = 'RUNNING', worker_id = $2, lease_token = 1,
			    lease_expires_at = NOW() + INTERVAL '30 seconds', started_at = NOW(), updated_at = NOW()
			WHERE id = $1
		`, *result.RunID, worker.WorkerID); err != nil {
			t.Fatalf("mark run running: %v", err)
		}
		if err := jobStore.FailRun(ctx, store.FailRunInput{
			WorkerID: worker.WorkerID, RunID: *result.RunID, LeaseToken: 1,
			ErrorCode: "HTTP_500", ErrorMessage: "terminal failure", Retryable: false,
		}); err != nil {
			t.Fatalf("fail run: %v", err)
		}
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var dryRunResp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			RunID        string `json:"run_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/redrive", map[string]any{
		"tenant_id":     "tenant-api",
		"dead_lettered": true,
		"status":        []string{"FAILED", "PENDING"},
		"dry_run":       true,
	}, &dryRunResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk redrive dry-run response, got %d", status)
	}
	for _, item := range dryRunResp.Results {
		if item.RunID != "" {
			t.Fatalf("expected no run ids during dry-run, got %+v", item)
		}
		if item.Status != "would_accept" && item.Status != "would_skip" {
			t.Fatalf("expected would_accept/would_skip statuses during dry-run, got %+v", item)
		}
	}
	deadLettered := true
	rowsAfterDryRun, err := jobStore.ListRuns(ctx, store.RunFilter{TenantID: "tenant-api", DeadLettered: &deadLettered, Limit: 100})
	if err != nil {
		t.Fatalf("list runs after redrive dry-run: %v", err)
	}
	if len(rowsAfterDryRun) != 2 {
		t.Fatalf("expected dry-run to leave dead-lettered runs unchanged, got %+v", rowsAfterDryRun)
	}

	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			RunID        string `json:"run_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/redrive", map[string]any{
		"tenant_id":     "tenant-api",
		"dead_lettered": true,
		"status":        []string{"FAILED", "PENDING"},
	}, &resp)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 bulk redrive response, got %d", status)
	}
	if resp.Count != 2 || len(resp.Results) != 2 {
		t.Fatalf("expected two bulk redrive results, got %+v", resp)
	}
	accepted := 0
	skipped := 0
	for _, item := range resp.Results {
		switch item.Status {
		case "accepted":
			accepted++
		case "skipped":
			skipped++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk redrive item: %+v", item)
		}
	}
	if accepted != 2 || skipped != 0 {
		t.Fatalf("expected two accepted redrive results, got %+v", resp.Results)
	}
}

func TestBulkCancelRunsByFilter(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-cancel",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create cron job: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `UPDATE job_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE job_id = $1`, result.JobID); err != nil {
		t.Fatalf("set due schedule: %v", err)
	}
	if _, err := jobStore.MaterializeDueRuns(ctx, 2); err != nil {
		t.Fatalf("materialize due runs: %v", err)
	}
	runs, err := jobStore.ListRuns(ctx, store.RunFilter{JobID: result.JobID})
	if err != nil {
		t.Fatalf("list runs before cancel: %v", err)
	}
	worker, err := jobStore.RegisterWorker(ctx, store.RegisterWorkerInput{
		Name:           "api-bulk-cancel-worker",
		Queues:         []string{"default"},
		Capabilities:   map[string]any{"http": true},
		MaxConcurrency: 1,
		Metadata:       map[string]any{"role": "api"},
	})
	if err != nil {
		t.Fatalf("register cancel worker: %v", err)
	}
	if _, err := jobStore.DB().ExecContext(ctx, `
		UPDATE runs
		SET status = 'RUNNING', worker_id = $2, lease_token = 1,
		    lease_expires_at = NOW() + INTERVAL '30 seconds', started_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`, runs[0].ID, worker.WorkerID); err != nil {
		t.Fatalf("mark cancel run running: %v", err)
	}
	if err := jobStore.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID: worker.WorkerID, RunID: runs[0].ID, LeaseToken: 1,
		Result: map[string]any{"status_code": 200},
	}); err != nil {
		t.Fatalf("complete one run before bulk cancel: %v", err)
	}
	if _, err := jobStore.TriggerJob(ctx, result.JobID, nil); err != nil {
		t.Fatalf("trigger fresh pending run before bulk cancel: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var dryRunResp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/cancel", map[string]any{
		"job_id":  result.JobID,
		"dry_run": true,
	}, &dryRunResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk cancel dry-run response, got %d", status)
	}
	for _, item := range dryRunResp.Results {
		if item.Status != "would_cancel" && item.Status != "would_skip" {
			t.Fatalf("expected would_cancel/would_skip statuses during dry-run, got %+v", item)
		}
	}
	runsAfterDryRun, err := jobStore.ListRuns(ctx, store.RunFilter{JobID: result.JobID, Limit: 100})
	if err != nil {
		t.Fatalf("list runs after cancel dry-run: %v", err)
	}
	pendingCount := 0
	for _, run := range runsAfterDryRun {
		if run.Status == "PENDING" {
			pendingCount++
		}
	}
	if pendingCount == 0 {
		t.Fatalf("expected dry-run to preserve pending work, got %+v", runsAfterDryRun)
	}

	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/cancel", map[string]any{
		"job_id": result.JobID,
	}, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk cancel response, got %d", status)
	}
	if resp.Count < 2 || len(resp.Results) < 2 {
		t.Fatalf("expected at least two bulk cancel results, got %+v", resp)
	}
	canceled := 0
	skipped := 0
	for _, item := range resp.Results {
		switch item.Status {
		case "canceled":
			canceled++
		case "skipped":
			skipped++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk cancel item: %+v", item)
		}
	}
	if canceled == 0 || skipped == 0 {
		t.Fatalf("expected mixed canceled/skipped results, got %+v", resp.Results)
	}
}

func TestBulkDisableAndEnableJobs(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	jobIDs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		result, err := jobStore.CreateJob(ctx, store.CreateJobInput{
			Name:         "api-bulk-disable-" + string(rune('a'+i)),
			TenantID:     "tenant-api",
			Queue:        "default",
			Kind:         "http",
			Payload:      map[string]any{"url": "https://example.internal/task"},
			ScheduleType: "once",
		})
		if err != nil {
			t.Fatalf("create job: %v", err)
		}
		jobIDs = append(jobIDs, result.JobID)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var disableResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/disable", map[string]any{
		"job_ids": jobIDs,
	}, &disableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk disable response, got %d", status)
	}
	if disableResp.Count != 2 || len(disableResp.Results) != 2 {
		t.Fatalf("expected two disabled jobs, got %+v", disableResp)
	}
	for _, item := range disableResp.Results {
		if item.Status != "disabled" {
			t.Fatalf("unexpected bulk disable item: %+v", item)
		}
	}

	var enableResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/enable", map[string]any{
		"job_ids": jobIDs,
	}, &enableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk enable response, got %d", status)
	}
	if enableResp.Count != 2 || len(enableResp.Results) != 2 {
		t.Fatalf("expected two enabled jobs, got %+v", enableResp)
	}
	for _, item := range enableResp.Results {
		if item.Status != "active" {
			t.Fatalf("unexpected bulk enable item: %+v", item)
		}
	}
}

func TestBulkPauseAndResumeJobsWithPartialFailures(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	activeJob, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-pause-active",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create active job: %v", err)
	}
	disabledJob, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-pause-disabled",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create disabled job: %v", err)
	}
	if _, err := jobStore.DisableJob(ctx, disabledJob.JobID, nil); err != nil {
		t.Fatalf("disable seed job: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var pauseResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/pause", map[string]any{
		"job_ids": []string{activeJob.JobID, disabledJob.JobID},
	}, &pauseResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk pause response, got %d", status)
	}
	if pauseResp.Count != 2 || len(pauseResp.Results) != 2 {
		t.Fatalf("expected two bulk pause results, got %+v", pauseResp)
	}
	paused := 0
	skipped := 0
	for _, item := range pauseResp.Results {
		switch item.Status {
		case "paused":
			paused++
		case "skipped":
			skipped++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk pause item: %+v", item)
		}
	}
	if paused != 1 || skipped != 1 {
		t.Fatalf("expected one paused and one skipped result, got %+v", pauseResp.Results)
	}

	var resumeResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/resume", map[string]any{
		"job_ids": []string{activeJob.JobID, disabledJob.JobID},
	}, &resumeResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk resume response, got %d", status)
	}
	active := 0
	skipped = 0
	for _, item := range resumeResp.Results {
		switch item.Status {
		case "active":
			active++
		case "skipped":
			skipped++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk resume item: %+v", item)
		}
	}
	if active != 1 || skipped != 1 {
		t.Fatalf("expected one resumed and one skipped result, got %+v", resumeResp.Results)
	}
}

func TestBulkJobLifecycleDryRunDoesNotMutateState(t *testing.T) {
	jobStore := openTestStore(t)
	ctx := context.Background()
	resetTablesForAPI(t, jobStore)

	activeJob, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-dry-run-active",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create active job: %v", err)
	}
	disabledJob, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-dry-run-disabled",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create disabled job: %v", err)
	}
	if _, err := jobStore.DisableJob(ctx, disabledJob.JobID, nil); err != nil {
		t.Fatalf("disable seed job: %v", err)
	}
	pausedJob, err := jobStore.CreateJob(ctx, store.CreateJobInput{
		Name:         "api-bulk-dry-run-paused",
		TenantID:     "tenant-api",
		Queue:        "default",
		Kind:         "http",
		Payload:      map[string]any{"url": "https://example.internal/task"},
		ScheduleType: "cron",
		CronExpr:     "*/5 * * * *",
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatalf("create paused job: %v", err)
	}
	if _, err := jobStore.PauseJob(ctx, pausedJob.JobID, nil); err != nil {
		t.Fatalf("pause seed job: %v", err)
	}

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	assertJobState := func(jobID string, disabled bool, paused bool) {
		t.Helper()
		job, err := jobStore.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get job %s: %v", jobID, err)
		}
		if (job.DisabledAt != nil) != disabled {
			t.Fatalf("job %s disabled mismatch: got %+v", jobID, job.DisabledAt)
		}
		if (job.PausedAt != nil) != paused {
			t.Fatalf("job %s paused mismatch: got %+v", jobID, job.PausedAt)
		}
	}

	var disableResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/disable", map[string]any{
		"job_ids": []string{activeJob.JobID, disabledJob.JobID},
		"dry_run": true,
	}, &disableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk disable dry-run response, got %d", status)
	}
	wouldChange := 0
	wouldSkip := 0
	for _, item := range disableResp.Results {
		switch item.Status {
		case "would_change":
			wouldChange++
		case "would_skip":
			wouldSkip++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected dry-run skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk disable dry-run item: %+v", item)
		}
	}
	if wouldChange != 1 || wouldSkip != 1 {
		t.Fatalf("expected one would_change and one would_skip result, got %+v", disableResp.Results)
	}
	assertJobState(activeJob.JobID, false, false)
	assertJobState(disabledJob.JobID, true, false)

	var enableResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/enable", map[string]any{
		"job_ids": []string{activeJob.JobID, disabledJob.JobID},
		"dry_run": true,
	}, &enableResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk enable dry-run response, got %d", status)
	}
	wouldChange = 0
	wouldSkip = 0
	for _, item := range enableResp.Results {
		switch item.Status {
		case "would_change":
			wouldChange++
		case "would_skip":
			wouldSkip++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected dry-run skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk enable dry-run item: %+v", item)
		}
	}
	if wouldChange != 1 || wouldSkip != 1 {
		t.Fatalf("expected one would_change and one would_skip result, got %+v", enableResp.Results)
	}
	assertJobState(activeJob.JobID, false, false)
	assertJobState(disabledJob.JobID, true, false)

	var pauseResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/pause", map[string]any{
		"job_ids": []string{activeJob.JobID, pausedJob.JobID},
		"dry_run": true,
	}, &pauseResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk pause dry-run response, got %d", status)
	}
	wouldChange = 0
	wouldSkip = 0
	for _, item := range pauseResp.Results {
		switch item.Status {
		case "would_change":
			wouldChange++
		case "would_skip":
			wouldSkip++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected dry-run skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk pause dry-run item: %+v", item)
		}
	}
	if wouldChange != 1 || wouldSkip != 1 {
		t.Fatalf("expected one would_change and one would_skip result, got %+v", pauseResp.Results)
	}
	assertJobState(activeJob.JobID, false, false)
	assertJobState(pausedJob.JobID, false, true)

	var resumeResp struct {
		Count   int `json:"count"`
		Results []struct {
			JobID        string `json:"job_id"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/resume", map[string]any{
		"job_ids": []string{activeJob.JobID, pausedJob.JobID},
		"dry_run": true,
	}, &resumeResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 bulk resume dry-run response, got %d", status)
	}
	wouldChange = 0
	wouldSkip = 0
	for _, item := range resumeResp.Results {
		switch item.Status {
		case "would_change":
			wouldChange++
		case "would_skip":
			wouldSkip++
			if item.ErrorCode == "" || item.ErrorMessage == "" {
				t.Fatalf("expected dry-run skip reason, got %+v", item)
			}
		default:
			t.Fatalf("unexpected bulk resume dry-run item: %+v", item)
		}
	}
	if wouldChange != 1 || wouldSkip != 1 {
		t.Fatalf("expected one would_change and one would_skip result, got %+v", resumeResp.Results)
	}
	assertJobState(activeJob.JobID, false, false)
	assertJobState(pausedJob.JobID, false, true)
}

func TestRegisterWorkerReusesIdentityByName(t *testing.T) {
	jobStore := openTestStore(t)

	server := newTestServer(t, jobStore)
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var first RegisterWorkerResponse
	status := doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 1,
	}, &first)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering worker, got %d", status)
	}

	var second RegisterWorkerResponse
	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/register", map[string]any{
		"name":            "worker-api",
		"queues":          []string{"default"},
		"capabilities":    map[string]any{"http": true},
		"max_concurrency": 2,
	}, &second)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 re-registering worker, got %d", status)
	}
	if first.WorkerID != second.WorkerID {
		t.Fatalf("expected stable worker identity, got %s then %s", first.WorkerID, second.WorkerID)
	}
}

func doJSONRequest(t *testing.T, client *http.Client, token, method, url string, requestBody any, responseBody any) int {
	return doJSONRequestWithHeaders(t, client, token, method, url, nil, requestBody, responseBody)
}

func doJSONRequestWithHeaders(t *testing.T, client *http.Client, token, method, url string, headers map[string]string, requestBody any, responseBody any) int {
	t.Helper()

	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if responseBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(responseBody); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	return resp.StatusCode
}

func testAPIConfig() config.APIConfig {
	return config.APIConfig{
		AuthTokens:              adminToken + ":admin," + tenantToken + ":tenant:tenant-api," + workerToken + ":worker:worker-api",
		WorkerHeartbeatInterval: 5 * time.Second,
		WorkerLeaseDuration:     30 * time.Second,
	}
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

func newTestServer(t *testing.T, jobStore *store.Store) *Server {
	t.Helper()

	server, err := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server
}

func resetTablesForAPI(t *testing.T, jobStore *store.Store) {
	t.Helper()

	tx, err := jobStore.DB().Begin()
	if err != nil {
		t.Fatalf("begin reset tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(989898)`); err != nil {
		t.Fatalf("acquire reset lock: %v", err)
	}
	if _, err := tx.Exec(`
		ALTER TABLE workers ADD COLUMN IF NOT EXISTS session_token_hash TEXT NOT NULL DEFAULT '';
		ALTER TABLE workers ADD COLUMN IF NOT EXISTS session_issued_at TIMESTAMPTZ;
	`); err != nil {
		t.Fatalf("ensure worker session columns: %v", err)
	}
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS api_idempotency_keys (
			tenant_id TEXT NOT NULL,
			operation TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			response_status INTEGER NOT NULL,
			response_body JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, operation, idempotency_key)
		)
	`); err != nil {
		t.Fatalf("ensure idempotency table: %v", err)
	}
	if _, err := tx.Exec(`TRUNCATE TABLE api_idempotency_keys, audit_events, run_events, runs, job_schedules, workers, jobs, tenant_quotas RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit reset tx: %v", err)
	}
}

func TestMain(m *testing.M) {
	dbURL := os.Getenv("RUNQ_DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultTestDBURL
	}

	db, err := store.Open(dbURL)
	if err == nil {
		if _, lockErr := db.DB().Exec(`SELECT pg_advisory_lock(999001)`); lockErr == nil {
			code := m.Run()
			_, _ = db.DB().Exec(`SELECT pg_advisory_unlock(999001)`)
			_ = db.Close()
			os.Exit(code)
		}
		_ = db.Close()
	}

	os.Exit(m.Run())
}
