package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
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

	var beforeNextRunAt time.Time
	if err := jobStore.DB().QueryRowContext(context.Background(), `SELECT next_run_at FROM job_schedules WHERE job_id = $1`, createResp.JobID).Scan(&beforeNextRunAt); err != nil {
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

	var afterNextRunAt time.Time
	if err := jobStore.DB().QueryRowContext(context.Background(), `SELECT next_run_at FROM job_schedules WHERE job_id = $1`, createResp.JobID).Scan(&afterNextRunAt); err != nil {
		t.Fatalf("load cron schedule after update: %v", err)
	}
	if !afterNextRunAt.After(beforeNextRunAt) {
		t.Fatalf("expected next_run_at to be recomputed, before=%s after=%s", beforeNextRunAt, afterNextRunAt)
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
		Jobs       []store.Job   `json:"jobs"`
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
		Runs       []store.Run   `json:"runs"`
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

func TestCreateJobRejectsDuplicateDedupeKey(t *testing.T) {
	jobStore := openTestStore(t)

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

	status = doJSONRequest(t, httpServer.Client(), workerToken, http.MethodPost, httpServer.URL+"/v1/workers/"+otherResp.WorkerID+"/poll", map[string]any{
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
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/requeue", map[string]any{
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
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/redrive", map[string]any{
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

	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			FromRun      string `json:"from_run"`
			Status       string `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"results"`
	}
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/runs/cancel", map[string]any{
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
	if _, err := tx.Exec(`TRUNCATE TABLE audit_events, run_events, runs, job_schedules, workers, jobs, tenant_quotas RESTART IDENTITY CASCADE`); err != nil {
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
