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

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
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
}

func TestCancelJobEndpointCancelsPendingRun(t *testing.T) {
	jobStore := openTestStore(t)

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	queue := "api-cancel-" + time.Now().UTC().Format("150405.000000000")
	var createResp CreateJobResponse
	status := doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs", map[string]any{
		"name":      "api-cancel-test",
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
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodPost, httpServer.URL+"/v1/jobs/"+createResp.JobID+"/cancel", nil, &cancelResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 cancel response, got %d", status)
	}
	if cancelResp.CanceledRuns != 1 {
		t.Fatalf("expected one canceled run, got %+v", cancelResp)
	}

	var runResp GetRunResponse
	status = doJSONRequest(t, httpServer.Client(), tenantToken, http.MethodGet, httpServer.URL+"/v1/runs/"+*createResp.RunID, nil, &runResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 get run, got %d", status)
	}
	if runResp.Run.Status != "CANCELED" {
		t.Fatalf("expected canceled run, got %+v", runResp.Run)
	}
}

func TestTenantQuotaEndpoints(t *testing.T) {
	jobStore := openTestStore(t)

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	var upsertResp TenantQuotaResponse
	status := doJSONRequest(t, httpServer.Client(), adminToken, http.MethodPut, httpServer.URL+"/v1/tenants/tenant-api/quota", map[string]any{
		"max_inflight": 2,
	}, &upsertResp)
	if status != http.StatusOK {
		t.Fatalf("expected 200 upsert quota, got %d", status)
	}
	if upsertResp.TenantID != "tenant-api" || upsertResp.MaxInflight != 2 {
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
		if quota.TenantID == "tenant-api" && quota.MaxInflight == 2 {
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
			break
		}
	}
	if !found {
		t.Fatalf("expected quota audit event in response, got %+v", auditResp.Events)
	}
}

func TestUnauthorizedRequestsAreRejected(t *testing.T) {
	jobStore := openTestStore(t)

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	httpServer := httptest.NewServer(server.mux)
	defer httpServer.Close()

	status := doJSONRequest(t, httpServer.Client(), "", http.MethodGet, httpServer.URL+"/v1/jobs", nil, &map[string]any{})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got %d", status)
	}
}

func TestTenantCannotCrossTenantBoundaries(t *testing.T) {
	jobStore := openTestStore(t)

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
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

	server := NewServer(testAPIConfig(), log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
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
		AuthTokens: adminToken + ":admin," + tenantToken + ":tenant:tenant-api," + workerToken + ":worker",
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
