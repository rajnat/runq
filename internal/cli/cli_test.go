package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	apiPkg "github.com/eswar/runq/internal/api"
	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/store"
)

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv("RUNQ_API_BASE_URL", "http://example.test")
	t.Setenv("RUNQ_API_TOKEN", "secret-token")

	cfg := LoadConfigFromEnv()
	if cfg.BaseURL != "http://example.test" {
		t.Fatalf("expected base url from env, got %q", cfg.BaseURL)
	}
	if cfg.Token != "secret-token" {
		t.Fatalf("expected token from env, got %q", cfg.Token)
	}
}

func TestRunAuthMe(t *testing.T) {
	jobStore := openTestStoreForCLI(t)
	resetTablesForCLI(t, jobStore)
	server, err := apiPkg.NewServer(config.APIConfig{
		Address:                 ":0",
		DBConnString:            "",
		AuthTokens:              "tenant-token:tenant:tenant-api",
		WorkerHeartbeatInterval: 5 * time.Second,
		WorkerLeaseDuration:     30 * time.Second,
	}, log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	httpServer := httptest.NewServer(server.MuxForTests())
	defer httpServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(AppConfig{BaseURL: httpServer.URL, Token: "tenant-token"}, &stdout, &stderr)
	if err := app.Run([]string{"auth", "me"}); err != nil {
		t.Fatalf("run auth me: %v stderr=%s", err, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "role=tenant") || !strings.Contains(output, "tenant_id=tenant-api") {
		t.Fatalf("unexpected auth me output: %q", output)
	}
}

func TestRunConfigShow(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(AppConfig{BaseURL: "http://localhost:8080", Token: "token-value"}, &stdout, &stderr)
	if err := app.Run([]string{"config", "show"}); err != nil {
		t.Fatalf("run config show: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "base_url=http://localhost:8080") || !strings.Contains(output, "token_set=true") {
		t.Fatalf("unexpected config output: %q", output)
	}
}

func TestRunJobsCommands(t *testing.T) {
	jobStore := openTestStoreForCLI(t)
	resetTablesForCLI(t, jobStore)
	server, err := apiPkg.NewServer(config.APIConfig{
		Address:                 ":0",
		DBConnString:            "",
		AuthTokens:              "tenant-token:tenant:tenant-api",
		WorkerHeartbeatInterval: 5 * time.Second,
		WorkerLeaseDuration:     30 * time.Second,
	}, log.New(io.Discard, "", 0), jobStore, observability.NewRegistry())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	httpServer := httptest.NewServer(server.MuxForTests())
	defer httpServer.Close()

	newApp := func() (*App, *bytes.Buffer, *bytes.Buffer) {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		return New(AppConfig{BaseURL: httpServer.URL, Token: "tenant-token"}, &stdout, &stderr), &stdout, &stderr
	}

	createPayload := `{"name":"cli-job","tenant_id":"tenant-api","queue":"default","kind":"http","payload":{"url":"https://example.internal/task"}}`
	app, stdout, stderr := newApp()
	if err := app.Run([]string{"jobs", "create", createPayload}); err != nil {
		t.Fatalf("jobs create: %v stderr=%s", err, stderr.String())
	}
	createOut := stdout.String()
	if !strings.Contains(createOut, `"job_id"`) {
		t.Fatalf("expected job create output to include job_id, got %q", createOut)
	}
	var createResp apiPkg.CreateJobResponse
	if err := json.Unmarshal(stdout.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal create output: %v output=%q", err, createOut)
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "get", createResp.JobID}); err != nil {
		t.Fatalf("jobs get: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"id":"`+createResp.JobID+`"`) {
		t.Fatalf("expected jobs get output to include job id, got %q", stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "list", "--tenant-id", "tenant-api"}); err != nil {
		t.Fatalf("jobs list: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"jobs"`) || !strings.Contains(stdout.String(), createResp.JobID) {
		t.Fatalf("expected jobs list output to include created job, got %q", stdout.String())
	}

	updatePayload := `{"name":"cli-job-updated"}`
	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "update", createResp.JobID, updatePayload}); err != nil {
		t.Fatalf("jobs update: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `cli-job-updated`) {
		t.Fatalf("expected jobs update output to include new name, got %q", stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "disable", createResp.JobID}); err != nil {
		t.Fatalf("jobs disable: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status":"disabled"`) {
		t.Fatalf("expected jobs disable output to include disabled status, got %q", stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "enable", createResp.JobID}); err != nil {
		t.Fatalf("jobs enable: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status":"active"`) {
		t.Fatalf("expected jobs enable output to include active status, got %q", stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "pause", createResp.JobID}); err != nil {
		t.Fatalf("jobs pause: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status":"paused"`) {
		t.Fatalf("expected jobs pause output to include paused status, got %q", stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "resume", createResp.JobID}); err != nil {
		t.Fatalf("jobs resume: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status":"active"`) {
		t.Fatalf("expected jobs resume output to include active status, got %q", stdout.String())
	}

	triggerPayload := `{"name":"cli-trigger-job","tenant_id":"tenant-api","queue":"default","kind":"http","payload":{"url":"https://example.internal/task"},"schedule":{"type":"cron","cron":"*/5 * * * *","timezone":"UTC"}}`
	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "create", triggerPayload}); err != nil {
		t.Fatalf("jobs create trigger job: %v stderr=%s", err, stderr.String())
	}
	var triggerCreateResp apiPkg.CreateJobResponse
	if err := json.Unmarshal(stdout.Bytes(), &triggerCreateResp); err != nil {
		t.Fatalf("unmarshal trigger create output: %v output=%q", err, stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "trigger", triggerCreateResp.JobID}); err != nil {
		t.Fatalf("jobs trigger: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"run_id"`) {
		t.Fatalf("expected jobs trigger output to include run_id, got %q", stdout.String())
	}

	cancelPayload := `{"name":"cli-cancel-job","tenant_id":"tenant-api","queue":"default","kind":"http","payload":{"url":"https://example.internal/task"}}`
	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "create", cancelPayload}); err != nil {
		t.Fatalf("jobs create cancel job: %v stderr=%s", err, stderr.String())
	}
	var cancelCreateResp apiPkg.CreateJobResponse
	if err := json.Unmarshal(stdout.Bytes(), &cancelCreateResp); err != nil {
		t.Fatalf("unmarshal cancel create output: %v output=%q", err, stdout.String())
	}

	app, stdout, stderr = newApp()
	if err := app.Run([]string{"jobs", "cancel", cancelCreateResp.JobID}); err != nil {
		t.Fatalf("jobs cancel: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"canceled_runs"`) {
		t.Fatalf("expected jobs cancel output to include canceled_runs, got %q", stdout.String())
	}
}

func openTestStoreForCLI(t *testing.T) *store.Store {
	t.Helper()
	dbURL := os.Getenv("RUNQ_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://runq:runq@localhost:5432/runq?sslmode=disable"
	}
	jobStore, err := store.Open(dbURL)
	if err != nil {
		t.Skipf("skipping cli integration test, db unavailable: %v", err)
	}
	t.Cleanup(func() { _ = jobStore.Close() })
	return jobStore
}

func resetTablesForCLI(t *testing.T, jobStore *store.Store) {
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
