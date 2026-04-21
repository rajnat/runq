package cli

import (
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

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
	server, err := apiPkg.NewServer(config.APIConfig{
		Address:                 ":0",
		DBConnString:            "",
		AuthTokens:              "tenant-token:tenant:tenant-api",
		WorkerHeartbeatInterval: 5,
		WorkerLeaseDuration:     30,
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
