package config

import (
	"testing"
)

func TestAPIConfigValidateRejectsEmptyAuthTokensByDefault(t *testing.T) {
	cfg := APIConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty auth tokens")
	}
}

func TestAPIConfigValidateAllowsExplicitInsecureDevMode(t *testing.T) {
	cfg := APIConfig{InsecureDevMode: true}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected insecure dev mode to allow empty auth tokens: %v", err)
	}
}

func TestLoadAPIDefaultsBindMetricsToLoopback(t *testing.T) {
	t.Setenv("RUNQ_API_METRICS_ADDR", "")
	cfg := LoadAPI()
	if cfg.MetricsAddress != "127.0.0.1:9090" {
		t.Fatalf("expected loopback metrics address, got %q", cfg.MetricsAddress)
	}
}

func TestLoadWorkerDefaultsBindMetricsToLoopback(t *testing.T) {
	t.Setenv("RUNQ_WORKER_METRICS_ADDR", "")
	cfg := LoadWorker()
	if cfg.MetricsAddress != "127.0.0.1:9093" {
		t.Fatalf("expected loopback worker metrics address, got %q", cfg.MetricsAddress)
	}
}

func TestWorkerConfigValidateRejectsMissingAuthToken(t *testing.T) {
	cfg := WorkerConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing worker auth token")
	}
}
