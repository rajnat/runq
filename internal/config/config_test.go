package config

import (
	"testing"
	"time"
)

func TestAPIConfigValidateRejectsEmptyAuthTokensByDefault(t *testing.T) {
	cfg := APIConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty auth tokens")
	}
}

func TestAPIConfigValidateRejectsInsecureDevModeOnNonLoopbackBind(t *testing.T) {
	cfg := APIConfig{InsecureDevMode: true, Address: ":8080"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected insecure dev mode on non-loopback bind to be rejected")
	}
}

func TestAPIConfigValidateAllowsExplicitInsecureDevModeOnLoopbackBind(t *testing.T) {
	cfg := APIConfig{InsecureDevMode: true, Address: "127.0.0.1:8080"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected loopback insecure dev mode to validate: %v", err)
	}
}

func TestLoadAPIDefaultsBindMetricsToLoopback(t *testing.T) {
	t.Setenv("RUNQ_API_METRICS_ADDR", "")
	cfg := LoadAPI()
	if cfg.MetricsAddress != "127.0.0.1:9090" {
		t.Fatalf("expected loopback metrics address, got %q", cfg.MetricsAddress)
	}
}

func TestLoadAPIParsesDBPoolAndHTTPTimeoutSettings(t *testing.T) {
	t.Setenv("RUNQ_DB_MAX_IDLE_CONNS", "9")
	t.Setenv("RUNQ_DB_MAX_OPEN_CONNS", "17")
	t.Setenv("RUNQ_DB_CONN_MAX_LIFETIME_SECONDS", "123")
	t.Setenv("RUNQ_API_READ_TIMEOUT_SECONDS", "11")
	t.Setenv("RUNQ_API_WRITE_TIMEOUT_SECONDS", "12")
	t.Setenv("RUNQ_API_IDLE_TIMEOUT_SECONDS", "13")
	t.Setenv("RUNQ_API_READ_HEADER_TIMEOUT_SECONDS", "14")
	t.Setenv("RUNQ_METRICS_READ_TIMEOUT_SECONDS", "21")
	t.Setenv("RUNQ_METRICS_WRITE_TIMEOUT_SECONDS", "22")
	t.Setenv("RUNQ_METRICS_IDLE_TIMEOUT_SECONDS", "23")
	t.Setenv("RUNQ_METRICS_READ_HEADER_TIMEOUT_SECONDS", "24")

	cfg := LoadAPI()
	if cfg.DBMaxIdleConns != 9 || cfg.DBMaxOpenConns != 17 {
		t.Fatalf("unexpected db pool config: %+v", cfg)
	}
	if cfg.DBConnMaxLifetime != 123*time.Second {
		t.Fatalf("expected db conn lifetime 123s, got %s", cfg.DBConnMaxLifetime)
	}
	if cfg.ReadTimeout != 11*time.Second || cfg.WriteTimeout != 12*time.Second || cfg.IdleTimeout != 13*time.Second || cfg.ReadHeaderTimeout != 14*time.Second {
		t.Fatalf("unexpected api timeouts: %+v", cfg)
	}
	if cfg.MetricsReadTimeout != 21*time.Second || cfg.MetricsWriteTimeout != 22*time.Second || cfg.MetricsIdleTimeout != 23*time.Second || cfg.MetricsReadHeaderTimeout != 24*time.Second {
		t.Fatalf("unexpected metrics timeouts: %+v", cfg)
	}
}

func TestLoadComponentParsesDBPoolAndMetricsTimeoutSettings(t *testing.T) {
	t.Setenv("RUNQ_DB_MAX_IDLE_CONNS", "5")
	t.Setenv("RUNQ_DB_MAX_OPEN_CONNS", "15")
	t.Setenv("RUNQ_DB_CONN_MAX_LIFETIME_SECONDS", "45")
	t.Setenv("RUNQ_METRICS_READ_TIMEOUT_SECONDS", "31")
	t.Setenv("RUNQ_METRICS_WRITE_TIMEOUT_SECONDS", "32")
	t.Setenv("RUNQ_METRICS_IDLE_TIMEOUT_SECONDS", "33")
	t.Setenv("RUNQ_METRICS_READ_HEADER_TIMEOUT_SECONDS", "34")

	cfg := LoadComponent("scheduler")
	if cfg.DBMaxIdleConns != 5 || cfg.DBMaxOpenConns != 15 {
		t.Fatalf("unexpected component db pool config: %+v", cfg)
	}
	if cfg.DBConnMaxLifetime != 45*time.Second {
		t.Fatalf("expected component db conn lifetime 45s, got %s", cfg.DBConnMaxLifetime)
	}
	if cfg.MetricsReadTimeout != 31*time.Second || cfg.MetricsWriteTimeout != 32*time.Second || cfg.MetricsIdleTimeout != 33*time.Second || cfg.MetricsReadHeaderTimeout != 34*time.Second {
		t.Fatalf("unexpected component metrics timeouts: %+v", cfg)
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
