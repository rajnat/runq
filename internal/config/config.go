package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAPIAddr       = ":8080"
	defaultAPIBaseURL    = "http://localhost:8080"
	defaultDBConnString  = "postgres://runq:runq@localhost:5432/runq?sslmode=disable"
	defaultTickInterval  = 5 * time.Second
	defaultMigrationsDir = "migrations"
)

type APIConfig struct {
	Address        string
	DBConnString   string
	AuthTokens     string
	MetricsAddress string
	TraceEndpoint  string
}

type ComponentConfig struct {
	ComponentName     string
	DBConnString      string
	TickInterval      time.Duration
	LeaseDuration     time.Duration
	ClaimBatchSize    int
	TenantMaxInflight int
	MetricsAddress    string
	TraceEndpoint     string
}

type MigrationConfig struct {
	DatabaseURL   string
	MigrationsDir string
}

type WorkerConfig struct {
	APIBaseURL        string
	AuthToken         string
	Name              string
	Queues            []string
	Capabilities      []string
	MaxConcurrency    int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	ExecutionTime     time.Duration
	MetricsAddress    string
	TraceEndpoint     string
}

func LoadAPI() APIConfig {
	return APIConfig{
		Address:        envOrDefault("RUNQ_API_ADDR", defaultAPIAddr),
		DBConnString:   envOrDefault("RUNQ_DATABASE_URL", defaultDBConnString),
		AuthTokens:     envOrDefault("RUNQ_API_TOKENS", ""),
		MetricsAddress: envOrDefault("RUNQ_API_METRICS_ADDR", ":9090"),
		TraceEndpoint:  envOrDefault("RUNQ_TRACE_OTLP_ENDPOINT", ""),
	}
}

func LoadComponent(name string) ComponentConfig {
	return ComponentConfig{
		ComponentName:     name,
		DBConnString:      envOrDefault("RUNQ_DATABASE_URL", defaultDBConnString),
		TickInterval:      durationEnvOrDefault("RUNQ_TICK_INTERVAL_SECONDS", defaultTickInterval),
		LeaseDuration:     durationEnvOrDefault("RUNQ_LEASE_DURATION_SECONDS", 30*time.Second),
		ClaimBatchSize:    intEnvOrDefault("RUNQ_CLAIM_BATCH_SIZE", 10),
		TenantMaxInflight: intEnvOrDefaultAllowZero("RUNQ_TENANT_MAX_INFLIGHT", 0),
		MetricsAddress:    envOrDefault(componentMetricsEnvKey(name), defaultComponentMetricsAddr(name)),
		TraceEndpoint:     envOrDefault("RUNQ_TRACE_OTLP_ENDPOINT", ""),
	}
}

func LoadMigration() MigrationConfig {
	return MigrationConfig{
		DatabaseURL:   envOrDefault("RUNQ_DATABASE_URL", defaultDBConnString),
		MigrationsDir: envOrDefault("RUNQ_MIGRATIONS_DIR", defaultMigrationsDir),
	}
}

func LoadWorker() WorkerConfig {
	return WorkerConfig{
		APIBaseURL:        envOrDefault("RUNQ_API_BASE_URL", defaultAPIBaseURL),
		AuthToken:         envOrDefault("RUNQ_WORKER_AUTH_TOKEN", ""),
		Name:              envOrDefault("RUNQ_WORKER_NAME", "worker-local"),
		Queues:            csvEnvOrDefault("RUNQ_WORKER_QUEUES", []string{"default"}),
		Capabilities:      csvEnvOrDefault("RUNQ_WORKER_CAPABILITIES", []string{"http"}),
		MaxConcurrency:    intEnvOrDefault("RUNQ_WORKER_MAX_CONCURRENCY", 4),
		PollInterval:      durationEnvOrDefault("RUNQ_WORKER_POLL_INTERVAL_SECONDS", 2*time.Second),
		HeartbeatInterval: durationEnvOrDefault("RUNQ_WORKER_HEARTBEAT_INTERVAL_SECONDS", 5*time.Second),
		ExecutionTime:     durationEnvOrDefault("RUNQ_WORKER_EXECUTION_SECONDS", 3*time.Second),
		MetricsAddress:    envOrDefault("RUNQ_WORKER_METRICS_ADDR", ":9093"),
		TraceEndpoint:     envOrDefault("RUNQ_TRACE_OTLP_ENDPOINT", ""),
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func intEnvOrDefault(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return parsed
}

func intEnvOrDefaultAllowZero(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}

	return parsed
}

func csvEnvOrDefault(key string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			items = append(items, part)
		}
	}
	if len(items) == 0 {
		return fallback
	}
	return items
}

func componentMetricsEnvKey(name string) string {
	return "RUNQ_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_METRICS_ADDR"
}

func defaultComponentMetricsAddr(name string) string {
	switch name {
	case "scheduler":
		return ":9091"
	case "reaper":
		return ":9092"
	case "worker":
		return ":9093"
	default:
		return ""
	}
}
