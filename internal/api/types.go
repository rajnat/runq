package api

import (
	"errors"
	"strings"

	"github.com/eswar/runq/internal/store"
)

type CreateJobRequest struct {
	Name                    string         `json:"name"`
	TenantID                string         `json:"tenant_id,omitempty"`
	Queue                   string         `json:"queue"`
	Kind                    string         `json:"kind"`
	Payload                 map[string]any `json:"payload"`
	Schedule                *Schedule      `json:"schedule,omitempty"`
	Priority                int            `json:"priority"`
	MaxRetries              int            `json:"max_retries"`
	TimeoutSeconds          int            `json:"timeout_seconds"`
	RetryBackoffBaseSeconds int            `json:"retry_backoff_base_seconds"`
	DedupeKey               string         `json:"dedupe_key,omitempty"`
}

type Schedule struct {
	Type     string `json:"type"`
	Cron     string `json:"cron,omitempty"`
	Timezone string `json:"timezone,omitempty"`
}

type CreateJobResponse struct {
	JobID  string  `json:"job_id"`
	RunID  *string `json:"run_id"`
	Status string  `json:"status"`
}

type RegisterWorkerRequest struct {
	Name           string         `json:"name"`
	Queues         []string       `json:"queues"`
	Capabilities   map[string]any `json:"capabilities"`
	MaxConcurrency int            `json:"max_concurrency"`
	Metadata       map[string]any `json:"metadata"`
}

type RegisterWorkerResponse struct {
	WorkerID                  string `json:"worker_id"`
	HeartbeatIntervalSeconds  int    `json:"heartbeat_interval_seconds"`
	LeaseRenewIntervalSeconds int    `json:"lease_renew_interval_seconds"`
}

type PollWorkerRequest struct {
	AvailableSlots int `json:"available_slots"`
}

type PollWorkerResponse struct {
	Assignments []WorkerAssignmentResponse `json:"assignments"`
}

type WorkerAssignmentResponse struct {
	RunID          string         `json:"run_id"`
	JobID          string         `json:"job_id"`
	Kind           string         `json:"kind"`
	Payload        map[string]any `json:"payload"`
	TimeoutSeconds int            `json:"timeout_seconds"`
	LeaseToken     int64          `json:"lease_token"`
	LeaseExpiresAt string         `json:"lease_expires_at"`
}

type HeartbeatRequest struct {
	Running []HeartbeatRun `json:"running"`
}

type HeartbeatRun struct {
	RunID      string         `json:"run_id"`
	LeaseToken int64          `json:"lease_token"`
	Progress   map[string]any `json:"progress"`
}

type CompleteRunRequest struct {
	RunID      string         `json:"run_id"`
	LeaseToken int64          `json:"lease_token"`
	Status     string         `json:"status"`
	Result     map[string]any `json:"result"`
}

type FailRunRequest struct {
	RunID        string `json:"run_id"`
	LeaseToken   int64  `json:"lease_token"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

type GetRunResponse struct {
	Run    store.Run        `json:"run"`
	Events []store.RunEvent `json:"events"`
}

type CancelJobResponse struct {
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
	CanceledRuns int64  `json:"canceled_runs"`
}

type UpsertTenantQuotaRequest struct {
	MaxInflight int `json:"max_inflight"`
}

type TenantQuotaResponse struct {
	TenantID    string `json:"tenant_id"`
	MaxInflight int    `json:"max_inflight"`
	UpdatedAt   string `json:"updated_at"`
}

type ListAuditEventsResponse struct {
	Events []store.AuditEvent `json:"events"`
}

func (r CreateJobRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(r.Queue) == "" {
		return errors.New("queue is required")
	}
	if strings.TrimSpace(r.Kind) == "" {
		return errors.New("kind is required")
	}
	if r.Schedule != nil {
		if strings.TrimSpace(r.Schedule.Type) == "" {
			return errors.New("schedule.type is required")
		}
		if r.Schedule.Type == "cron" && strings.TrimSpace(r.Schedule.Cron) == "" {
			return errors.New("schedule.cron is required for cron schedules")
		}
	}
	return nil
}

func (r RegisterWorkerRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if len(r.Queues) == 0 {
		return errors.New("at least one queue is required")
	}
	if r.MaxConcurrency <= 0 {
		return errors.New("max_concurrency must be greater than zero")
	}
	return nil
}

func (r PollWorkerRequest) Validate() error {
	if r.AvailableSlots <= 0 {
		return errors.New("available_slots must be greater than zero")
	}
	return nil
}

func (r CompleteRunRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("run_id is required")
	}
	if r.LeaseToken <= 0 {
		return errors.New("lease_token must be greater than zero")
	}
	if r.Status != "SUCCEEDED" {
		return errors.New("status must be SUCCEEDED")
	}
	return nil
}

func (r FailRunRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("run_id is required")
	}
	if r.LeaseToken <= 0 {
		return errors.New("lease_token must be greater than zero")
	}
	if strings.TrimSpace(r.ErrorCode) == "" {
		return errors.New("error_code is required")
	}
	return nil
}

func (r UpsertTenantQuotaRequest) Validate() error {
	if r.MaxInflight <= 0 {
		return errors.New("max_inflight must be greater than zero")
	}
	return nil
}

func (r CreateJobRequest) ToStoreInput() store.CreateJobInput {
	input := store.CreateJobInput{
		Name:                    strings.TrimSpace(r.Name),
		TenantID:                strings.TrimSpace(r.TenantID),
		Queue:                   strings.TrimSpace(r.Queue),
		Kind:                    strings.TrimSpace(r.Kind),
		Payload:                 r.Payload,
		Priority:                defaultInt(r.Priority, 100),
		MaxRetries:              defaultInt(r.MaxRetries, 3),
		TimeoutSeconds:          defaultInt(r.TimeoutSeconds, 300),
		RetryBackoffBaseSeconds: defaultInt(r.RetryBackoffBaseSeconds, 5),
		DedupeKey:               strings.TrimSpace(r.DedupeKey),
	}

	if r.Schedule == nil {
		input.ScheduleType = "once"
		return input
	}

	input.ScheduleType = strings.TrimSpace(r.Schedule.Type)
	input.CronExpr = strings.TrimSpace(r.Schedule.Cron)
	input.Timezone = strings.TrimSpace(r.Schedule.Timezone)
	if input.Timezone == "" {
		input.Timezone = "UTC"
	}

	return input
}

func (r RegisterWorkerRequest) ToStoreInput() store.RegisterWorkerInput {
	queues := make([]string, 0, len(r.Queues))
	for _, queue := range r.Queues {
		trimmed := strings.TrimSpace(queue)
		if trimmed != "" {
			queues = append(queues, trimmed)
		}
	}

	return store.RegisterWorkerInput{
		Name:           strings.TrimSpace(r.Name),
		Queues:         queues,
		Capabilities:   r.Capabilities,
		MaxConcurrency: r.MaxConcurrency,
		Metadata:       r.Metadata,
	}
}

func defaultInt(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func parseOptionalBool(value string) (*bool, error) {
	if value == "" {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		v := true
		return &v, nil
	case "false":
		v := false
		return &v, nil
	default:
		return nil, errors.New("must be true or false")
	}
}
