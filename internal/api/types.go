package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

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
	ConcurrencyKey          string         `json:"concurrency_key,omitempty"`
}

type Schedule struct {
	Type     string `json:"type"`
	Cron     string `json:"cron,omitempty"`
	Timezone string `json:"timezone,omitempty"`
	RunAt    string `json:"run_at,omitempty"`
}

type CreateJobResponse struct {
	JobID  string  `json:"job_id"`
	RunID  *string `json:"run_id"`
	Status string  `json:"status"`
}

type UpdateJobRequest struct {
	Name                    *string        `json:"name,omitempty"`
	Queue                   *string        `json:"queue,omitempty"`
	Payload                 map[string]any `json:"payload,omitempty"`
	Schedule                *Schedule      `json:"schedule,omitempty"`
	Priority                *int           `json:"priority,omitempty"`
	MaxRetries              *int           `json:"max_retries,omitempty"`
	TimeoutSeconds          *int           `json:"timeout_seconds,omitempty"`
	RetryBackoffBaseSeconds *int           `json:"retry_backoff_base_seconds,omitempty"`
	ConcurrencyKey          *string        `json:"concurrency_key,omitempty"`
}

type AuthMeResponse struct {
	Role       string  `json:"role"`
	TenantID   *string `json:"tenant_id,omitempty"`
	WorkerName *string `json:"worker_name,omitempty"`
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
	WorkerSessionToken        string `json:"worker_session_token"`
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

type PaginationMeta struct {
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Returned   int     `json:"returned"`
	HasMore    bool    `json:"has_more"`
	NextOffset *int    `json:"next_offset,omitempty"`
	NextCursor *string `json:"next_cursor,omitempty"`
}

type ListJobsResponse struct {
	Jobs       []store.Job    `json:"jobs"`
	Pagination PaginationMeta `json:"pagination"`
}

type ListRunsResponse struct {
	Runs       []store.Run    `json:"runs"`
	Pagination PaginationMeta `json:"pagination"`
}

type ListWorkersResponse struct {
	Workers    []store.Worker `json:"workers"`
	Pagination PaginationMeta `json:"pagination"`
}

type WorkerInflightRun struct {
	RunID          string     `json:"run_id"`
	JobID          string     `json:"job_id"`
	TenantID       string     `json:"tenant_id"`
	Queue          string     `json:"queue"`
	Status         string     `json:"status"`
	Attempt        int        `json:"attempt"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	LeaseToken     int64      `json:"lease_token"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
}

type WorkerHealthSummary struct {
	HeartbeatAgeSeconds   int64 `json:"heartbeat_age_seconds"`
	HeartbeatDriftSeconds int64 `json:"heartbeat_drift_seconds"`
	HeartbeatStale        bool  `json:"heartbeat_stale"`
	InflightAssignments   int   `json:"inflight_assignments"`
	AvailableCapacity     int   `json:"available_capacity"`
	AtCapacity            bool  `json:"at_capacity"`
}

type WorkerDetail struct {
	store.Worker
	InflightAssignmentCount int                 `json:"inflight_assignment_count"`
	InflightRuns            []WorkerInflightRun `json:"inflight_runs,omitempty"`
	Health                  WorkerHealthSummary `json:"health"`
}

type WorkerDetailResponse struct {
	Worker WorkerDetail `json:"worker"`
}

type BulkRunOperationRequest struct {
	RunIDs       []string `json:"run_ids,omitempty"`
	TenantID     string   `json:"tenant_id,omitempty"`
	JobID        string   `json:"job_id,omitempty"`
	Statuses     []string `json:"status,omitempty"`
	DeadLettered *bool    `json:"dead_lettered,omitempty"`
	DryRun       bool     `json:"dry_run,omitempty"`
}

type BulkRunOperationItem struct {
	FromRun      string `json:"from_run"`
	RunID        string `json:"run_id,omitempty"`
	Status       string `json:"status"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type BulkRunOperationResponse struct {
	Count   int                    `json:"count"`
	Results []BulkRunOperationItem `json:"results"`
}

type BulkJobOperationRequest struct {
	JobIDs   []string `json:"job_ids,omitempty"`
	TenantID string   `json:"tenant_id,omitempty"`
	Queue    string   `json:"queue,omitempty"`
	Kind     string   `json:"kind,omitempty"`
	Paused   *bool    `json:"paused,omitempty"`
	Disabled *bool    `json:"disabled,omitempty"`
	DryRun   bool     `json:"dry_run,omitempty"`
}

type BulkJobOperationItem struct {
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type BulkJobOperationResponse struct {
	Count   int                    `json:"count"`
	Results []BulkJobOperationItem `json:"results"`
}

type RequeueRunResponse struct {
	RunID   string `json:"run_id"`
	Status  string `json:"status"`
	JobID   string `json:"job_id"`
	Source  string `json:"source"`
	FromRun string `json:"from_run"`
}

type CancelJobResponse struct {
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
	CanceledRuns int64  `json:"canceled_runs"`
}

type JobLifecycleResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type TriggerJobResponse struct {
	JobID  string `json:"job_id"`
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type UpsertTenantQuotaRequest struct {
	MaxInflight    int `json:"max_inflight"`
	MaxPendingRuns int `json:"max_pending_runs"`
	MaxActiveJobs  int `json:"max_active_jobs"`
}

type TenantQuotaResponse struct {
	TenantID       string `json:"tenant_id"`
	MaxInflight    int    `json:"max_inflight"`
	MaxPendingRuns int    `json:"max_pending_runs"`
	MaxActiveJobs  int    `json:"max_active_jobs"`
	UpdatedAt      string `json:"updated_at"`
}

type ListAuditEventsResponse struct {
	Events     []store.AuditEvent `json:"events"`
	Pagination PaginationMeta     `json:"pagination"`
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
		scheduleType := strings.TrimSpace(r.Schedule.Type)
		if scheduleType == "" {
			return errors.New("schedule.type is required")
		}
		switch scheduleType {
		case "once":
			if strings.TrimSpace(r.Schedule.Cron) != "" {
				return errors.New("schedule.cron is only valid for cron schedules")
			}
			if strings.TrimSpace(r.Schedule.Timezone) != "" {
				return errors.New("schedule.timezone is only valid for cron schedules")
			}
			if strings.TrimSpace(r.Schedule.RunAt) != "" {
				return errors.New("schedule.run_at is only valid for delayed schedules")
			}
		case "delayed":
			if strings.TrimSpace(r.Schedule.Cron) != "" {
				return errors.New("schedule.cron is only valid for cron schedules")
			}
			if strings.TrimSpace(r.Schedule.Timezone) != "" {
				return errors.New("schedule.timezone is only valid for cron schedules")
			}
			runAt := strings.TrimSpace(r.Schedule.RunAt)
			if runAt == "" {
				return errors.New("schedule.run_at is required for delayed schedules")
			}
			if _, err := time.Parse(time.RFC3339, runAt); err != nil {
				return errors.New("schedule.run_at must be a valid RFC3339 timestamp")
			}
		case "cron":
			if strings.TrimSpace(r.Schedule.Cron) == "" {
				return errors.New("schedule.cron is required for cron schedules")
			}
			if strings.TrimSpace(r.Schedule.RunAt) != "" {
				return errors.New("schedule.run_at is only valid for delayed schedules")
			}
		default:
			return errors.New("schedule.type must be one of once, delayed, or cron")
		}
	}
	return nil
}

func (r UpdateJobRequest) Validate() error {
	if r.Name != nil && strings.TrimSpace(*r.Name) == "" {
		return errors.New("name must not be blank")
	}
	if r.Queue != nil && strings.TrimSpace(*r.Queue) == "" {
		return errors.New("queue must not be blank")
	}
	if r.Schedule != nil {
		scheduleType := strings.TrimSpace(r.Schedule.Type)
		if scheduleType == "" {
			return errors.New("schedule.type is required")
		}
		switch scheduleType {
		case "delayed":
			runAt := strings.TrimSpace(r.Schedule.RunAt)
			if runAt == "" {
				return errors.New("schedule.run_at is required for delayed schedules")
			}
			if strings.TrimSpace(r.Schedule.Cron) != "" || strings.TrimSpace(r.Schedule.Timezone) != "" {
				return errors.New("delayed schedule updates only support run_at")
			}
			if _, err := time.Parse(time.RFC3339, runAt); err != nil {
				return errors.New("schedule.run_at must be a valid RFC3339 timestamp")
			}
		case "cron":
			if strings.TrimSpace(r.Schedule.RunAt) != "" {
				return errors.New("schedule.run_at is only valid for delayed schedules")
			}
			if strings.TrimSpace(r.Schedule.Cron) == "" && strings.TrimSpace(r.Schedule.Timezone) == "" {
				return errors.New("cron schedule updates require cron or timezone")
			}
		case "once":
			return errors.New("once schedules cannot be mutated")
		default:
			return errors.New("schedule.type must be one of delayed or cron")
		}
	}
	if r.Priority != nil && *r.Priority <= 0 {
		return errors.New("priority must be greater than zero")
	}
	if r.MaxRetries != nil && *r.MaxRetries < 0 {
		return errors.New("max_retries must be zero or greater")
	}
	if r.TimeoutSeconds != nil && *r.TimeoutSeconds <= 0 {
		return errors.New("timeout_seconds must be greater than zero")
	}
	if r.RetryBackoffBaseSeconds != nil && *r.RetryBackoffBaseSeconds <= 0 {
		return errors.New("retry_backoff_base_seconds must be greater than zero")
	}
	return nil
}

func (r BulkRunOperationRequest) Validate() error {
	if len(r.RunIDs) == 0 && strings.TrimSpace(r.JobID) == "" && len(r.Statuses) == 0 && r.DeadLettered == nil {
		return errors.New("at least one selector is required")
	}
	for _, id := range r.RunIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("run_ids must not contain blank values")
		}
	}
	return nil
}

func (r BulkJobOperationRequest) Validate() error {
	if len(r.JobIDs) == 0 && strings.TrimSpace(r.TenantID) == "" && strings.TrimSpace(r.Queue) == "" && strings.TrimSpace(r.Kind) == "" && r.Paused == nil && r.Disabled == nil {
		return errors.New("at least one selector is required")
	}
	for _, id := range r.JobIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("job_ids must not contain blank values")
		}
	}
	return nil
}

func (r RegisterWorkerRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	hasQueue := false
	for _, queue := range r.Queues {
		if strings.TrimSpace(queue) != "" {
			hasQueue = true
			break
		}
	}
	if !hasQueue {
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

func (r HeartbeatRequest) Validate() error {
	if len(r.Running) > 100 {
		return errors.New("running must contain at most 100 items")
	}
	seen := make(map[string]struct{}, len(r.Running))
	for _, item := range r.Running {
		if strings.TrimSpace(item.RunID) == "" || item.LeaseToken <= 0 {
			return errors.New("each running item must include run_id and lease_token")
		}
		if _, exists := seen[item.RunID]; exists {
			return errors.New("running must not contain duplicate run_id values")
		}
		seen[item.RunID] = struct{}{}
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
	if r.MaxInflight < 0 {
		return errors.New("max_inflight must be zero or greater")
	}
	if r.MaxPendingRuns < 0 {
		return errors.New("max_pending_runs must be zero or greater")
	}
	if r.MaxActiveJobs < 0 {
		return errors.New("max_active_jobs must be zero or greater")
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
		ConcurrencyKey:          strings.TrimSpace(r.ConcurrencyKey),
	}

	if r.Schedule == nil {
		input.ScheduleType = "once"
		return input
	}

	input.ScheduleType = strings.TrimSpace(r.Schedule.Type)
	input.CronExpr = strings.TrimSpace(r.Schedule.Cron)
	input.Timezone = strings.TrimSpace(r.Schedule.Timezone)
	if input.ScheduleType == "delayed" {
		runAt, err := time.Parse(time.RFC3339, strings.TrimSpace(r.Schedule.RunAt))
		if err == nil {
			utc := runAt.UTC()
			input.RunAt = &utc
		}
	}
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

func parseOptionalInt(value string, minimum int) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum {
		return 0, errors.New("invalid integer")
	}
	return parsed, nil
}

func parseOptionalTime(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, errors.New("invalid timestamp")
	}
	utc := parsed.UTC()
	return &utc, nil
}

func scheduleTypePtr(schedule *Schedule) *string {
	if schedule == nil {
		return nil
	}
	value := strings.TrimSpace(schedule.Type)
	if value == "" {
		return nil
	}
	return &value
}

func scheduleCronPtr(schedule *Schedule) *string {
	if schedule == nil {
		return nil
	}
	value := strings.TrimSpace(schedule.Cron)
	if value == "" {
		return nil
	}
	return &value
}

func scheduleTimezonePtr(schedule *Schedule) *string {
	if schedule == nil {
		return nil
	}
	value := strings.TrimSpace(schedule.Timezone)
	if value == "" {
		return nil
	}
	return &value
}

func scheduleRunAtPtr(schedule *Schedule) *time.Time {
	if schedule == nil {
		return nil
	}
	value := strings.TrimSpace(schedule.RunAt)
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	utc := parsed.UTC()
	return &utc
}

func paginationMeta(limit, offset, returned int, hasMore bool) PaginationMeta {
	meta := PaginationMeta{
		Limit:    limit,
		Offset:   offset,
		Returned: returned,
		HasMore:  hasMore,
	}
	if hasMore {
		next := offset + returned
		meta.NextOffset = &next
	}
	return meta
}

func clampPageLimit(limit int) int {
	const defaultPageLimit = 100
	const maxPageLimit = 200
	if limit <= 0 {
		return defaultPageLimit
	}
	if limit > maxPageLimit {
		return maxPageLimit
	}
	return limit
}

type encodedCursor struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
}

func encodePageCursor(boundary *store.PageBoundary) (*string, error) {
	if boundary == nil {
		return nil, nil
	}
	payload, err := json.Marshal(encodedCursor{CreatedAt: boundary.CreatedAt.UTC().Format(time.RFC3339Nano), ID: boundary.ID})
	if err != nil {
		return nil, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return &encoded, nil
}

func decodePageCursor(value string) (*store.PageBoundary, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("cursor must be valid base64url")
	}
	var payload encodedCursor
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, errors.New("cursor must be valid JSON")
	}
	if payload.ID == "" || payload.CreatedAt == "" {
		return nil, errors.New("cursor is missing required fields")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil {
		return nil, errors.New("cursor created_at must be RFC3339Nano")
	}
	return &store.PageBoundary{CreatedAt: createdAt.UTC(), ID: payload.ID}, nil
}
