package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"github.com/eswar/runq/internal/store"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Server struct {
	cfg        config.APIConfig
	logger     *log.Logger
	mux        *http.ServeMux
	store      *store.Store
	metrics    *observability.Registry
	authTokens map[string]principal
}

func NewServer(cfg config.APIConfig, logger *log.Logger, jobStore *store.Store, metrics *observability.Registry) (*Server, error) {
	authTokens, err := parseAuthTokens(cfg)
	if err != nil {
		return nil, fmt.Errorf("parse api auth tokens: %w", err)
	}

	server := &Server{
		cfg:        cfg,
		logger:     logger,
		mux:        http.NewServeMux(),
		store:      jobStore,
		metrics:    metrics,
		authTokens: authTokens,
	}

	server.routes()

	return server, nil
}

func (s *Server) Run() error {
	httpServer := &http.Server{
		Addr:              s.cfg.Address,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	s.logger.Printf("starting api server on %s", s.cfg.Address)
	return httpServer.ListenAndServe()
}

func (s *Server) routes() {
	s.handle("GET /healthz", s.handleHealth)
	s.handle("GET /readyz", s.handleReady)
	s.mux.Handle("/metrics", s.metrics.Handler())
	s.handle("GET /v1/jobs", s.handleListJobs)
	s.handle("POST /v1/jobs/{jobID}/cancel", s.handleCancelJob)
	s.handle("GET /v1/tenants/quotas", s.handleListTenantQuotas)
	s.handle("PUT /v1/tenants/{tenantID}/quota", s.handleUpsertTenantQuota)
	s.handle("GET /v1/audit/events", s.handleListAuditEvents)
	s.handle("GET /v1/runs", s.handleListRuns)
	s.handle("GET /v1/runs/{runID}", s.handleGetRun)
	s.handle("POST /v1/jobs", s.handleCreateJob)
	s.handle("GET /v1/workers", s.handleListWorkers)
	s.handle("POST /v1/workers/register", s.handleRegisterWorker)
	s.handle("POST /v1/workers/{workerID}/poll", s.handlePollWorker)
	s.handle("POST /v1/workers/{workerID}/heartbeat", s.handleHeartbeatWorker)
	s.handle("POST /v1/workers/{workerID}/complete", s.handleCompleteRun)
	s.handle("POST /v1/workers/{workerID}/fail", s.handleFailRun)
}

func (s *Server) handle(pattern string, handler http.HandlerFunc) {
	s.mux.Handle(pattern, s.instrument(pattern, handler))
}

func (s *Server) instrument(route string, handler http.HandlerFunc) http.Handler {
	method := strings.TrimSpace(strings.SplitN(route, " ", 2)[0])
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := observability.Extract(r.Context(), r.Header)
		ctx, span := observability.Tracer("runq/api").Start(ctx, route, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		handler(recorder, r.WithContext(ctx))

		s.metrics.IncCounterVec("runq_api_requests_total", map[string]string{
			"method": method,
			"route":  route,
			"status": statusClass(recorder.status),
		})
		s.metrics.ObserveHistogramVec("runq_api_request_duration_seconds", map[string]string{
			"method": method,
			"route":  route,
		}, time.Since(started).Seconds())
		span.SetAttributes(
			attribute.String("http.request.method", method),
			attribute.String("http.route", route),
			attribute.String("url.path", r.URL.Path),
			attribute.Int("http.response.status_code", recorder.status),
		)
		if recorder.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func statusClass(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "DATABASE_UNAVAILABLE", "database is unavailable")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
	})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot list jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	disabled, err := parseOptionalBool(r.URL.Query().Get("disabled"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "disabled must be true or false")
		return
	}

	filterTenantID, allowed := authorizedTenantFilter(principal, r.URL.Query().Get("tenant_id"))
	if !allowed {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	jobs, err := s.store.ListJobs(ctx, store.JobFilter{
		TenantID: filterTenantID,
		Queue:    r.URL.Query().Get("queue"),
		Kind:     r.URL.Query().Get("kind"),
		Disabled: disabled,
	})
	if err != nil {
		s.logger.Printf("list jobs failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list jobs")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}

	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	if principal.Role == roleTenant {
		if req.TenantID != "" && req.TenantID != principal.TenantID {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		req.TenantID = principal.TenantID
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot create jobs")
		return
	}

	resp, err := s.createJob(ctx, req)
	if err != nil {
		s.logger.Printf("create job failed: %v", err)
		s.metrics.IncCounter("runq_api_job_create_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to create job")
		return
	}

	s.metrics.IncCounter("runq_api_jobs_created_total")
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot cancel jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	job, err := s.store.GetJob(ctx, r.PathValue("jobID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		s.logger.Printf("load job for cancel failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	result, err := s.store.CancelJob(ctx, r.PathValue("jobID"), "canceled via api", s.auditInput(principal, "JOB_CANCEL", "job", job.ID, job.TenantID, map[string]any{
		"reason": "canceled via api",
	}))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		s.logger.Printf("cancel job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to cancel job")
		return
	}

	writeJSON(w, http.StatusOK, CancelJobResponse{
		JobID:        result.JobID,
		Status:       "cancel_requested",
		CanceledRuns: result.CanceledRuns,
	})
}

func (s *Server) handleListTenantQuotas(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	quotas, err := s.store.ListTenantQuotas(ctx)
	if err != nil {
		s.logger.Printf("list tenant quotas failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list tenant quotas")
		return
	}

	response := make([]TenantQuotaResponse, 0, len(quotas))
	for _, quota := range quotas {
		response = append(response, TenantQuotaResponse{
			TenantID:    quota.TenantID,
			MaxInflight: quota.MaxInflight,
			UpdatedAt:   quota.UpdatedAt.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"tenant_quotas": response})
}

func (s *Server) handleListAuditEvents(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		var parsed int
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "limit must be a positive integer")
			return
		}
		limit = parsed
	}

	events, err := s.store.ListAuditEvents(ctx, store.AuditEventFilter{
		TenantID:     strings.TrimSpace(r.URL.Query().Get("tenant_id")),
		Action:       strings.TrimSpace(r.URL.Query().Get("action")),
		ResourceType: strings.TrimSpace(r.URL.Query().Get("resource_type")),
		Limit:        limit,
	})
	if err != nil {
		s.logger.Printf("list audit events failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list audit events")
		return
	}

	writeJSON(w, http.StatusOK, ListAuditEventsResponse{Events: events})
}

func (s *Server) handleUpsertTenantQuota(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req UpsertTenantQuotaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	tenantID := r.PathValue("tenantID")
	quota, err := s.store.UpsertTenantQuota(ctx, tenantID, req.MaxInflight, s.auditInput(principal, "TENANT_QUOTA_UPSERT", "tenant_quota", tenantID, tenantID, map[string]any{
		"max_inflight": req.MaxInflight,
	}))
	if err != nil {
		s.logger.Printf("upsert tenant quota failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to upsert tenant quota")
		return
	}

	writeJSON(w, http.StatusOK, TenantQuotaResponse{
		TenantID:    quota.TenantID,
		MaxInflight: quota.MaxInflight,
		UpdatedAt:   quota.UpdatedAt.Format(time.RFC3339),
	})
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot list runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	filterTenantID, allowed := authorizedTenantFilter(principal, r.URL.Query().Get("tenant_id"))
	if !allowed {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	runs, err := s.store.ListRuns(ctx, store.RunFilter{
		TenantID: filterTenantID,
		Status:   r.URL.Query().Get("status"),
		Queue:    r.URL.Query().Get("queue"),
		WorkerID: r.URL.Query().Get("worker_id"),
		JobID:    r.URL.Query().Get("job_id"),
	})
	if err != nil {
		s.logger.Printf("list runs failed: %v", err)
		s.metrics.IncCounter("runq_api_run_list_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list runs")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot inspect runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	run, events, err := s.store.GetRun(ctx, r.PathValue("runID"))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "CONFLICT", "failed to load run")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		s.logger.Printf("get run failed: %v", err)
		s.metrics.IncCounter("runq_api_run_get_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to get run")
		return
	}
	if !canAccessTenant(principal, run.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	writeJSON(w, http.StatusOK, GetRunResponse{
		Run:    run,
		Events: events,
	})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	workers, err := s.store.ListWorkers(ctx)
	if err != nil {
		s.logger.Printf("list workers failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list workers")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleWorker && principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}

	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	resp, err := s.store.RegisterWorker(ctx, req.ToStoreInput())
	if err != nil {
		s.logger.Printf("register worker failed: %v", err)
		s.metrics.IncCounter("runq_api_worker_register_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to register worker")
		return
	}

	s.metrics.IncCounter("runq_api_workers_registered_total")
	writeJSON(w, http.StatusCreated, RegisterWorkerResponse{
		WorkerID:                  resp.WorkerID,
		HeartbeatIntervalSeconds:  5,
		LeaseRenewIntervalSeconds: 10,
	})
}

func (s *Server) handlePollWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleWorker && principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req PollWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	assignments, err := s.store.PollAssignments(ctx, r.PathValue("workerID"), req.AvailableSlots)
	if err != nil {
		s.logger.Printf("poll worker failed: %v", err)
		s.metrics.IncCounter("runq_api_worker_poll_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to poll assignments")
		return
	}
	s.metrics.AddCounter("runq_api_worker_assignments_returned_total", uint64(len(assignments)))

	response := PollWorkerResponse{Assignments: make([]WorkerAssignmentResponse, 0, len(assignments))}
	for _, assignment := range assignments {
		response.Assignments = append(response.Assignments, WorkerAssignmentResponse{
			RunID:          assignment.RunID,
			JobID:          assignment.JobID,
			Kind:           assignment.Kind,
			Payload:        assignment.Payload,
			TimeoutSeconds: assignment.TimeoutSeconds,
			LeaseToken:     assignment.LeaseToken,
			LeaseExpiresAt: assignment.LeaseExpiry.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleHeartbeatWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleWorker && principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}

	items := make([]store.HeartbeatUpdate, 0, len(req.Running))
	for _, item := range req.Running {
		if item.RunID == "" || item.LeaseToken <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "each running item must include run_id and lease_token")
			return
		}
		items = append(items, store.HeartbeatUpdate{
			RunID:      item.RunID,
			LeaseToken: item.LeaseToken,
			Progress:   item.Progress,
		})
	}

	if err := s.store.HeartbeatWorker(ctx, r.PathValue("workerID"), items, 30*time.Second); err != nil {
		s.logger.Printf("heartbeat worker failed: %v", err)
		if errors.Is(err, store.ErrConflict) {
			s.metrics.IncCounter("runq_api_worker_heartbeat_conflicts_total")
			writeError(w, http.StatusConflict, "LEASE_CONFLICT", "stale worker ownership")
			return
		}
		s.metrics.IncCounter("runq_api_worker_heartbeat_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to record heartbeat")
		return
	}

	s.metrics.IncCounter("runq_api_worker_heartbeats_total")
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCompleteRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleWorker && principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req CompleteRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	err := s.store.CompleteRun(ctx, store.CompleteRunInput{
		WorkerID:   r.PathValue("workerID"),
		RunID:      req.RunID,
		LeaseToken: req.LeaseToken,
		Result:     req.Result,
	})
	if err != nil {
		s.logger.Printf("complete run failed: %v", err)
		if errors.Is(err, store.ErrConflict) {
			s.metrics.IncCounter("runq_api_run_complete_conflicts_total")
			writeError(w, http.StatusConflict, "LEASE_CONFLICT", "stale worker ownership")
			return
		}
		s.metrics.IncCounter("runq_api_run_complete_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to complete run")
		return
	}

	s.metrics.IncCounter("runq_api_runs_completed_total")
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *Server) handleFailRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role != roleWorker && principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker access required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req FailRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	err := s.store.FailRun(ctx, store.FailRunInput{
		WorkerID:     r.PathValue("workerID"),
		RunID:        req.RunID,
		LeaseToken:   req.LeaseToken,
		ErrorCode:    req.ErrorCode,
		ErrorMessage: req.ErrorMessage,
		Retryable:    req.Retryable,
	})
	if err != nil {
		s.logger.Printf("fail run failed: %v", err)
		if errors.Is(err, store.ErrConflict) {
			s.metrics.IncCounter("runq_api_run_fail_conflicts_total")
			writeError(w, http.StatusConflict, "LEASE_CONFLICT", "stale worker ownership")
			return
		}
		s.metrics.IncCounter("runq_api_run_fail_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to fail run")
		return
	}

	s.metrics.IncCounter("runq_api_runs_failed_total")
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *Server) createJob(ctx context.Context, req CreateJobRequest) (CreateJobResponse, error) {
	result, err := s.store.CreateJob(ctx, req.ToStoreInput())
	if err != nil {
		return CreateJobResponse{}, err
	}

	return CreateJobResponse{
		JobID:  result.JobID,
		RunID:  result.RunID,
		Status: "accepted",
	}, nil
}

func authorizedTenantFilter(principal principal, requested string) (string, bool) {
	if principal.Role == roleAdmin || principal.Role == roleWorker {
		return requested, true
	}
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return principal.TenantID, true
	}
	if requested != principal.TenantID {
		return "", false
	}
	return requested, true
}

func canAccessTenant(principal principal, tenantID string) bool {
	if principal.Role == roleAdmin || principal.Role == roleWorker {
		return true
	}
	return principal.TenantID == tenantID
}

func (s *Server) auditInput(principal principal, action, resourceType, resourceID, tenantID string, payload map[string]any) *store.AuditEventInput {
	if principal.Role != roleAdmin {
		return nil
	}

	return &store.AuditEventInput{
		ActorRole:    string(principal.Role),
		ActorID:      auditActorID(principal),
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		TenantID:     tenantID,
		Payload:      payload,
	}
}

func auditActorID(principal principal) string {
	if principal.TenantID != "" {
		return principal.TenantID
	}
	return string(principal.Role)
}
