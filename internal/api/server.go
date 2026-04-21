package api

import (
	"context"
	"database/sql"
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

func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{
		Addr:              s.cfg.Address,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	s.logger.Printf("starting api server on %s", s.cfg.Address)
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		err := <-errCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func (s *Server) routes() {
	s.handle("GET /healthz", s.handleHealth)
	s.handle("GET /readyz", s.handleReady)
	s.mux.Handle("/metrics", s.metrics.Handler())
	s.handle("GET /v1/auth/me", s.handleAuthMe)
	s.handle("GET /v1/jobs", s.handleListJobs)
	s.handle("GET /v1/jobs/{jobID}", s.handleGetJob)
	s.handle("PATCH /v1/jobs/{jobID}", s.handleUpdateJob)
	s.handle("POST /v1/jobs/disable", s.handleBulkDisableJobs)
	s.handle("POST /v1/jobs/enable", s.handleBulkEnableJobs)
	s.handle("POST /v1/jobs/pause", s.handleBulkPauseJobs)
	s.handle("POST /v1/jobs/resume", s.handleBulkResumeJobs)
	s.handle("POST /v1/jobs/{jobID}/disable", s.handleDisableJob)
	s.handle("POST /v1/jobs/{jobID}/enable", s.handleEnableJob)
	s.handle("POST /v1/jobs/{jobID}/pause", s.handlePauseJob)
	s.handle("POST /v1/jobs/{jobID}/resume", s.handleResumeJob)
	s.handle("POST /v1/jobs/{jobID}/trigger", s.handleTriggerJob)
	s.handle("POST /v1/jobs/{jobID}/cancel", s.handleCancelJob)
	s.handle("GET /v1/tenants/quotas", s.handleListTenantQuotas)
	s.handle("PUT /v1/tenants/{tenantID}/quota", s.handleUpsertTenantQuota)
	s.handle("GET /v1/audit/events", s.handleListAuditEvents)
	s.handle("GET /v1/runs", s.handleListRuns)
	s.handle("GET /v1/runs/{runID}", s.handleGetRun)
	s.handle("POST /v1/runs/requeue", s.handleBulkRequeueRuns)
	s.handle("POST /v1/runs/redrive", s.handleBulkRedriveRuns)
	s.handle("POST /v1/runs/cancel", s.handleBulkCancelRuns)
	s.handle("POST /v1/runs/{runID}/cancel", s.handleCancelRun)
	s.handle("POST /v1/runs/{runID}/requeue", s.handleRequeueRun)
	s.handle("POST /v1/runs/{runID}/redrive", s.handleRedriveRun)
	s.handle("POST /v1/jobs", s.handleCreateJob)
	s.handle("GET /v1/workers", s.handleListWorkers)
	s.handle("GET /v1/workers/{workerID}", s.handleGetWorker)
	s.handle("POST /v1/workers/{workerID}/drain", s.handleDrainWorker)
	s.handle("POST /v1/workers/{workerID}/reactivate", s.handleReactivateWorker)
	s.handle("POST /v1/workers/{workerID}/decommission", s.handleDecommissionWorker)
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

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "DATABASE_UNAVAILABLE", "database is unavailable")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
	})
}

func (s *Server) handleAuthMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}

	resp := AuthMeResponse{Role: string(principal.Role)}
	if principal.TenantID != "" {
		resp.TenantID = &principal.TenantID
	}
	if principal.WorkerName != "" {
		resp.WorkerName = &principal.WorkerName
	}

	writeJSON(w, http.StatusOK, resp)
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
	paused, err := parseOptionalBool(r.URL.Query().Get("paused"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "paused must be true or false")
		return
	}
	limit, err := parseOptionalInt(r.URL.Query().Get("limit"), 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "limit must be a positive integer")
		return
	}
	offset, err := parseOptionalInt(r.URL.Query().Get("offset"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be zero or greater")
		return
	}
	cursor, err := decodePageCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	createdAfter, err := parseOptionalTime(r.URL.Query().Get("created_after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "created_after must be RFC3339")
		return
	}
	createdBefore, err := parseOptionalTime(r.URL.Query().Get("created_before"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "created_before must be RFC3339")
		return
	}
	updatedAfter, err := parseOptionalTime(r.URL.Query().Get("updated_after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updated_after must be RFC3339")
		return
	}
	updatedBefore, err := parseOptionalTime(r.URL.Query().Get("updated_before"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updated_before must be RFC3339")
		return
	}

	filterTenantID, allowed := authorizedTenantFilter(principal, r.URL.Query().Get("tenant_id"))
	if !allowed {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	jobs, hasMore, nextBoundary, err := s.store.ListJobsPage(ctx, store.JobFilter{
		TenantID:       filterTenantID,
		Queue:          r.URL.Query().Get("queue"),
		Kind:           r.URL.Query().Get("kind"),
		Name:           strings.TrimSpace(r.URL.Query().Get("name")),
		DedupeKey:      strings.TrimSpace(r.URL.Query().Get("dedupe_key")),
		ConcurrencyKey: strings.TrimSpace(r.URL.Query().Get("concurrency_key")),
		CreatedAfter:   createdAfter,
		CreatedBefore:  createdBefore,
		UpdatedAfter:   updatedAfter,
		UpdatedBefore:  updatedBefore,
		Disabled:       disabled,
		Paused:         paused,
		Limit:          limit,
		Offset:         offset,
		Cursor:         cursor,
	})
	if err != nil {
		s.logger.Printf("list jobs failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list jobs")
		return
	}
	nextCursor, err := encodePageCursor(nextBoundary)
	if err != nil {
		s.logger.Printf("encode jobs cursor failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to build jobs pagination")
		return
	}
	pagination := paginationMeta(limit, offset, len(jobs), hasMore)
	pagination.NextCursor = nextCursor

	writeJSON(w, http.StatusOK, ListJobsResponse{
		Jobs: jobs,
		Pagination: pagination,
	})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot get jobs")
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
		s.logger.Printf("get job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) selectJobsForBulkOperation(ctx context.Context, principal principal, req BulkJobOperationRequest) ([]store.Job, error) {
	if len(req.JobIDs) > 0 {
		jobs := make([]store.Job, 0, len(req.JobIDs))
		for _, jobID := range req.JobIDs {
			job, err := s.store.GetJob(ctx, strings.TrimSpace(jobID))
			if err != nil {
				return nil, err
			}
			if !canAccessTenant(principal, job.TenantID) {
				return nil, store.ErrConflict
			}
			jobs = append(jobs, job)
		}
		return jobs, nil
	}
	filterTenantID, allowed := authorizedTenantFilter(principal, req.TenantID)
	if !allowed {
		return nil, store.ErrConflict
	}
	jobs, err := s.store.ListJobs(ctx, store.JobFilter{
		TenantID: filterTenantID,
		Queue:    strings.TrimSpace(req.Queue),
		Kind:     strings.TrimSpace(req.Kind),
		Disabled: req.Disabled,
		Paused:   req.Paused,
		Limit:    200,
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Server) handleBulkDisableJobs(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot disable jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var req BulkJobOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok { return }
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	jobs, err := s.selectJobsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load jobs")
		return
	}
	results := make([]BulkJobOperationItem, 0, len(jobs))
	for _, job := range jobs {
		if req.DryRun {
			item := BulkJobOperationItem{JobID: job.ID}
			if job.DisabledAt == nil {
				item.Status = "would_change"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "JOB_DISABLE_CONFLICT"
				item.ErrorMessage = "job cannot be disabled in its current state"
			}
			results = append(results, item)
			continue
		}
		result, err := s.store.DisableJob(ctx, job.ID, s.auditInput(principal, "JOB_DISABLE", "job", job.ID, job.TenantID, map[string]any{"bulk": true}))
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				results = append(results, BulkJobOperationItem{JobID: job.ID, Status: "skipped", ErrorCode: "JOB_DISABLE_CONFLICT", ErrorMessage: "job cannot be disabled in its current state"})
				continue
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to disable jobs")
			return
		}
		results = append(results, BulkJobOperationItem{JobID: result.JobID, Status: result.Status})
	}
	writeJSON(w, http.StatusOK, BulkJobOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleBulkEnableJobs(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot enable jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var req BulkJobOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok { return }
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	jobs, err := s.selectJobsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load jobs")
		return
	}
	results := make([]BulkJobOperationItem, 0, len(jobs))
	for _, job := range jobs {
		if req.DryRun {
			item := BulkJobOperationItem{JobID: job.ID}
			if job.DisabledAt != nil {
				item.Status = "would_change"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "JOB_ENABLE_CONFLICT"
				item.ErrorMessage = "job cannot be enabled in its current state"
			}
			results = append(results, item)
			continue
		}
		result, err := s.store.EnableJob(ctx, job.ID, s.auditInput(principal, "JOB_ENABLE", "job", job.ID, job.TenantID, map[string]any{"bulk": true}))
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				results = append(results, BulkJobOperationItem{JobID: job.ID, Status: "skipped", ErrorCode: "JOB_ENABLE_CONFLICT", ErrorMessage: "job cannot be enabled in its current state"})
				continue
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to enable jobs")
			return
		}
		results = append(results, BulkJobOperationItem{JobID: result.JobID, Status: result.Status})
	}
	writeJSON(w, http.StatusOK, BulkJobOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleBulkPauseJobs(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot pause jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var req BulkJobOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok { return }
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	jobs, err := s.selectJobsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load jobs")
		return
	}
	results := make([]BulkJobOperationItem, 0, len(jobs))
	for _, job := range jobs {
		if req.DryRun {
			item := BulkJobOperationItem{JobID: job.ID}
			if job.PausedAt == nil && job.DisabledAt == nil {
				item.Status = "would_change"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "JOB_PAUSE_CONFLICT"
				item.ErrorMessage = "job cannot be paused in its current state"
			}
			results = append(results, item)
			continue
		}
		result, err := s.store.PauseJob(ctx, job.ID, s.auditInput(principal, "JOB_PAUSE", "job", job.ID, job.TenantID, map[string]any{"bulk": true}))
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				results = append(results, BulkJobOperationItem{JobID: job.ID, Status: "skipped", ErrorCode: "JOB_PAUSE_CONFLICT", ErrorMessage: "job cannot be paused in its current state"})
				continue
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to pause jobs")
			return
		}
		results = append(results, BulkJobOperationItem{JobID: result.JobID, Status: result.Status})
	}
	writeJSON(w, http.StatusOK, BulkJobOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleBulkResumeJobs(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot resume jobs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var req BulkJobOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok { return }
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	jobs, err := s.selectJobsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load jobs")
		return
	}
	results := make([]BulkJobOperationItem, 0, len(jobs))
	for _, job := range jobs {
		if req.DryRun {
			item := BulkJobOperationItem{JobID: job.ID}
			if job.PausedAt != nil && job.DisabledAt == nil {
				item.Status = "would_change"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "JOB_RESUME_CONFLICT"
				item.ErrorMessage = "job cannot be resumed in its current state"
			}
			results = append(results, item)
			continue
		}
		result, err := s.store.ResumeJob(ctx, job.ID, s.auditInput(principal, "JOB_RESUME", "job", job.ID, job.TenantID, map[string]any{"bulk": true}))
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				results = append(results, BulkJobOperationItem{JobID: job.ID, Status: "skipped", ErrorCode: "JOB_RESUME_CONFLICT", ErrorMessage: "job cannot be resumed in its current state"})
				continue
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to resume jobs")
			return
		}
		results = append(results, BulkJobOperationItem{JobID: result.JobID, Status: result.Status})
	}
	writeJSON(w, http.StatusOK, BulkJobOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleUpdateJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot update jobs")
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
		s.logger.Printf("load job for update failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	var req UpdateJobRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	updated, err := s.store.UpdateJob(ctx, job.ID, store.UpdateJobInput{
		Name:                    req.Name,
		Queue:                   req.Queue,
		Payload:                 req.Payload,
		ScheduleType:            scheduleTypePtr(req.Schedule),
		CronExpr:                scheduleCronPtr(req.Schedule),
		Timezone:                scheduleTimezonePtr(req.Schedule),
		RunAt:                   scheduleRunAtPtr(req.Schedule),
		Priority:                req.Priority,
		MaxRetries:              req.MaxRetries,
		TimeoutSeconds:          req.TimeoutSeconds,
		RetryBackoffBaseSeconds: req.RetryBackoffBaseSeconds,
		ConcurrencyKey:          req.ConcurrencyKey,
	}, s.auditInput(principal, "JOB_UPDATE", "job", job.ID, job.TenantID, nil))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "JOB_UPDATE_CONFLICT", "job cannot be updated in its current state")
			return
		}
		s.logger.Printf("update job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to update job")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"job": updated})
}

func (s *Server) handleDisableJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot disable jobs")
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
		s.logger.Printf("load job for disable failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	result, err := s.store.DisableJob(ctx, job.ID, s.auditInput(principal, "JOB_DISABLE", "job", job.ID, job.TenantID, nil))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "JOB_DISABLE_CONFLICT", "job cannot be disabled in its current state")
			return
		}
		s.logger.Printf("disable job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to disable job")
		return
	}

	writeJSON(w, http.StatusOK, JobLifecycleResponse{JobID: result.JobID, Status: result.Status})
}

func (s *Server) handleEnableJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot enable jobs")
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
		s.logger.Printf("load job for enable failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	result, err := s.store.EnableJob(ctx, job.ID, s.auditInput(principal, "JOB_ENABLE", "job", job.ID, job.TenantID, nil))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "JOB_ENABLE_CONFLICT", "job cannot be enabled in its current state")
			return
		}
		s.logger.Printf("enable job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to enable job")
		return
	}

	writeJSON(w, http.StatusOK, JobLifecycleResponse{JobID: result.JobID, Status: result.Status})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var req CreateJobRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
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
		if errors.Is(err, store.ErrAlreadyExists) {
			writeError(w, http.StatusConflict, "JOB_ALREADY_EXISTS", "an active job with this dedupe key already exists")
			return
		}
		if errors.Is(err, store.ErrQuotaExceeded) {
			writeError(w, http.StatusTooManyRequests, "TENANT_QUOTA_EXCEEDED", "tenant quota exceeded for job admission")
			return
		}
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

func (s *Server) handlePauseJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot pause jobs")
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
		s.logger.Printf("load job for pause failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	result, err := s.store.PauseJob(ctx, job.ID, s.auditInput(principal, "JOB_PAUSE", "job", job.ID, job.TenantID, nil))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "JOB_PAUSE_CONFLICT", "job cannot be paused in its current state")
			return
		}
		s.logger.Printf("pause job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to pause job")
		return
	}

	writeJSON(w, http.StatusOK, JobLifecycleResponse{JobID: result.JobID, Status: result.Status})
}

func (s *Server) handleResumeJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot resume jobs")
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
		s.logger.Printf("load job for resume failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	result, err := s.store.ResumeJob(ctx, job.ID, s.auditInput(principal, "JOB_RESUME", "job", job.ID, job.TenantID, nil))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "JOB_RESUME_CONFLICT", "job cannot be resumed in its current state")
			return
		}
		s.logger.Printf("resume job failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to resume job")
		return
	}

	writeJSON(w, http.StatusOK, JobLifecycleResponse{JobID: result.JobID, Status: result.Status})
}

func (s *Server) handleTriggerJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot trigger jobs")
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
		s.logger.Printf("load job for trigger failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load job")
		return
	}
	if !canAccessTenant(principal, job.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	runID, err := s.store.TriggerJob(ctx, job.ID, s.auditInput(principal, "JOB_TRIGGER", "job", job.ID, job.TenantID, nil))
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, "JOB_TRIGGER_CONFLICT", "job cannot be triggered in its current state")
		case errors.Is(err, store.ErrQuotaExceeded):
			writeError(w, http.StatusTooManyRequests, "TENANT_QUOTA_EXCEEDED", "tenant quota exceeded for pending run admission")
		default:
			s.logger.Printf("trigger job failed: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to trigger job")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, TriggerJobResponse{JobID: job.ID, RunID: runID, Status: "accepted"})
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
			TenantID:       quota.TenantID,
			MaxInflight:    quota.MaxInflight,
			MaxPendingRuns: quota.MaxPendingRuns,
			MaxActiveJobs:  quota.MaxActiveJobs,
			UpdatedAt:      quota.UpdatedAt.Format(time.RFC3339),
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

	limit, err := parseOptionalInt(r.URL.Query().Get("limit"), 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "limit must be a positive integer")
		return
	}
	offset, err := parseOptionalInt(r.URL.Query().Get("offset"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be zero or greater")
		return
	}
	cursor, err := decodePageCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	events, hasMore, nextBoundary, err := s.store.ListAuditEventsPage(ctx, store.AuditEventFilter{
		TenantID:     strings.TrimSpace(r.URL.Query().Get("tenant_id")),
		Action:       strings.TrimSpace(r.URL.Query().Get("action")),
		ResourceType: strings.TrimSpace(r.URL.Query().Get("resource_type")),
		Limit:        limit,
		Offset:       offset,
		Cursor:       cursor,
	})
	if err != nil {
		s.logger.Printf("list audit events failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list audit events")
		return
	}
	nextCursor, err := encodePageCursor(nextBoundary)
	if err != nil {
		s.logger.Printf("encode audit cursor failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to build audit pagination")
		return
	}
	pagination := paginationMeta(limit, offset, len(events), hasMore)
	pagination.NextCursor = nextCursor

	writeJSON(w, http.StatusOK, ListAuditEventsResponse{Events: events, Pagination: pagination})
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
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	tenantID := r.PathValue("tenantID")
	quota, err := s.store.UpsertTenantQuota(ctx, tenantID, req.MaxInflight, req.MaxPendingRuns, req.MaxActiveJobs, s.auditInput(principal, "TENANT_QUOTA_UPSERT", "tenant_quota", tenantID, tenantID, map[string]any{
		"max_inflight":     req.MaxInflight,
		"max_pending_runs": req.MaxPendingRuns,
		"max_active_jobs":  req.MaxActiveJobs,
	}))
	if err != nil {
		s.logger.Printf("upsert tenant quota failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to upsert tenant quota")
		return
	}

	writeJSON(w, http.StatusOK, TenantQuotaResponse{
		TenantID:       quota.TenantID,
		MaxInflight:    quota.MaxInflight,
		MaxPendingRuns: quota.MaxPendingRuns,
		MaxActiveJobs:  quota.MaxActiveJobs,
		UpdatedAt:      quota.UpdatedAt.Format(time.RFC3339),
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
	deadLettered, err := parseOptionalBool(r.URL.Query().Get("dead_lettered"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "dead_lettered must be true or false")
		return
	}
	limit, err := parseOptionalInt(r.URL.Query().Get("limit"), 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "limit must be a positive integer")
		return
	}
	offset, err := parseOptionalInt(r.URL.Query().Get("offset"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be zero or greater")
		return
	}
	cursor, err := decodePageCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	runs, hasMore, nextBoundary, err := s.store.ListRunsPage(ctx, store.RunFilter{
		TenantID:     filterTenantID,
		Statuses:     strings.Split(r.URL.Query().Get("status"), ","),
		Queue:        r.URL.Query().Get("queue"),
		WorkerID:     r.URL.Query().Get("worker_id"),
		JobID:        r.URL.Query().Get("job_id"),
		DeadLettered: deadLettered,
		Limit:        limit,
		Offset:       offset,
		Cursor:       cursor,
	})
	if err != nil {
		s.logger.Printf("list runs failed: %v", err)
		s.metrics.IncCounter("runq_api_run_list_errors_total")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list runs")
		return
	}
	nextCursor, err := encodePageCursor(nextBoundary)
	if err != nil {
		s.logger.Printf("encode runs cursor failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to build runs pagination")
		return
	}
	pagination := paginationMeta(limit, offset, len(runs), hasMore)
	pagination.NextCursor = nextCursor

	writeJSON(w, http.StatusOK, ListRunsResponse{
		Runs: runs,
		Pagination: pagination,
	})
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

func (s *Server) selectRunsForBulkOperation(ctx context.Context, principal principal, req BulkRunOperationRequest) ([]store.Run, error) {
	if len(req.RunIDs) > 0 {
		runs := make([]store.Run, 0, len(req.RunIDs))
		for _, runID := range req.RunIDs {
			run, _, err := s.store.GetRun(ctx, strings.TrimSpace(runID))
			if err != nil {
				return nil, err
			}
			if !canAccessTenant(principal, run.TenantID) {
				return nil, store.ErrConflict
			}
			runs = append(runs, run)
		}
		return runs, nil
	}

	filterTenantID, allowed := authorizedTenantFilter(principal, req.TenantID)
	if !allowed {
		return nil, store.ErrConflict
	}
	runs, err := s.store.ListRuns(ctx, store.RunFilter{
		TenantID:     filterTenantID,
		Statuses:     req.Statuses,
		JobID:        strings.TrimSpace(req.JobID),
		DeadLettered: req.DeadLettered,
		Limit:        200,
	})
	if err != nil {
		return nil, err
	}
	return runs, nil
}

func (s *Server) handleBulkRequeueRuns(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot requeue runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var req BulkRunOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	runs, err := s.selectRunsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load runs")
		return
	}
	results := make([]BulkRunOperationItem, 0, len(runs))
	for _, run := range runs {
		if req.DryRun {
			item := BulkRunOperationItem{FromRun: run.ID}
			if run.Status == "FAILED" || run.Status == "TIMED_OUT" || run.Status == "CANCELED" {
				item.Status = "would_accept"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "RUN_REQUEUE_CONFLICT"
				item.ErrorMessage = "run cannot be requeued in its current state"
			}
			results = append(results, item)
			continue
		}
		newRunID, err := s.store.RequeueRun(ctx, run.ID, s.auditInput(principal, "RUN_REQUEUE", "run", run.ID, run.TenantID, map[string]any{"job_id": run.JobID, "bulk": true}))
		if err != nil {
			switch {
			case errors.Is(err, store.ErrConflict):
				results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "RUN_REQUEUE_CONFLICT", ErrorMessage: "run cannot be requeued in its current state"})
				continue
			case errors.Is(err, store.ErrQuotaExceeded):
				results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "TENANT_QUOTA_EXCEEDED", ErrorMessage: "tenant quota exceeded for pending run admission"})
				continue
			default:
				writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to requeue runs")
				return
			}
		}
		results = append(results, BulkRunOperationItem{FromRun: run.ID, RunID: newRunID, Status: "accepted"})
	}
	statusCode := http.StatusAccepted
	if req.DryRun {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, BulkRunOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleBulkRedriveRuns(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot redrive runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var req BulkRunOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	runs, err := s.selectRunsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load runs")
		return
	}
	results := make([]BulkRunOperationItem, 0, len(runs))
	for _, run := range runs {
		if req.DryRun {
			item := BulkRunOperationItem{FromRun: run.ID}
			if run.DeadLetteredAt == nil {
				item.Status = "would_skip"
				item.ErrorCode = "RUN_NOT_DEAD_LETTERED"
				item.ErrorMessage = "run is not in the dead-letter queue"
			} else {
				item.Status = "would_accept"
			}
			results = append(results, item)
			continue
		}
		if run.DeadLetteredAt == nil {
			results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "RUN_NOT_DEAD_LETTERED", ErrorMessage: "run is not in the dead-letter queue"})
			continue
		}
		newRunID, err := s.store.RequeueRun(ctx, run.ID, s.auditInput(principal, "RUN_REDRIVE", "run", run.ID, run.TenantID, map[string]any{"job_id": run.JobID, "bulk": true}))
		if err != nil {
			switch {
			case errors.Is(err, store.ErrConflict):
				results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "RUN_REDRIVE_CONFLICT", ErrorMessage: "run cannot be redriven in its current state"})
				continue
			case errors.Is(err, store.ErrQuotaExceeded):
				results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "TENANT_QUOTA_EXCEEDED", ErrorMessage: "tenant quota exceeded for pending run admission"})
				continue
			default:
				writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to redrive runs")
				return
			}
		}
		results = append(results, BulkRunOperationItem{FromRun: run.ID, RunID: newRunID, Status: "accepted"})
	}
	statusCode := http.StatusAccepted
	if req.DryRun {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, BulkRunOperationResponse{Count: len(results), Results: results})
}

func (s *Server) handleBulkCancelRuns(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot cancel runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var req BulkRunOperationRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	runs, err := s.selectRunsForBulkOperation(ctx, principal, req)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load runs")
		return
	}
	var canceledSet map[string]struct{}
	if !req.DryRun {
		canceled, err := s.store.CancelRuns(ctx, extractRunIDs(runs), "canceled via bulk api", s.auditInput(principal, "RUN_CANCEL", "run_batch", "bulk", tenantIDForRuns(runs), map[string]any{"count": len(runs)}))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to cancel runs")
			return
		}
		canceledSet = make(map[string]struct{}, len(canceled))
		for _, runID := range canceled {
			canceledSet[runID] = struct{}{}
		}
	}
	results := make([]BulkRunOperationItem, 0, len(runs))
	for _, run := range runs {
		if req.DryRun {
			item := BulkRunOperationItem{FromRun: run.ID}
			if run.Status == "PENDING" || run.Status == "RUNNING" {
				item.Status = "would_cancel"
			} else {
				item.Status = "would_skip"
				item.ErrorCode = "RUN_CANCEL_CONFLICT"
				item.ErrorMessage = "run cannot be canceled in its current state"
			}
			results = append(results, item)
			continue
		}
		if _, ok := canceledSet[run.ID]; ok {
			results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "canceled"})
			continue
		}
		results = append(results, BulkRunOperationItem{FromRun: run.ID, Status: "skipped", ErrorCode: "RUN_CANCEL_CONFLICT", ErrorMessage: "run cannot be canceled in its current state"})
	}
	statusCode := http.StatusOK
	writeJSON(w, statusCode, BulkRunOperationResponse{Count: len(results), Results: results})
}

func extractRunIDs(runs []store.Run) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.ID)
	}
	return ids
}

func tenantIDForRuns(runs []store.Run) string {
	if len(runs) == 0 {
		return ""
	}
	return runs[0].TenantID
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot cancel runs")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	run, _, err := s.store.GetRun(ctx, r.PathValue("runID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load run")
		return
	}
	if !canAccessTenant(principal, run.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}
	canceled, err := s.store.CancelRuns(ctx, []string{run.ID}, "canceled via api", s.auditInput(principal, "RUN_CANCEL", "run", run.ID, run.TenantID, nil))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to cancel run")
		return
	}
	if len(canceled) == 0 {
		writeError(w, http.StatusConflict, "RUN_CANCEL_CONFLICT", "run cannot be canceled in its current state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"from_run": run.ID, "status": "canceled"})
}

func (s *Server) handleRequeueRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot requeue runs")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	run, _, err := s.store.GetRun(ctx, r.PathValue("runID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		s.logger.Printf("load run for requeue failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load run")
		return
	}
	if !canAccessTenant(principal, run.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}

	newRunID, err := s.store.RequeueRun(ctx, run.ID, s.auditInput(principal, "RUN_REQUEUE", "run", run.ID, run.TenantID, map[string]any{
		"job_id": run.JobID,
	}))
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, "RUN_REQUEUE_CONFLICT", "run cannot be requeued in its current state")
		case errors.Is(err, store.ErrQuotaExceeded):
			writeError(w, http.StatusTooManyRequests, "TENANT_QUOTA_EXCEEDED", "tenant quota exceeded for pending run admission")
		default:
			s.logger.Printf("requeue run failed: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to requeue run")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, RequeueRunResponse{
		RunID:   newRunID,
		Status:  "accepted",
		JobID:   run.JobID,
		Source:  "manual_requeue",
		FromRun: run.ID,
	})
}

func (s *Server) handleRedriveRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	if principal.Role == roleWorker {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker principals cannot redrive runs")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	run, _, err := s.store.GetRun(ctx, r.PathValue("runID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
			return
		}
		s.logger.Printf("load run for redrive failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load run")
		return
	}
	if !canAccessTenant(principal, run.TenantID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "tenant access denied")
		return
	}
	if run.DeadLetteredAt == nil {
		writeError(w, http.StatusConflict, "RUN_REDRIVE_CONFLICT", "run is not in the dead-letter queue")
		return
	}

	newRunID, err := s.store.RequeueRun(ctx, run.ID, s.auditInput(principal, "RUN_REDRIVE", "run", run.ID, run.TenantID, map[string]any{
		"job_id": run.JobID,
	}))
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			writeError(w, http.StatusNotFound, "NOT_FOUND", "run not found")
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, "RUN_REDRIVE_CONFLICT", "run cannot be redriven in its current state")
		case errors.Is(err, store.ErrQuotaExceeded):
			writeError(w, http.StatusTooManyRequests, "TENANT_QUOTA_EXCEEDED", "tenant quota exceeded for pending run admission")
		default:
			s.logger.Printf("redrive run failed: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to redrive run")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, RequeueRunResponse{
		RunID:   newRunID,
		Status:  "accepted",
		JobID:   run.JobID,
		Source:  "dead_letter_redrive",
		FromRun: run.ID,
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

	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	if statusFilter != "" && statusFilter != "healthy" && statusFilter != "drained" && statusFilter != "decommissioned" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "status must be one of healthy, drained, or decommissioned")
		return
	}
	queueFilter := strings.TrimSpace(r.URL.Query().Get("queue"))
	capabilityFilter := strings.TrimSpace(r.URL.Query().Get("capability"))

	limit, err := parseOptionalInt(r.URL.Query().Get("limit"), 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "limit must be a positive integer")
		return
	}
	offset, err := parseOptionalInt(r.URL.Query().Get("offset"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "offset must be zero or greater")
		return
	}
	cursor, err := decodePageCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	workers, hasMore, nextBoundary, err := s.store.ListWorkersPage(ctx, store.WorkerFilter{
		Status:     statusFilter,
		Queue:      queueFilter,
		Capability: capabilityFilter,
		Limit:      limit,
		Offset:     offset,
		Cursor:     cursor,
	})
	if err != nil {
		s.logger.Printf("list workers failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to list workers")
		return
	}
	nextCursor, err := encodePageCursor(nextBoundary)
	if err != nil {
		s.logger.Printf("encode workers cursor failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to build workers pagination")
		return
	}
	pagination := paginationMeta(limit, offset, len(workers), hasMore)
	pagination.NextCursor = nextCursor

	writeJSON(w, http.StatusOK, ListWorkersResponse{Workers: workers, Pagination: pagination})
}

func (s *Server) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	worker, err := s.store.GetWorker(ctx, r.PathValue("workerID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load worker")
		return
	}
	inflightRuns, err := s.store.ListRuns(ctx, store.RunFilter{WorkerID: worker.ID, Status: "RUNNING", Limit: worker.MaxConcurrency})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load worker inflight runs")
		return
	}
	writeJSON(w, http.StatusOK, WorkerDetailResponse{Worker: s.workerDetail(worker, inflightRuns)})
}

func (s *Server) workerDetail(worker store.Worker, inflightRuns []store.Run) WorkerDetail {
	detail := WorkerDetail{Worker: worker}
	detail.InflightAssignmentCount = len(inflightRuns)
	if len(inflightRuns) > 0 {
		detail.InflightRuns = make([]WorkerInflightRun, 0, len(inflightRuns))
		for _, run := range inflightRuns {
			detail.InflightRuns = append(detail.InflightRuns, WorkerInflightRun{
				RunID:          run.ID,
				JobID:          run.JobID,
				TenantID:       run.TenantID,
				Queue:          run.Queue,
				Status:         run.Status,
				Attempt:        run.Attempt,
				StartedAt:      run.StartedAt,
				LeaseToken:     run.LeaseToken,
				LeaseExpiresAt: run.LeaseExpiresAt,
			})
		}
	}
	age := time.Since(worker.LastHeartbeatAt)
	if age < 0 {
		age = 0
	}
	heartbeatAgeSeconds := int64(age / time.Second)
	heartbeatDriftSeconds := heartbeatAgeSeconds - int64(s.cfg.WorkerHeartbeatInterval/time.Second)
	if heartbeatDriftSeconds < 0 {
		heartbeatDriftSeconds = 0
	}
	availableCapacity := worker.MaxConcurrency - len(inflightRuns)
	if availableCapacity < 0 {
		availableCapacity = 0
	}
	detail.Health = WorkerHealthSummary{
		HeartbeatAgeSeconds:   heartbeatAgeSeconds,
		HeartbeatDriftSeconds: heartbeatDriftSeconds,
		HeartbeatStale:        heartbeatDriftSeconds > 0,
		InflightAssignments:   len(inflightRuns),
		AvailableCapacity:     availableCapacity,
		AtCapacity:            availableCapacity == 0,
	}
	return detail
}

func (s *Server) handleDrainWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	worker, err := s.store.SetWorkerStatus(ctx, r.PathValue("workerID"), "drained")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to drain worker")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"worker_id": worker.ID, "status": worker.Status})
}

func (s *Server) handleReactivateWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	worker, err := s.store.ReactivateWorker(ctx, r.PathValue("workerID"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return
		}
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "WORKER_REACTIVATE_CONFLICT", "worker can only be reactivated from drained state")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to reactivate worker")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"worker_id": worker.ID, "status": worker.Status})
}

func (s *Server) handleDecommissionWorker(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRequest(w, r)
	if !ok { return }
	if principal.Role != roleAdmin {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "admin access required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	worker, err := s.store.SetWorkerStatus(ctx, r.PathValue("workerID"), "decommissioned")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to decommission worker")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"worker_id": worker.ID, "status": worker.Status})
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
	if ok := decodeJSONBody(w, r, &req); !ok {
		return
	}

	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if principal.Role == roleWorker && req.Name != principal.WorkerName {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker token does not match requested worker identity")
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
		HeartbeatIntervalSeconds:  int(s.cfg.WorkerHeartbeatInterval / time.Second),
		LeaseRenewIntervalSeconds: int(s.cfg.WorkerLeaseDuration / time.Second),
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
	if ok := s.authorizeWorkerIdentity(ctx, w, principal, r.PathValue("workerID")); !ok {
		return
	}

	var req PollWorkerRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
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
	if ok := s.authorizeWorkerIdentity(ctx, w, principal, r.PathValue("workerID")); !ok {
		return
	}

	var req HeartbeatRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
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

	if err := s.store.HeartbeatWorker(ctx, r.PathValue("workerID"), items, s.cfg.WorkerLeaseDuration); err != nil {
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
	if ok := s.authorizeWorkerIdentity(ctx, w, principal, r.PathValue("workerID")); !ok {
		return
	}

	var req CompleteRunRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
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
	if ok := s.authorizeWorkerIdentity(ctx, w, principal, r.PathValue("workerID")); !ok {
		return
	}

	var req FailRunRequest
	if ok := decodeJSONBody(w, r, &req); !ok {
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

func (s *Server) authorizeWorkerIdentity(ctx context.Context, w http.ResponseWriter, principal principal, workerID string) bool {
	if principal.Role != roleWorker {
		return true
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	worker, err := s.store.GetWorker(lookupCtx, workerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return false
		}
		s.logger.Printf("load worker failed: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to load worker")
		return false
	}
	if worker.Name != principal.WorkerName {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "worker token does not match requested worker identity")
		return false
	}
	return true
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
