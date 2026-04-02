package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type WorkerProcess struct {
	logger     *log.Logger
	cfg        config.WorkerConfig
	httpClient *http.Client
	metrics    *observability.Registry
	assignWG   sync.WaitGroup

	mu       sync.Mutex
	workerID string
	running  map[string]*activeRun
}

type activeRun struct {
	RunID      string
	LeaseToken int64
	Progress   int
	Aborted    bool
}

type registerWorkerRequest struct {
	Name           string         `json:"name"`
	Queues         []string       `json:"queues"`
	Capabilities   map[string]any `json:"capabilities"`
	MaxConcurrency int            `json:"max_concurrency"`
	Metadata       map[string]any `json:"metadata"`
}

type registerWorkerResponse struct {
	WorkerID                  string `json:"worker_id"`
	HeartbeatIntervalSeconds  int    `json:"heartbeat_interval_seconds"`
	LeaseRenewIntervalSeconds int    `json:"lease_renew_interval_seconds"`
}

type pollWorkerRequest struct {
	AvailableSlots int `json:"available_slots"`
}

type pollWorkerResponse struct {
	Assignments []workerAssignment `json:"assignments"`
}

type workerAssignment struct {
	RunID          string         `json:"run_id"`
	JobID          string         `json:"job_id"`
	Kind           string         `json:"kind"`
	Payload        map[string]any `json:"payload"`
	TimeoutSeconds int            `json:"timeout_seconds"`
	LeaseToken     int64          `json:"lease_token"`
	LeaseExpiresAt string         `json:"lease_expires_at"`
}

type heartbeatRequest struct {
	Running []heartbeatRun `json:"running"`
}

type heartbeatRun struct {
	RunID      string         `json:"run_id"`
	LeaseToken int64          `json:"lease_token"`
	Progress   map[string]any `json:"progress"`
}

type completeRunRequest struct {
	RunID      string         `json:"run_id"`
	LeaseToken int64          `json:"lease_token"`
	Status     string         `json:"status"`
	Result     map[string]any `json:"result"`
}

type failRunRequest struct {
	RunID        string `json:"run_id"`
	LeaseToken   int64  `json:"lease_token"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

func NewWorkerProcess(logger *log.Logger, cfg config.WorkerConfig, metrics *observability.Registry) *WorkerProcess {
	return &WorkerProcess{
		logger: logger,
		cfg:    cfg,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		metrics: metrics,
		running: make(map[string]*activeRun),
	}
}

func (w *WorkerProcess) Run(ctx context.Context) error {
	runCtx, cancelAssignments := context.WithCancel(ctx)
	defer cancelAssignments()

	workerID, err := w.register(ctx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.workerID = workerID
	w.mu.Unlock()

	w.logger.Printf("registered worker_id=%s queues=%v max_concurrency=%d", workerID, w.cfg.Queues, w.cfg.MaxConcurrency)
	w.metrics.IncCounter("runq_worker_registrations_total")
	w.metrics.SetGauge("runq_worker_running_runs", 0)

	pollTicker := time.NewTicker(w.cfg.PollInterval)
	defer pollTicker.Stop()

	heartbeatTicker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			cancelAssignments()
			w.assignWG.Wait()
			return ctx.Err()
		case <-heartbeatTicker.C:
			if err := w.sendHeartbeat(ctx); err != nil {
				w.metrics.IncCounter("runq_worker_heartbeat_errors_total")
				w.logger.Printf("heartbeat failed: %v", err)
			}
		case <-pollTicker.C:
			if err := w.pollAndDispatch(ctx, runCtx); err != nil {
				w.metrics.IncCounter("runq_worker_poll_errors_total")
				w.logger.Printf("poll failed: %v", err)
			}
		}
	}
}

func (w *WorkerProcess) register(ctx context.Context) (string, error) {
	req := registerWorkerRequest{
		Name:           w.cfg.Name,
		Queues:         w.cfg.Queues,
		Capabilities:   capabilityMap(w.cfg.Capabilities),
		MaxConcurrency: w.cfg.MaxConcurrency,
		Metadata:       map[string]any{"runtime": "runq-worker"},
	}

	var resp registerWorkerResponse
	if err := w.doJSON(ctx, http.MethodPost, "/v1/workers/register", req, &resp); err != nil {
		return "", err
	}
	if interval := serverHeartbeatInterval(resp); interval > 0 {
		w.cfg.HeartbeatInterval = interval
	}

	return resp.WorkerID, nil
}

func (w *WorkerProcess) pollAndDispatch(ctx context.Context, runCtx context.Context) error {
	started := time.Now()
	defer w.metrics.ObserveHistogram("runq_worker_poll_duration_seconds", time.Since(started).Seconds())

	workerID := w.getWorkerID()
	if workerID == "" {
		return fmt.Errorf("worker not registered")
	}

	availableSlots := w.cfg.MaxConcurrency - w.runningCount()
	if availableSlots <= 0 {
		return nil
	}

	var resp pollWorkerResponse
	path := fmt.Sprintf("/v1/workers/%s/poll", workerID)
	if err := w.doJSON(ctx, http.MethodPost, path, pollWorkerRequest{AvailableSlots: availableSlots}, &resp); err != nil {
		return err
	}
	w.metrics.AddCounter("runq_worker_assignments_polled_total", uint64(len(resp.Assignments)))

	for _, assignment := range resp.Assignments {
		if !w.markRunning(assignment.RunID, assignment.LeaseToken) {
			continue
		}
		w.logger.Printf("starting run=%s job=%s kind=%s", assignment.RunID, assignment.JobID, assignment.Kind)
		w.metrics.IncCounter("runq_worker_runs_started_total")
		w.assignWG.Add(1)
		go w.executeAssignment(runCtx, assignment)
	}

	return nil
}

func (w *WorkerProcess) executeAssignment(ctx context.Context, assignment workerAssignment) {
	defer w.assignWG.Done()
	defer w.clearRunning(assignment.RunID)
	ctx, span := observability.Tracer("runq/worker").Start(ctx, "worker.execute_assignment")
	defer span.End()
	span.SetAttributes(
		attribute.String("runq.run_id", assignment.RunID),
		attribute.String("runq.job_id", assignment.JobID),
		attribute.String("runq.kind", assignment.Kind),
	)

	steps := int(w.cfg.ExecutionTime / time.Second)
	if steps < 1 {
		steps = 1
	}

	for i := 1; i <= steps; i++ {
		if err := ctx.Err(); err != nil {
			w.metrics.IncCounter("runq_worker_runs_abandoned_total")
			w.logger.Printf("abandoning run=%s due to worker shutdown", assignment.RunID)
			span.SetStatus(codes.Error, "worker shutdown")
			return
		}
		if w.isAborted(assignment.RunID) {
			w.metrics.IncCounter("runq_worker_runs_abandoned_total")
			w.logger.Printf("abandoning run=%s due to lease loss or cancellation", assignment.RunID)
			span.SetStatus(codes.Error, "lease loss or cancellation")
			return
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			w.metrics.IncCounter("runq_worker_runs_abandoned_total")
			w.logger.Printf("abandoning run=%s due to worker shutdown", assignment.RunID)
			span.SetStatus(codes.Error, "worker shutdown")
			return
		case <-timer.C:
		}
		w.setProgress(assignment.RunID, int(float64(i)*100/float64(steps)))
	}

	if w.isAborted(assignment.RunID) {
		w.metrics.IncCounter("runq_worker_runs_abandoned_total")
		w.logger.Printf("abandoning run=%s due to lease loss or cancellation", assignment.RunID)
		span.SetStatus(codes.Error, "lease loss or cancellation")
		return
	}

	if shouldFailAssignment(assignment) {
		req := failRunRequest{
			RunID:        assignment.RunID,
			LeaseToken:   assignment.LeaseToken,
			ErrorCode:    "SIMULATED_FAILURE",
			ErrorMessage: "worker simulated failure",
			Retryable:    true,
		}
		path := fmt.Sprintf("/v1/workers/%s/fail", w.getWorkerID())
		if err := w.doJSON(ctx, http.MethodPost, path, req, nil); err != nil {
			var statusErr *httpStatusError
			if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict {
				w.metrics.IncCounter("runq_worker_runs_abandoned_total")
				w.logger.Printf("abandoning run=%s after fail conflict: %s", assignment.RunID, statusErr.Body)
				span.SetStatus(codes.Error, "fail conflict")
				return
			}
			w.metrics.IncCounter("runq_worker_run_fail_errors_total")
			w.logger.Printf("fail run=%s error=%v", assignment.RunID, err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return
		}
		w.metrics.IncCounter("runq_worker_runs_failed_total")
		w.logger.Printf("failed run=%s retryable=true", assignment.RunID)
		span.SetStatus(codes.Error, "simulated failure")
		return
	}

	req := completeRunRequest{
		RunID:      assignment.RunID,
		LeaseToken: assignment.LeaseToken,
		Status:     "SUCCEEDED",
		Result: map[string]any{
			"status_code": 200,
			"worker":      w.cfg.Name,
		},
	}
	path := fmt.Sprintf("/v1/workers/%s/complete", w.getWorkerID())
	if err := w.doJSON(ctx, http.MethodPost, path, req, nil); err != nil {
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict {
			w.metrics.IncCounter("runq_worker_runs_abandoned_total")
			w.logger.Printf("abandoning run=%s after complete conflict: %s", assignment.RunID, statusErr.Body)
			span.SetStatus(codes.Error, "complete conflict")
			return
		}
		w.metrics.IncCounter("runq_worker_run_complete_errors_total")
		w.logger.Printf("complete run=%s error=%v", assignment.RunID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	w.metrics.IncCounter("runq_worker_runs_completed_total")
	w.logger.Printf("completed run=%s", assignment.RunID)
}

func (w *WorkerProcess) sendHeartbeat(ctx context.Context) error {
	started := time.Now()
	defer w.metrics.ObserveHistogram("runq_worker_heartbeat_duration_seconds", time.Since(started).Seconds())

	workerID := w.getWorkerID()
	if workerID == "" {
		return fmt.Errorf("worker not registered")
	}

	items := w.snapshotRunning()
	path := fmt.Sprintf("/v1/workers/%s/heartbeat", workerID)
	if len(items) == 0 {
		err := w.doJSON(ctx, http.MethodPost, path, heartbeatRequest{Running: nil}, nil)
		if err == nil {
			w.metrics.IncCounter("runq_worker_heartbeats_sent_total")
		}
		return err
	}

	for _, item := range items {
		err := w.doJSON(ctx, http.MethodPost, path, heartbeatRequest{Running: []heartbeatRun{item}}, nil)
		if err == nil {
			w.metrics.IncCounter("runq_worker_heartbeats_sent_total")
			continue
		}

		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict {
			w.abortRun(item.RunID)
			w.metrics.IncCounter("runq_worker_heartbeat_conflicts_total")
			w.logger.Printf("heartbeat conflict for run=%s: %s", item.RunID, statusErr.Body)
			continue
		}
		return err
	}

	return nil
}

type httpStatusError struct {
	StatusCode int
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("unexpected status %d: %s", e.StatusCode, e.Body)
}

func (w *WorkerProcess) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) error {
	ctx, span := observability.Tracer("runq/worker-http").Start(ctx, method+" "+path, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(w.cfg.APIBaseURL, "/")+path, body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(w.cfg.AuthToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	observability.Inject(ctx, req.Header)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	span.SetAttributes(
		attribute.String("http.request.method", method),
		attribute.String("url.path", path),
		attribute.Int("http.response.status_code", resp.StatusCode),
	)

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
		return &httpStatusError{
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(bodyBytes)),
		}
	}

	if responseBody == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func (w *WorkerProcess) getWorkerID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.workerID
}

func (w *WorkerProcess) runningCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.running)
}

func (w *WorkerProcess) markRunning(runID string, leaseToken int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.running[runID]; exists {
		return false
	}
	w.running[runID] = &activeRun{
		RunID:      runID,
		LeaseToken: leaseToken,
		Progress:   0,
	}
	w.metrics.SetGauge("runq_worker_running_runs", int64(len(w.running)))
	return true
}

func (w *WorkerProcess) clearRunning(runID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.running, runID)
	w.metrics.SetGauge("runq_worker_running_runs", int64(len(w.running)))
}

func (w *WorkerProcess) setProgress(runID string, progress int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if item, ok := w.running[runID]; ok {
		item.Progress = progress
	}
}

func (w *WorkerProcess) abortRun(runID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if item, ok := w.running[runID]; ok {
		item.Aborted = true
	}
}

func (w *WorkerProcess) isAborted(runID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	item, ok := w.running[runID]
	return ok && item.Aborted
}

func (w *WorkerProcess) snapshotRunning() []heartbeatRun {
	w.mu.Lock()
	defer w.mu.Unlock()

	items := make([]heartbeatRun, 0, len(w.running))
	for _, item := range w.running {
		items = append(items, heartbeatRun{
			RunID:      item.RunID,
			LeaseToken: item.LeaseToken,
			Progress: map[string]any{
				"percent": item.Progress,
			},
		})
	}
	return items
}

func shouldFailAssignment(assignment workerAssignment) bool {
	if assignment.Payload == nil {
		return false
	}
	if value, ok := assignment.Payload["simulate_failure"].(bool); ok && value {
		return true
	}
	if url, ok := assignment.Payload["url"].(string); ok && strings.Contains(url, "fail") {
		return true
	}
	return false
}

func capabilityMap(items []string) map[string]any {
	capabilities := make(map[string]any, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		capabilities[trimmed] = true
	}
	if len(capabilities) == 0 {
		capabilities["http"] = true
	}
	return capabilities
}

func serverHeartbeatInterval(resp registerWorkerResponse) time.Duration {
	if resp.LeaseRenewIntervalSeconds > 0 {
		return time.Duration(resp.LeaseRenewIntervalSeconds) * time.Second
	}
	if resp.HeartbeatIntervalSeconds > 0 {
		return time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
	}
	return 0
}
