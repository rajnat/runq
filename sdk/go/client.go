package runqsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

type AuthMeResponse struct {
	Role       string  `json:"role"`
	TenantID   *string `json:"tenant_id,omitempty"`
	WorkerName *string `json:"worker_name,omitempty"`
}

type CreateJobResponse struct {
	JobID  string  `json:"job_id"`
	RunID  *string `json:"run_id,omitempty"`
	Status string  `json:"status"`
}

type PaginationMeta struct {
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Returned   int     `json:"returned"`
	HasMore    bool    `json:"has_more"`
	NextOffset *int    `json:"next_offset,omitempty"`
	NextCursor *string `json:"next_cursor,omitempty"`
}

type Job struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	TenantID  string `json:"tenant_id"`
	Queue     string `json:"queue"`
	Kind      string `json:"kind"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type ListJobsResponse struct {
	Jobs       []Job          `json:"jobs"`
	Pagination PaginationMeta `json:"pagination"`
}

type Run struct {
	ID       string `json:"id"`
	JobID    string `json:"job_id"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
	Attempt  int    `json:"attempt"`
}

type ListRunsResponse struct {
	Runs       []Run          `json:"runs"`
	Pagination PaginationMeta `json:"pagination"`
}

type Worker struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Status         string         `json:"status"`
	MaxConcurrency int            `json:"max_concurrency"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type ListWorkersResponse struct {
	Workers    []Worker       `json:"workers"`
	Pagination PaginationMeta `json:"pagination"`
}

type TenantQuotaResponse struct {
	TenantID       string `json:"tenant_id"`
	MaxInflight    int    `json:"max_inflight"`
	MaxPendingRuns int    `json:"max_pending_runs"`
	MaxActiveJobs  int    `json:"max_active_jobs"`
	UpdatedAt      string `json:"updated_at"`
}

type RegisterWorkerResponse struct {
	WorkerID                  string `json:"worker_id"`
	HeartbeatIntervalSeconds  int    `json:"heartbeat_interval_seconds"`
	LeaseRenewIntervalSeconds int    `json:"lease_renew_interval_seconds"`
}

func NewClient(baseURL, token string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	return &Client{
		baseURL: baseURL,
		token:   strings.TrimSpace(token),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) AuthMe(ctx context.Context) (AuthMeResponse, error) {
	var resp AuthMeResponse
	return resp, c.doJSON(ctx, http.MethodGet, "/v1/auth/me", nil, &resp)
}

func (c *Client) CreateJob(ctx context.Context, payload map[string]any) (CreateJobResponse, error) {
	var resp CreateJobResponse
	return resp, c.doJSON(ctx, http.MethodPost, "/v1/jobs", payload, &resp)
}

func (c *Client) ListJobs(ctx context.Context, params map[string]string) (ListJobsResponse, error) {
	var resp ListJobsResponse
	path := "/v1/jobs"
	if len(params) > 0 {
		q := url.Values{}
		for k, v := range params {
			if strings.TrimSpace(v) != "" {
				q.Set(k, v)
			}
		}
		path += "?" + q.Encode()
	}
	return resp, c.doJSON(ctx, http.MethodGet, path, nil, &resp)
}

func (c *Client) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	var resp map[string]any
	return resp, c.doJSON(ctx, http.MethodGet, "/v1/jobs/"+jobID, nil, &resp)
}

func (c *Client) LookupJobByDedupeKey(ctx context.Context, tenantID, dedupeKey string) (map[string]any, error) {
	q := url.Values{}
	q.Set("tenant_id", tenantID)
	q.Set("dedupe_key", dedupeKey)
	var resp map[string]any
	return resp, c.doJSON(ctx, http.MethodGet, "/v1/jobs/lookup?"+q.Encode(), nil, &resp)
}

func (c *Client) ListRuns(ctx context.Context, params map[string]string) (ListRunsResponse, error) {
	var resp ListRunsResponse
	path := "/v1/runs"
	if len(params) > 0 {
		q := url.Values{}
		for k, v := range params {
			if strings.TrimSpace(v) != "" {
				q.Set(k, v)
			}
		}
		path += "?" + q.Encode()
	}
	return resp, c.doJSON(ctx, http.MethodGet, path, nil, &resp)
}

func (c *Client) ListWorkers(ctx context.Context, params map[string]string) (ListWorkersResponse, error) {
	var resp ListWorkersResponse
	path := "/v1/workers"
	if len(params) > 0 {
		q := url.Values{}
		for k, v := range params {
			if strings.TrimSpace(v) != "" {
				q.Set(k, v)
			}
		}
		path += "?" + q.Encode()
	}
	return resp, c.doJSON(ctx, http.MethodGet, path, nil, &resp)
}

func (c *Client) RegisterWorker(ctx context.Context, payload map[string]any) (RegisterWorkerResponse, error) {
	var resp RegisterWorkerResponse
	return resp, c.doJSON(ctx, http.MethodPost, "/v1/workers/register", payload, &resp)
}

func (c *Client) UpsertTenantQuota(ctx context.Context, tenantID string, payload map[string]any) (TenantQuotaResponse, error) {
	var resp TenantQuotaResponse
	return resp, c.doJSON(ctx, http.MethodPut, "/v1/tenants/"+tenantID+"/quota", payload, &resp)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, dst any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}
