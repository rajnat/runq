package runqsdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateJobSendsBearerTokenAndParsesResponse(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CreateJobResponse{JobID: "job-123", Status: "accepted"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-token")
	resp, err := client.CreateJob(context.Background(), map[string]any{"name": "sdk-job"})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if authHeader != "Bearer tenant-token" {
		t.Fatalf("expected bearer token header, got %q", authHeader)
	}
	if resp.JobID != "job-123" || resp.Status != "accepted" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestDoJSONReturnsErrorBodyOnNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-token")
	err := client.doJSON(context.Background(), http.MethodGet, "/v1/jobs", nil, &map[string]any{})
	if err == nil {
		t.Fatal("expected error for non-2xx response")
	}
	if !strings.Contains(err.Error(), "status=429") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected error: %v", err)
	}
}
