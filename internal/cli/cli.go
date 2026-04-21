package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	apiPkg "github.com/eswar/runq/internal/api"
)

type AppConfig struct {
	BaseURL string
	Token   string
}

type App struct {
	cfg    AppConfig
	stdout io.Writer
	stderr io.Writer
	client *http.Client
}

func LoadConfigFromEnv() AppConfig {
	baseURL := strings.TrimSpace(os.Getenv("RUNQ_API_BASE_URL"))
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	return AppConfig{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   strings.TrimSpace(os.Getenv("RUNQ_API_TOKEN")),
	}
}

func New(cfg AppConfig, stdout, stderr io.Writer) *App {
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.BaseURL == "" {
		cfg.BaseURL = "http://localhost:8080"
	}
	return &App{
		cfg:    cfg,
		stdout: stdout,
		stderr: stderr,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *App) Run(args []string) error {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(a.stdout, "usage: runq <auth|config|jobs|runs|workers|quotas> ...")
		return nil
	}
	switch args[0] {
	case "auth":
		return a.runAuth(args[1:])
	case "config":
		return a.runConfig(args[1:])
	case "jobs":
		return a.runJobs(args[1:])
	case "runs", "workers", "quotas":
		_, _ = fmt.Fprintf(a.stdout, "%s commands not implemented yet\n", args[0])
		return nil
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func (a *App) runAuth(args []string) error {
	if len(args) == 1 && args[0] == "me" {
		var resp apiPkg.AuthMeResponse
		if err := a.getJSON(context.Background(), "/v1/auth/me", &resp); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stdout, "role=%s\n", resp.Role)
		if resp.TenantID != nil {
			_, _ = fmt.Fprintf(a.stdout, "tenant_id=%s\n", *resp.TenantID)
		}
		if resp.WorkerName != nil {
			_, _ = fmt.Fprintf(a.stdout, "worker_name=%s\n", *resp.WorkerName)
		}
		return nil
	}
	return errors.New("usage: runq auth me")
}

func (a *App) runConfig(args []string) error {
	if len(args) == 1 && args[0] == "show" {
		_, _ = fmt.Fprintf(a.stdout, "base_url=%s\n", a.cfg.BaseURL)
		_, _ = fmt.Fprintf(a.stdout, "token_set=%t\n", a.cfg.Token != "")
		return nil
	}
	return errors.New("usage: runq config show")
}

func (a *App) runJobs(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: runq jobs <list|get|create|update|disable>")
	}
	switch args[0] {
	case "list":
		query := url.Values{}
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) || !strings.HasPrefix(args[i], "--") {
				return errors.New("usage: runq jobs list [--tenant-id <tenant>] [--queue <queue>]")
			}
			query.Set(strings.ReplaceAll(strings.TrimPrefix(args[i], "--"), "-", "_"), args[i+1])
		}
		var resp apiPkg.ListJobsResponse
		if err := a.getJSON(context.Background(), "/v1/jobs?"+query.Encode(), &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "get":
		if len(args) != 2 {
			return errors.New("usage: runq jobs get <job-id>")
		}
		var resp map[string]any
		if err := a.getJSON(context.Background(), "/v1/jobs/"+args[1], &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "create":
		if len(args) != 2 {
			return errors.New("usage: runq jobs create <json-payload>")
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(args[1]), &payload); err != nil {
			return err
		}
		var resp apiPkg.CreateJobResponse
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs", payload, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "update":
		if len(args) != 3 {
			return errors.New("usage: runq jobs update <job-id> <json-payload>")
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(args[2]), &payload); err != nil {
			return err
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPatch, "/v1/jobs/"+args[1], payload, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "disable":
		if len(args) != 2 {
			return errors.New("usage: runq jobs disable <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/disable", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "enable":
		if len(args) != 2 {
			return errors.New("usage: runq jobs enable <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/enable", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "pause":
		if len(args) != 2 {
			return errors.New("usage: runq jobs pause <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/pause", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "resume":
		if len(args) != 2 {
			return errors.New("usage: runq jobs resume <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/resume", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "trigger":
		if len(args) != 2 {
			return errors.New("usage: runq jobs trigger <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/trigger", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	case "cancel":
		if len(args) != 2 {
			return errors.New("usage: runq jobs cancel <job-id>")
		}
		var resp map[string]any
		if err := a.doJSON(context.Background(), http.MethodPost, "/v1/jobs/"+args[1]+"/cancel", nil, &resp); err != nil {
			return err
		}
		return a.writeJSON(resp)
	default:
		return fmt.Errorf("unknown jobs command: %s", args[0])
	}
}

func (a *App) getJSON(ctx context.Context, path string, dst any) error {
	return a.doRequestJSON(ctx, http.MethodGet, path, nil, dst)
}

func (a *App) doJSON(ctx context.Context, method, path string, body any, dst any) error {
	return a.doRequestJSON(ctx, method, path, body, dst)
}

func (a *App) doRequestJSON(ctx context.Context, method, path string, body any, dst any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.cfg.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.Token)
	}
	resp, err := a.client.Do(req)
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

func (a *App) writeJSON(v any) error {
	enc := json.NewEncoder(a.stdout)
	return enc.Encode(v)
}
