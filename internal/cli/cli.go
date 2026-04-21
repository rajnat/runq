package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	case "jobs", "runs", "workers", "quotas":
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

func (a *App) getJSON(ctx context.Context, path string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.BaseURL+path, nil)
	if err != nil {
		return err
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
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}
