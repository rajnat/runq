package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/eswar/runq/internal/config"
)

type principalRole string

const (
	roleAdmin  principalRole = "admin"
	roleTenant principalRole = "tenant"
	roleWorker principalRole = "worker"
)

type principal struct {
	Role       principalRole
	TenantID   string
	WorkerName string
}

type contextKey string

const principalContextKey contextKey = "principal"

const workerSessionHeader = "X-Runq-Worker-Session"

func parseAuthTokens(cfg config.APIConfig) (map[string]principal, error) {
	spec := strings.TrimSpace(cfg.AuthTokens)
	if spec == "" {
		return nil, nil
	}

	result := make(map[string]principal)
	entries := strings.Split(spec, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("invalid auth token entry %q", entry)
		}

		token := strings.TrimSpace(parts[0])
		role := principalRole(strings.TrimSpace(parts[1]))
		scope := ""
		if len(parts) == 3 {
			scope = strings.TrimSpace(parts[2])
		}
		if token == "" {
			return nil, fmt.Errorf("empty auth token in entry %q", entry)
		}
		switch role {
		case roleAdmin, roleTenant, roleWorker:
		default:
			return nil, fmt.Errorf("invalid auth role %q", role)
		}
		if role == roleTenant && scope == "" {
			return nil, fmt.Errorf("tenant role requires tenant id in entry %q", entry)
		}
		if role == roleWorker && scope == "" {
			return nil, fmt.Errorf("worker role requires worker name in entry %q", entry)
		}
		p := principal{Role: role}
		switch role {
		case roleTenant:
			p.TenantID = scope
		case roleWorker:
			p.WorkerName = scope
		}
		result[token] = p
	}

	return result, nil
}

func (s *Server) authenticateRequest(w http.ResponseWriter, r *http.Request) (principal, bool) {
	if len(s.authTokens) == 0 && s.cfg.InsecureDevMode {
		return principal{Role: roleAdmin}, true
	}
	if !s.allowSourceRequest(r) {
		s.metrics.IncCounterVec("runq_api_auth_failures_total", map[string]string{"reason": "preauth_rate_limited"})
		writeError(w, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded")
		return principal{}, false
	}

	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		s.metrics.IncCounterVec("runq_api_auth_failures_total", map[string]string{"reason": "missing_bearer_token"})
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token")
		return principal{}, false
	}

	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	authPrincipal, ok := s.authTokens[token]
	if !ok {
		s.metrics.IncCounterVec("runq_api_auth_failures_total", map[string]string{"reason": "invalid_bearer_token"})
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid bearer token")
		return principal{}, false
	}
	if !s.allowAuthenticatedRequest(token, authPrincipal) {
		writeError(w, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded")
		return principal{}, false
	}

	return authPrincipal, true
}

func (s *Server) allowSourceRequest(r *http.Request) bool {
	if s.preAuthLimiter == nil {
		return true
	}
	return s.preAuthLimiter.Allow(sourceRateLimitKey(r), time.Now())
}

func sourceRateLimitKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return host
}

func (s *Server) allowAuthenticatedRequest(token string, principal principal) bool {
	now := time.Now()
	if !s.tokenLimiter.Allow(token, now) {
		return false
	}
	if principal.Role == roleTenant && !s.tenantLimiter.Allow(principal.TenantID, now) {
		return false
	}
	return true
}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalContextKey, p)
}
