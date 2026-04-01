package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/eswar/runq/internal/config"
)

type principalRole string

const (
	roleAdmin  principalRole = "admin"
	roleTenant principalRole = "tenant"
	roleWorker principalRole = "worker"
)

type principal struct {
	Role     principalRole
	TenantID string
}

type contextKey string

const principalContextKey contextKey = "principal"

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
		tenantID := ""
		if len(parts) == 3 {
			tenantID = strings.TrimSpace(parts[2])
		}
		if token == "" {
			return nil, fmt.Errorf("empty auth token in entry %q", entry)
		}
		switch role {
		case roleAdmin, roleTenant, roleWorker:
		default:
			return nil, fmt.Errorf("invalid auth role %q", role)
		}
		if role == roleTenant && tenantID == "" {
			return nil, fmt.Errorf("tenant role requires tenant id in entry %q", entry)
		}
		result[token] = principal{
			Role:     role,
			TenantID: tenantID,
		}
	}

	return result, nil
}

func (s *Server) authenticateRequest(w http.ResponseWriter, r *http.Request) (principal, bool) {
	if len(s.authTokens) == 0 {
		return principal{Role: roleAdmin}, true
	}

	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token")
		return principal{}, false
	}

	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	authPrincipal, ok := s.authTokens[token]
	if !ok {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid bearer token")
		return principal{}, false
	}

	return authPrincipal, true
}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalContextKey, p)
}
