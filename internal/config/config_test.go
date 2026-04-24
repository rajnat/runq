package config

import "testing"

func TestAPIConfigValidateRejectsEmptyAuthTokensByDefault(t *testing.T) {
	cfg := APIConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty auth tokens")
	}
}

func TestAPIConfigValidateAllowsExplicitInsecureDevMode(t *testing.T) {
	cfg := APIConfig{InsecureDevMode: true}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected insecure dev mode to allow empty auth tokens: %v", err)
	}
}
