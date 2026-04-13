package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCreatesDefaultConfig(t *testing.T) {
	t.Parallel()

	paths := PathsFromHome(t.TempDir())
	cfg, err := Load(paths)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Refresh.Margin != DefaultRefreshMargin {
		t.Fatalf("expected default margin %q, got %q", DefaultRefreshMargin, cfg.Refresh.Margin)
	}
	if cfg.Network.SubscriptionURL != DefaultSubscriptionURL {
		t.Fatalf("expected default subscription url %q, got %q", DefaultSubscriptionURL, cfg.Network.SubscriptionURL)
	}
	if cfg.Network.RefreshClientID != "" {
		t.Fatalf("expected empty default refresh client id, got %q", cfg.Network.RefreshClientID)
	}
	if _, err := os.Stat(paths.ConfigFile); err != nil {
		t.Fatalf("expected config file to be created: %v", err)
	}
	if info, err := os.Stat(paths.ConfigFile); err != nil {
		t.Fatalf("stat config file: %v", err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected config mode 600, got %o", info.Mode().Perm())
	}
}

func TestLoadMigratesFlatConfig(t *testing.T) {
	t.Parallel()

	paths := PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.ConfigFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(paths.ConfigFile, []byte(`{
  "codex_bin": "/tmp/codex",
  "refresh_margin": "2d",
  "usage_timeout_seconds": 11,
  "max_usage_workers": 3,
  "wham_usage_url": "https://example.com/usage",
  "subscription_url": "https://example.com/subscription",
  "refresh_url": "https://example.com/refresh",
  "refresh_client_id": "client-123",
  "refresh_timeout_seconds": 14
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(paths)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.CodexBin != "/tmp/codex" || cfg.Refresh.Margin != "2d" {
		t.Fatalf("unexpected migrated config: %+v", cfg)
	}
	if cfg.Network.UsageURL != "https://example.com/usage" || cfg.Network.SubscriptionURL != "https://example.com/subscription" || cfg.Network.RefreshClientID != "client-123" {
		t.Fatalf("unexpected network config: %+v", cfg.Network)
	}
}
