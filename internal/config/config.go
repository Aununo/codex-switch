package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codex-switch/internal/support"
)

const (
	DefaultUsageURL            = "https://chatgpt.com/backend-api/wham/usage"
	DefaultSubscriptionURL     = "https://chatgpt.com/backend-api/subscriptions"
	DefaultUsageTimeoutSeconds = 6
	DefaultMaxUsageWorkers     = 8
	DefaultRefreshURL          = "https://auth.openai.com/oauth/token"
	DefaultRefreshTimeout      = 8
	DefaultRefreshMargin       = "5d"
)

type Paths struct {
	HomeDir           string
	CodexDir          string
	AuthFile          string
	AccountsDir       string
	SyncMetaFile      string
	ConfigFile        string
	HermesDir         string
	HermesAuthFile    string
	HermesAccountsDir string
}

type Config struct {
	CodexBin string        `json:"codexBin"`
	Refresh  RefreshConfig `json:"refresh"`
	Network  NetworkConfig `json:"network"`
}

type RefreshConfig struct {
	Margin string `json:"margin"`
}

type NetworkConfig struct {
	UsageURL            string `json:"usageURL"`
	SubscriptionURL     string `json:"subscriptionURL"`
	UsageTimeoutSeconds int    `json:"usageTimeoutSeconds"`
	MaxUsageWorkers     int    `json:"maxUsageWorkers"`
	RefreshURL          string `json:"refreshURL"`
	RefreshClientID     string `json:"refreshClientID,omitempty"`
	RefreshTimeout      int    `json:"refreshTimeoutSeconds"`
}

func DefaultPaths() (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, err
	}
	return PathsFromHome(home), nil
}

func PathsFromHome(home string) Paths {
	codexDir := filepath.Join(home, ".codex")
	hermesDir := hermesDirFromHome(home)
	return Paths{
		HomeDir:           home,
		CodexDir:          codexDir,
		AuthFile:          filepath.Join(codexDir, "auth.json"),
		AccountsDir:       filepath.Join(codexDir, "accounts"),
		SyncMetaFile:      filepath.Join(codexDir, "accounts_sync_meta.json"),
		ConfigFile:        filepath.Join(codexDir, "codex-switch.json"),
		HermesDir:         hermesDir,
		HermesAuthFile:    filepath.Join(hermesDir, "auth.json"),
		HermesAccountsDir: filepath.Join(hermesDir, "accounts"),
	}
}

func hermesDirFromHome(home string) string {
	if value := strings.TrimSpace(os.Getenv("HERMES_HOME")); value != "" {
		if value == "~" {
			return home
		}
		if strings.HasPrefix(value, "~/") {
			return filepath.Join(home, strings.TrimPrefix(value, "~/"))
		}
		if filepath.IsAbs(value) {
			return filepath.Clean(value)
		}
		return filepath.Join(home, value)
	}
	return filepath.Join(home, ".hermes")
}

func DefaultConfig() Config {
	return Config{
		Refresh: RefreshConfig{
			Margin: DefaultRefreshMargin,
		},
		Network: NetworkConfig{
			UsageURL:            DefaultUsageURL,
			SubscriptionURL:     DefaultSubscriptionURL,
			UsageTimeoutSeconds: DefaultUsageTimeoutSeconds,
			MaxUsageWorkers:     DefaultMaxUsageWorkers,
			RefreshURL:          DefaultRefreshURL,
			RefreshTimeout:      DefaultRefreshTimeout,
		},
	}
}

func Load(paths Paths) (Config, error) {
	if err := os.MkdirAll(paths.CodexDir, 0o755); err != nil {
		return Config{}, err
	}

	defaults := DefaultConfig()
	if _, err := os.Stat(paths.ConfigFile); os.IsNotExist(err) {
		if err := write(paths.ConfigFile, defaults); err != nil {
			return Config{}, err
		}
		return defaults, nil
	}

	rawBytes, err := os.ReadFile(paths.ConfigFile)
	if err != nil {
		return Config{}, err
	}

	raw := map[string]any{}
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		return Config{}, err
	}

	migrated, changed, err := migrateConfig(raw, defaults)
	if err != nil {
		return Config{}, err
	}
	if changed {
		if err := write(paths.ConfigFile, migrated); err != nil {
			return Config{}, err
		}
	}

	return migrated, nil
}

func (c Config) RefreshMarginDuration() time.Duration {
	parsed, err := support.ParseFlexibleDuration(c.Refresh.Margin)
	if err != nil {
		fallback, _ := support.ParseFlexibleDuration(DefaultRefreshMargin)
		return fallback
	}
	return parsed
}

func (c Config) UsageTimeoutDuration() time.Duration {
	return time.Duration(c.Network.UsageTimeoutSeconds) * time.Second
}

func (c Config) RefreshTimeoutDuration() time.Duration {
	return time.Duration(c.Network.RefreshTimeout) * time.Second
}

func migrateConfig(raw map[string]any, defaults Config) (Config, bool, error) {
	cfg := defaults
	changed := false

	if value, ok := stringValue(raw["codexBin"]); ok {
		cfg.CodexBin = value
	}
	if value, ok := stringValue(raw["codex_bin"]); ok {
		cfg.CodexBin = value
		changed = true
	}

	if refreshMap, ok := raw["refresh"].(map[string]any); ok {
		if value, ok := stringValue(refreshMap["margin"]); ok {
			cfg.Refresh.Margin = value
		}
	}
	if value, ok := stringValue(raw["refresh_margin"]); ok {
		cfg.Refresh.Margin = value
		changed = true
	}
	if seconds, ok := intValue(raw["refresh_margin_seconds"]); ok {
		cfg.Refresh.Margin = fmt.Sprintf("%ds", seconds)
		changed = true
	}

	if networkMap, ok := raw["network"].(map[string]any); ok {
		if value, ok := stringValue(networkMap["usageURL"]); ok {
			cfg.Network.UsageURL = value
		}
		if value, ok := stringValue(networkMap["subscriptionURL"]); ok {
			cfg.Network.SubscriptionURL = value
		}
		if value, ok := intValue(networkMap["usageTimeoutSeconds"]); ok {
			cfg.Network.UsageTimeoutSeconds = value
		}
		if value, ok := intValue(networkMap["maxUsageWorkers"]); ok {
			cfg.Network.MaxUsageWorkers = value
		}
		if value, ok := stringValue(networkMap["refreshURL"]); ok {
			cfg.Network.RefreshURL = value
		}
		if value, ok := stringValue(networkMap["refreshClientID"]); ok {
			cfg.Network.RefreshClientID = value
		}
		if value, ok := intValue(networkMap["refreshTimeoutSeconds"]); ok {
			cfg.Network.RefreshTimeout = value
		}
	}

	if value, ok := stringValue(raw["wham_usage_url"]); ok {
		cfg.Network.UsageURL = value
		changed = true
	}
	if value, ok := stringValue(raw["subscription_url"]); ok {
		cfg.Network.SubscriptionURL = value
		changed = true
	}
	if value, ok := intValue(raw["usage_timeout_seconds"]); ok {
		cfg.Network.UsageTimeoutSeconds = value
		changed = true
	}
	if value, ok := intValue(raw["max_usage_workers"]); ok {
		cfg.Network.MaxUsageWorkers = value
		changed = true
	}
	if value, ok := stringValue(raw["refresh_url"]); ok {
		cfg.Network.RefreshURL = value
		changed = true
	}
	if value, ok := stringValue(raw["refresh_client_id"]); ok {
		cfg.Network.RefreshClientID = value
		changed = true
	}
	if value, ok := intValue(raw["refresh_timeout_seconds"]); ok {
		cfg.Network.RefreshTimeout = value
		changed = true
	}

	if cfg.Refresh.Margin == "" {
		cfg.Refresh.Margin = defaults.Refresh.Margin
		changed = true
	}
	if cfg.Network.UsageURL == "" {
		cfg.Network.UsageURL = defaults.Network.UsageURL
		changed = true
	}
	if cfg.Network.SubscriptionURL == "" {
		cfg.Network.SubscriptionURL = defaults.Network.SubscriptionURL
		changed = true
	}
	if cfg.Network.UsageTimeoutSeconds <= 0 {
		cfg.Network.UsageTimeoutSeconds = defaults.Network.UsageTimeoutSeconds
		changed = true
	}
	if cfg.Network.MaxUsageWorkers <= 0 {
		cfg.Network.MaxUsageWorkers = defaults.Network.MaxUsageWorkers
		changed = true
	}
	if cfg.Network.RefreshURL == "" {
		cfg.Network.RefreshURL = defaults.Network.RefreshURL
		changed = true
	}
	if cfg.Network.RefreshTimeout <= 0 {
		cfg.Network.RefreshTimeout = defaults.Network.RefreshTimeout
		changed = true
	}

	return cfg, changed, nil
}

func write(path string, cfg Config) error {
	bytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	bytes = append(bytes, '\n')
	return os.WriteFile(path, bytes, 0o600)
}

func stringValue(value any) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	return text, true
}

func intValue(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}
