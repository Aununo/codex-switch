package hermesaccounts

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"codex-switch/internal/config"
)

func TestUseSwitchesOpenAICodexAndPreservesOtherProviders(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.HermesAccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}

	current := `{
  "version": 1,
  "providers": {
    "nous": {"access_token": "keep"},
    "openai-codex": {"tokens": {"account_id": "acct-old", "access_token": "old-access", "refresh_token": "old-refresh"}}
  },
  "credential_pool": {
    "openai-codex": [{"access_token": "old-access"}],
    "nous": [{"access_token": "keep"}]
  }
}`
	if err := os.WriteFile(paths.HermesAuthFile, []byte(current), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}

	saved := `{
  "version": 1,
  "providers": {
    "openai-codex": {
      "tokens": {"account_id": "acct-new", "access_token": "new-access", "refresh_token": "new-refresh"},
      "last_refresh": "2026-04-26T00:00:00Z",
      "auth_mode": "chatgpt"
    }
  },
  "credential_pool": {
    "openai-codex": [{
      "id": "new",
      "label": "new@example.com",
      "auth_type": "oauth",
      "priority": 0,
      "source": "manual",
      "access_token": "new-access",
      "refresh_token": "new-refresh"
    }]
  }
}`
	if err := os.WriteFile(filepath.Join(paths.HermesAccountsDir, "work.json"), []byte(saved), 0o600); err != nil {
		t.Fatalf("WriteFile saved account: %v", err)
	}

	if err := Use(paths, "work"); err != nil {
		t.Fatalf("Use: %v", err)
	}

	doc := readJSONFile(t, paths.HermesAuthFile)
	if got := nestedString(doc, "providers", "openai-codex", "tokens", "account_id"); got != "acct-new" {
		t.Fatalf("expected switched account id, got %q", got)
	}
	if got := nestedString(doc, "providers", "nous", "access_token"); got != "keep" {
		t.Fatalf("expected non-Codex provider to be preserved, got %q", got)
	}
	if got := nestedString(doc, "credential_pool", "nous", "0", "access_token"); got != "keep" {
		t.Fatalf("expected non-Codex pool to be preserved, got %q", got)
	}
	if got := nestedString(doc, "credential_pool", "openai-codex", "0", "access_token"); got != "new-access" {
		t.Fatalf("expected Codex pool to switch, got %q", got)
	}
	if got := nestedString(doc, "active_provider"); got != providerID {
		t.Fatalf("expected active provider %q, got %q", providerID, got)
	}
	if info, err := os.Stat(paths.HermesAuthFile); err != nil {
		t.Fatalf("stat auth: %v", err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected auth mode 0600, got %o", info.Mode().Perm())
	}
}

func TestSaveRequiresHermesCodexAuth(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.WriteFile(paths.HermesAuthFile, []byte(`{"providers":{"nous":{"access_token":"keep"}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}

	if err := Save(paths, "work", false); err == nil {
		t.Fatal("expected missing Codex provider to fail")
	}
}

func TestSaveRequiresForceForCaseInsensitiveExistingAlias(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.HermesAccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Hermes accounts: %v", err)
	}
	current := `{
  "providers": {
    "openai-codex": {"tokens": {"account_id": "acct-new", "access_token": "new-access", "refresh_token": "new-refresh"}}
  }
}`
	if err := os.WriteFile(paths.HermesAuthFile, []byte(current), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	existing := `{
  "providers": {
    "openai-codex": {"tokens": {"account_id": "acct-old", "access_token": "old-access", "refresh_token": "old-refresh"}}
  }
}`
	existingPath := filepath.Join(paths.HermesAccountsDir, "work.json")
	if err := os.WriteFile(existingPath, []byte(existing), 0o600); err != nil {
		t.Fatalf("WriteFile existing account: %v", err)
	}

	if err := Save(paths, "WORK", false); err == nil {
		t.Fatal("expected case-insensitive existing alias to require --force")
	}

	snapshot, err := ReadSnapshot(existingPath)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snapshot.AccountID != "acct-old" {
		t.Fatalf("expected existing alias to remain unchanged, got %q", snapshot.AccountID)
	}
}

func TestSaveAndCurrentName(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	doc := `{
  "providers": {
    "openai-codex": {
      "tokens": {
        "access_token": "` + tokenWithClaims(map[string]any{
		"email": "work@example.com",
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1",
		},
	}) + `",
        "refresh_token": "refresh"
      }
    }
  }
}`
	if err := os.WriteFile(paths.HermesAuthFile, []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}

	if err := Save(paths, "work", false); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := CurrentName(paths); got != "work" {
		t.Fatalf("expected current account work, got %q", got)
	}
	snapshot, err := ReadSnapshot(filepath.Join(paths.HermesAccountsDir, "work.json"))
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snapshot.Email != "work@example.com" {
		t.Fatalf("expected email from token, got %q", snapshot.Email)
	}
	if snapshot.AccountID != "acct-1" {
		t.Fatalf("expected account id from token, got %q", snapshot.AccountID)
	}
}

func TestUseImportsMatchingCodexSavedAccount(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Codex accounts: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll Hermes auth dir: %v", err)
	}
	current := `{
  "providers": {
    "nous": {"access_token": "keep"},
    "openai-codex": {"tokens": {"account_id": "acct-old", "access_token": "old-access", "refresh_token": "old-refresh"}}
  },
  "credential_pool": {
    "nous": [{"access_token": "keep"}]
  }
}`
	if err := os.WriteFile(paths.HermesAuthFile, []byte(current), 0o600); err != nil {
		t.Fatalf("WriteFile Hermes auth: %v", err)
	}
	codexAccount := `{
  "auth_mode": "chatgpt",
  "tokens": {
    "account_id": "acct-second",
    "access_token": "second-access",
    "refresh_token": "second-refresh"
  },
  "last_refresh": "2026-04-26T00:00:00Z"
}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "second.json"), []byte(codexAccount), 0o600); err != nil {
		t.Fatalf("WriteFile Codex account: %v", err)
	}

	if err := Use(paths, "second"); err != nil {
		t.Fatalf("Use: %v", err)
	}

	doc := readJSONFile(t, paths.HermesAuthFile)
	if got := nestedString(doc, "providers", "openai-codex", "tokens", "account_id"); got != "acct-second" {
		t.Fatalf("expected imported Codex account id, got %q", got)
	}
	if got := nestedString(doc, "providers", "nous", "access_token"); got != "keep" {
		t.Fatalf("expected non-Codex provider to be preserved, got %q", got)
	}
	if got := nestedString(doc, "credential_pool", "openai-codex", "0", "access_token"); got != "second-access" {
		t.Fatalf("expected synthesized Codex pool entry, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(paths.HermesAccountsDir, "second.json")); err != nil {
		t.Fatalf("expected imported Hermes snapshot: %v", err)
	}
}

func TestListAvailableAccountsMergesHermesAndCodexAliases(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.HermesAccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Hermes accounts: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Codex accounts: %v", err)
	}
	hermesDoc := `{
  "providers": {
    "openai-codex": {"tokens": {"account_id": "acct-work", "access_token": "work-access", "refresh_token": "work-refresh"}}
  }
}`
	if err := os.WriteFile(filepath.Join(paths.HermesAccountsDir, "work.json"), []byte(hermesDoc), 0o600); err != nil {
		t.Fatalf("WriteFile Hermes account: %v", err)
	}
	codexDoc := `{
  "tokens": {"account_id": "acct-second", "access_token": "second-access", "refresh_token": "second-refresh"}
}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "second.json"), []byte(codexDoc), 0o600); err != nil {
		t.Fatalf("WriteFile Codex account: %v", err)
	}

	names := ListAvailableAccountNames(paths)
	if len(names) != 2 || names[0] != "second" || names[1] != "work" {
		t.Fatalf("unexpected merged account names: %+v", names)
	}
	entries := ListAvailableAccounts(paths)
	if len(entries) != 2 {
		t.Fatalf("expected two entries, got %+v", entries)
	}
	if entries[0].Name != "second" || entries[0].Imported {
		t.Fatalf("expected second to be sourced from Codex, got %+v", entries[0])
	}
	if entries[1].Name != "work" || !entries[1].Imported {
		t.Fatalf("expected work to be imported Hermes snapshot, got %+v", entries[1])
	}
}

func TestListAvailableAccountsSkipsCodexAliasMissingRefreshToken(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Codex accounts: %v", err)
	}
	codexDoc := `{
  "tokens": {"account_id": "acct-bad", "access_token": "bad-access"}
}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "bad.json"), []byte(codexDoc), 0o600); err != nil {
		t.Fatalf("WriteFile Codex account: %v", err)
	}

	names := ListAvailableAccountNames(paths)
	if len(names) != 0 {
		t.Fatalf("expected no importable Codex aliases, got %+v", names)
	}
	entries := ListAvailableAccounts(paths)
	if len(entries) != 0 {
		t.Fatalf("expected no available Hermes accounts, got %+v", entries)
	}
	if err := Use(paths, "bad"); err == nil {
		t.Fatal("expected direct use of missing-refresh Codex alias to fail")
	}
}

func readJSONFile(t *testing.T, path string) map[string]any {
	t.Helper()
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(bytes, &doc); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	return doc
}

func nestedString(value any, path ...string) string {
	current := value
	for _, part := range path {
		switch typed := current.(type) {
		case map[string]any:
			current = typed[part]
		case []any:
			if part != "0" || len(typed) == 0 {
				return ""
			}
			current = typed[0]
		default:
			return ""
		}
	}
	text, _ := current.(string)
	return text
}

func tokenWithClaims(claims map[string]any) string {
	payload, _ := json.Marshal(claims)
	return "header." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}
