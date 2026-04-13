package accounts

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-switch/internal/auth"
	"codex-switch/internal/config"
)

func TestSaveUseRenameDeleteAndDetectCurrent(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	authDoc := []byte(`{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`)
	if err := os.WriteFile(paths.AuthFile, authDoc, 0o644); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	if err := Save(paths, "work", false); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.AccountsDir, "work.json")); err != nil {
		t.Fatalf("expected saved account file: %v", err)
	}
	if info, err := os.Stat(filepath.Join(paths.AccountsDir, "work.json")); err != nil {
		t.Fatalf("stat saved account: %v", err)
	} else if info.Mode().Perm() != secureFileMode {
		t.Fatalf("expected secure mode %o, got %o", secureFileMode, info.Mode().Perm())
	}
	if got := DetectCurrentAccountName(paths); got != "work" {
		t.Fatalf("expected current account work, got %q", got)
	}

	if err := Rename(paths, "work", "personal"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if err := Use(paths, "personal"); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if err := Delete(paths, "personal"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if names := ListAccountNames(paths); len(names) != 0 {
		t.Fatalf("expected no saved accounts, got %v", names)
	}
}

func TestDeleteMissingAccountReturnsError(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	if err := Delete(paths, "missing"); err == nil {
		t.Fatalf("expected error when deleting missing account")
	}
}

func TestRenameAllowsCaseOnlyChange(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}
	source := filepath.Join(paths.AccountsDir, "hxc.json")
	if err := os.WriteFile(source, []byte(`{"tokens":{"account_id":"acct-1"}}`), 0o600); err != nil {
		t.Fatalf("write account: %v", err)
	}

	if err := Rename(paths, "hxc", "Hxc"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.AccountsDir, "Hxc.json")); err != nil {
		t.Fatalf("expected renamed file: %v", err)
	}
	names := ListAccountNames(paths)
	if len(names) != 1 || names[0] != "Hxc" {
		t.Fatalf("expected only renamed account Hxc, got %v", names)
	}
}

func TestUsePreservesCanonicalAliasCaseOnCaseInsensitiveLookup(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir auth dir: %v", err)
	}
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"acct-old"}}`), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "Hxc.json"), []byte(`{"tokens":{"account_id":"acct-1"}}`), 0o600); err != nil {
		t.Fatalf("write account: %v", err)
	}

	if err := Use(paths, "hxc"); err != nil {
		t.Fatalf("Use: %v", err)
	}

	if got := DetectCurrentAccountName(paths); got != "Hxc" {
		t.Fatalf("expected canonical alias Hxc, got %q", got)
	}
}

func TestRejectsInvalidAccountNames(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"acct-1"}}`), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	for _, name := range []string{"../evil", "bad/name", "bad\\name", "has space"} {
		if err := Save(paths, name, false); err == nil {
			t.Fatalf("expected invalid name error for %q", name)
		}
	}
}

func TestPruneAndRecordLastChecked(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	content := []byte(`{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`)
	for _, name := range []string{"one", "two"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), content, 0o644); err != nil {
			t.Fatalf("write account: %v", err)
		}
	}
	if err := RecordLastChecked(paths, []string{"one", "two"}, time.Unix(200, 0)); err != nil {
		t.Fatalf("RecordLastChecked: %v", err)
	}

	pairs, err := Prune(paths, true)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected one prune pair, got %d", len(pairs))
	}
}

func TestSyncSavedAliasesUpdatesAllMatchingAliases(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	current := []byte(`{"tokens":{"account_id":"acct-1","access_token":"current","id_token":"current"}}`)
	old := []byte(`{"tokens":{"account_id":"acct-1","access_token":"old","id_token":"old"}}`)
	if err := os.WriteFile(paths.AuthFile, current, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	for _, name := range []string{"one", "two"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), old, 0o600); err != nil {
			t.Fatalf("write account: %v", err)
		}
	}

	updated, checked, warnings := SyncSavedAliases(paths)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	if len(updated) != 2 || len(checked) != 2 {
		t.Fatalf("expected all aliases to sync, updated=%v checked=%v", updated, checked)
	}
	for _, name := range []string{"one", "two"} {
		doc, err := auth.LoadDocument(filepath.Join(paths.AccountsDir, name+".json"))
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if auth.Tokens(doc)["access_token"] != "current" {
			t.Fatalf("expected alias %s to be refreshed", name)
		}
	}
}

func TestSyncSavedAliasesReportsCopyFailures(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	current := []byte(`{"tokens":{"account_id":"acct-1","access_token":"current","id_token":"current"}}`)
	old := []byte(`{"tokens":{"account_id":"acct-1","access_token":"old","id_token":"old"}}`)
	if err := os.WriteFile(paths.AuthFile, current, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	target := filepath.Join(paths.AccountsDir, "one.json")
	if err := os.WriteFile(target, old, 0o400); err != nil {
		t.Fatalf("write account: %v", err)
	}

	updated, checked, warnings := SyncSavedAliases(paths)
	if len(updated) != 0 {
		t.Fatalf("expected no successful updates, got %v", updated)
	}
	if len(checked) != 0 {
		t.Fatalf("expected failed alias sync not to be recorded as checked, got %v", checked)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "sync failed") {
		t.Fatalf("expected sync failure warning, got %v", warnings)
	}
}

func TestDetectCurrentAccountNameUsesInferredAccountID(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	currentDoc := []byte(`{"tokens":{"id_token":"` + tokenWithClaims(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1",
		},
	}) + `"}}`)
	savedDoc := []byte(`{"tokens":{"id_token":"` + tokenWithClaims(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1",
		},
	}) + `"}}`)

	if err := os.WriteFile(paths.AuthFile, currentDoc, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), savedDoc, 0o600); err != nil {
		t.Fatalf("write account: %v", err)
	}

	if got := DetectCurrentAccountName(paths); got != "work" {
		t.Fatalf("expected inferred current account work, got %q", got)
	}
}

func TestDetectCurrentAccountNamePrefersRecordedCurrentAlias(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	doc := []byte(`{"tokens":{"account_id":"acct-1","access_token":"same","id_token":"same"}}`)
	if err := os.WriteFile(paths.AuthFile, doc, 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	for _, name := range []string{"alpha", "zeta"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), doc, 0o600); err != nil {
			t.Fatalf("write account %s: %v", name, err)
		}
	}
	if err := SetCurrentAlias(paths, "zeta"); err != nil {
		t.Fatalf("SetCurrentAlias: %v", err)
	}

	if got := DetectCurrentAccountName(paths); got != "zeta" {
		t.Fatalf("expected recorded current alias zeta, got %q", got)
	}
}

func TestPruneReportsDeleteFailures(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	content := []byte(`{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`)
	for _, name := range []string{"one", "two"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), content, 0o600); err != nil {
			t.Fatalf("write account: %v", err)
		}
	}
	if err := RecordLastChecked(paths, []string{"one", "two"}, time.Unix(300, 0)); err != nil {
		t.Fatalf("RecordLastChecked: %v", err)
	}
	if err := os.Chmod(paths.AccountsDir, 0o500); err != nil {
		t.Fatalf("chmod accounts dir: %v", err)
	}
	defer os.Chmod(paths.AccountsDir, 0o755)

	pairs, err := Prune(paths, true)
	if err == nil {
		t.Fatalf("expected prune delete failure")
	}
	if len(pairs) != 1 {
		t.Fatalf("expected one prune pair, got %d", len(pairs))
	}
	meta := loadSyncMeta(paths)
	if _, ok := meta.LastChecked["one"]; !ok {
		t.Fatalf("expected metadata for one to remain after prune failure")
	}
	if _, ok := meta.LastChecked["two"]; !ok {
		t.Fatalf("expected metadata for failed removal to remain after prune failure")
	}
}

func TestBuildListRowsIncludesSubscriptionInfo(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	doc := `{"tokens":{"account_id":"acct-1","access_token":"token-1","id_token":"` + tokenWithClaims(map[string]any{
		"https://api.openai.com/profile": map[string]any{
			"email": "saved@example.com",
		},
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1",
			"chatgpt_plan_type":  "plus",
		},
	}) + `"}}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(doc), 0o600); err != nil {
		t.Fatalf("write account: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/usage":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "live@example.com",
				"plan_type": "plus",
				"rate_limit": map[string]any{
					"allowed": true,
				},
			})
		case "/subscription":
			if got := r.URL.Query().Get("account_id"); got != "acct-1" {
				t.Fatalf("expected account_id acct-1, got %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"plan_type":    "plus",
				"active_until": "2026-04-14T11:17:05Z",
				"will_renew":   true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.Network.UsageURL = server.URL + "/usage"
	cfg.Network.SubscriptionURL = server.URL + "/subscription"
	rows := BuildListRows(paths, cfg, server.Client(), false, time.Unix(1_700_000_000, 0))
	if len(rows.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows.Rows))
	}
	if rows.Rows[0].Plan != "plus renews 2026-04-14" {
		t.Fatalf("expected subscription info in plan, got %+v", rows.Rows[0])
	}
	if rows.Rows[0].Account != "work <live@example.com>" {
		t.Fatalf("expected live email to be used, got %+v", rows.Rows[0])
	}
}

func TestBuildListRowsKeepsUsagePlanWhenSubscriptionSummaryIsSparse(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := EnsureAccountsDir(paths); err != nil {
		t.Fatalf("EnsureAccountsDir: %v", err)
	}

	doc := `{"tokens":{"account_id":"acct-1","access_token":"token-1","id_token":"` + tokenWithClaims(map[string]any{
		"https://api.openai.com/profile": map[string]any{
			"email": "saved@example.com",
		},
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1",
			"chatgpt_plan_type":  "plus",
		},
	}) + `"}}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(doc), 0o600); err != nil {
		t.Fatalf("write account: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/usage":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "live@example.com",
				"plan_type": "plus",
				"rate_limit": map[string]any{
					"allowed": true,
				},
			})
		case "/subscription":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"will_renew": true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.Network.UsageURL = server.URL + "/usage"
	cfg.Network.SubscriptionURL = server.URL + "/subscription"
	rows := BuildListRows(paths, cfg, server.Client(), false, time.Unix(1_700_000_000, 0))
	if len(rows.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows.Rows))
	}
	if rows.Rows[0].Plan != "plus" {
		t.Fatalf("expected usage plan to be preserved, got %+v", rows.Rows[0])
	}
}

func tokenWithClaims(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payloadBytes, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	return header + "." + payload + ".sig"
}
