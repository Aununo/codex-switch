package cli

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-switch/internal/config"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

func writeActiveThreadFixtures(t *testing.T, paths config.Paths, now time.Time, sessionID, threadName string) {
	t.Helper()

	stateDBPath := filepath.Join(paths.CodexDir, "state_5.sqlite")
	if err := os.MkdirAll(filepath.Dir(stateDBPath), 0o755); err != nil {
		t.Fatalf("MkdirAll state db dir: %v", err)
	}

	db, err := sql.Open("sqlite", stateDBPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE threads (
			id TEXT PRIMARY KEY,
			rollout_path TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			source TEXT NOT NULL,
			model_provider TEXT NOT NULL,
			cwd TEXT NOT NULL,
			title TEXT NOT NULL,
			sandbox_policy TEXT NOT NULL,
			approval_mode TEXT NOT NULL,
			tokens_used INTEGER NOT NULL DEFAULT 0,
			has_user_event INTEGER NOT NULL DEFAULT 0,
			archived INTEGER NOT NULL DEFAULT 0
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE threads: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO threads (
			id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
			sandbox_policy, approval_mode, tokens_used, has_user_event, archived
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		sessionID,
		filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01", "rollout-2026-04-01T18-00-00-"+sessionID+".jsonl"),
		now.Unix(),
		now.Unix(),
		"vscode",
		"openai",
		paths.HomeDir,
		threadName,
		"workspace-write",
		"default",
		0,
		1,
		0,
	); err != nil {
		t.Fatalf("INSERT threads: %v", err)
	}
}

func mustParseRootTestTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("time.Parse: %v", err)
	}
	return parsed
}

func writeFakeAppServer(t *testing.T, dir string, payload string) string {
	t.Helper()

	pythonPath := filepath.Join(dir, "fake-codex.py")
	pythonCode := fmt.Sprintf(`import json
import sys

payload = %q

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    method = req.get("method")
    req_id = req.get("id")
    if method == "initialize":
        print(json.dumps({"id": req_id, "result": {"ok": True}}), flush=True)
    elif method == "thread/list":
        print(json.dumps({"id": req_id, "result": json.loads(payload)}), flush=True)
`, payload)
	if err := os.WriteFile(pythonPath, []byte(pythonCode), 0o755); err != nil {
		t.Fatalf("WriteFile fake app-server python: %v", err)
	}

	path := filepath.Join(dir, "fake-codex")
	script := fmt.Sprintf(`#!/bin/sh
exec python3 %q
`, pythonPath)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile fake app-server: %v", err)
	}
	return path
}

func TestCompletionCommandGeneratesZshScript(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	if err := cmd.GenZshCompletion(&out); err != nil {
		t.Fatalf("GenZshCompletion: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("_codex-switch")) {
		t.Fatalf("expected zsh completion output, got %q", out.String())
	}
}

func TestHelpDoesNotNeedRuntimePreparation(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"-h"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("Codex Switch CLI")) {
		t.Fatalf("unexpected help output: %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("[OPTIONS]")) {
		t.Fatalf("unexpected help output: %q", out.String())
	}
}

func TestSubcommandHelpUsesRequiredArgumentSyntax(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}

	cases := []struct {
		args   []string
		expect string
	}{
		{args: []string{"use", "-h"}, expect: "use <name>"},
		{args: []string{"threads", "-h"}, expect: "threads"},
		{args: []string{"rename", "-h"}, expect: "rename <old-name> <new-name>"},
		{args: []string{"delete", "-h"}, expect: "delete <name>"},
		{args: []string{"install-completion", "-h"}, expect: "install-completion <zsh|bash>"},
	}

	for _, tc := range cases {
		cmd := app.newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(tc.args)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute(%v): %v", tc.args, err)
		}
		if !bytes.Contains(out.Bytes(), []byte(tc.expect)) {
			t.Fatalf("expected help for %v to contain %q, got %q", tc.args, tc.expect, out.String())
		}
	}
}

func TestRootHelpShowsCommonCommandsFirst(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Client:         &http.Client{},
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"-h"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	output := out.String()
	loginIndex := bytes.Index(out.Bytes(), []byte("\n  login"))
	listIndex := bytes.Index(out.Bytes(), []byte("\n  list"))
	completionIndex := bytes.Index(out.Bytes(), []byte("\n  completion"))
	if loginIndex == -1 || listIndex == -1 || completionIndex == -1 {
		t.Fatalf("missing expected commands in help output: %q", output)
	}
	if completionIndex < loginIndex || completionIndex < listIndex {
		t.Fatalf("expected common commands before completion, got %q", output)
	}
}

func TestCompleteAccountNamesStopsAfterFirstArg(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	for _, name := range []string{"work", "home"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), []byte(`{"tokens":{"account_id":"acct"}}`), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	completer := completeAccountNames(paths)

	firstArg, directive := completer(nil, nil, "wo")
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive for first arg: %v", directive)
	}
	if len(firstArg) != 1 || firstArg[0] != "work" {
		t.Fatalf("unexpected first-arg completions: %v", firstArg)
	}

	secondArg, directive := completer(nil, []string{"work"}, "")
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive for second arg: %v", directive)
	}
	if len(secondArg) != 0 {
		t.Fatalf("expected no second-arg completions, got %v", secondArg)
	}
}

func TestInstallCompletionWritesZshScript(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Client:         &http.Client{},
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"install-completion", "zsh"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	target := paths.HomeDir + "/.zsh/completions/_codex-switch"
	content, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Contains(content, []byte("_codex-switch")) {
		t.Fatalf("unexpected script content: %q", string(content))
	}
	if !bytes.Contains(out.Bytes(), []byte("Completion installed")) {
		t.Fatalf("unexpected output: %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("shell: zsh")) {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestListStreamsRowsAsRequestsFinish(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}

	for name, token := range map[string]string{
		"a-slow": "slow-token",
		"b-fast": "fast-token",
	} {
		content := fmt.Sprintf(`{"tokens":{"account_id":"acct-%s","access_token":"%s","id_token":"id-%s"}}`, name, token, name)
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
		case "slow-token":
			time.Sleep(120 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "slow@example.com",
				"plan_type": "pro",
				"rate_limit": map[string]any{
					"allowed": true,
				},
			})
		case "fast-token":
			time.Sleep(10 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "fast@example.com",
				"plan_type": "plus",
				"rate_limit": map[string]any{
					"allowed": true,
				},
			})
		default:
			http.Error(w, "unknown token", http.StatusUnauthorized)
		}
	}))
	defer server.Close()

	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	cfg.Network.UsageURL = server.URL
	cfg.Network.MaxUsageWorkers = 2

	app := &App{
		Paths:          paths,
		Client:         server.Client(),
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(1_700_000_000, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"list"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	output := out.String()
	fastIndex := strings.Index(output, "b-fast <fast@example.com>")
	slowIndex := strings.Index(output, "a-slow <slow@example.com>")
	if fastIndex == -1 || slowIndex == -1 {
		t.Fatalf("expected both streamed rows in output, got %q", output)
	}
	if fastIndex > slowIndex {
		t.Fatalf("expected fast row before slow row, got %q", output)
	}
}

func TestTokenInfoShowsRefreshTokenDetails(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	doc := `{"tokens":{"id_token":"header.payload.sig","access_token":"header.payload.sig","refresh_token":"opaque-refresh-token"}}`
	if err := os.WriteFile(paths.AuthFile, []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"token-info"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !bytes.Contains(out.Bytes(), []byte("refresh_token")) {
		t.Fatalf("expected refresh_token row in output, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("refresh token type: opaque")) {
		t.Fatalf("expected refresh token type details, got %q", out.String())
	}
}

func TestDeletePromptsBeforeRemovingAccount(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(paths.AccountsDir, "work.json")
	if err := os.WriteFile(target, []byte(`{"tokens":{"account_id":"acct-1"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("y\n"))
	cmd.SetArgs([]string{"delete", "work"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("expected account to be removed, stat err=%v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`Delete saved account "work"?`)) {
		t.Fatalf("unexpected prompt output: %q", out.String())
	}
}

func TestPruneApplyPromptsBeforeDeletingDuplicates(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := []byte(`{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`)
	for _, name := range []string{"one", "two"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), content, 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("yes\n"))
	cmd.SetArgs([]string{"prune", "--apply"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(paths.AccountsDir, "*.json"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected one account file after prune, got %d (%v)", len(files), files)
	}
	if !bytes.Contains(out.Bytes(), []byte("Delete 1 duplicate saved account(s)?")) {
		t.Fatalf("unexpected prompt output: %q", out.String())
	}
}

func TestPruneApplyStillShowsResultsWhenDeleteFails(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := []byte(`{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`)
	for _, name := range []string{"one", "two"} {
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), content, 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}
	if err := os.Chmod(paths.AccountsDir, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	defer os.Chmod(paths.AccountsDir, 0o755)

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("yes\n"))
	cmd.SetArgs([]string{"prune", "--apply"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("expected prune apply error")
	}
	if !bytes.Contains(out.Bytes(), []byte("Prune partially applied")) {
		t.Fatalf("expected partial-apply warning, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("KEEP")) || !bytes.Contains(out.Bytes(), []byte("REMOVE")) {
		t.Fatalf("expected prune result table, got %q", out.String())
	}
}

func TestDeleteWithoutNameFailsBeforePrompt(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"delete"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("expected missing arg error")
	}
	if bytes.Contains(out.Bytes(), []byte("Delete saved account")) {
		t.Fatalf("delete prompt should not appear when arg is missing: %q", out.String())
	}
}

func TestSaveWithoutNameFailsAtCLIArgumentValidation(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"save"})
	err = cmd.Execute()
	if err == nil {
		t.Fatalf("expected missing arg error")
	}
	if strings.Contains(err.Error(), "Please provide an account name.") {
		t.Fatalf("expected Cobra arg validation error, got business-layer error: %v", err)
	}
}

func TestPersistentPreRunReportsRefreshWarnings(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	doc := `{"tokens":{"account_id":"acct-1","access_token":"bad-access-token","refresh_token":""}}`
	if err := os.WriteFile(paths.AuthFile, []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: true,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"token-info"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !bytes.Contains(errOut.Bytes(), []byte("startup refresh skipped")) {
		t.Fatalf("expected startup refresh warning, got stdout=%q stderr=%q", out.String(), errOut.String())
	}
}

func TestLoginReportsAliasSyncWarnings(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}
	target := filepath.Join(paths.AccountsDir, "work.json")
	if err := os.WriteFile(target, []byte(`{"tokens":{"account_id":"acct-1","access_token":"old","id_token":"old"}}`), 0o400); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	loginScript := filepath.Join(paths.HomeDir, "fake-codex")
	script := "#!/bin/sh\n" +
		"mkdir -p '" + filepath.Dir(paths.AuthFile) + "'\n" +
		"cat > '" + paths.AuthFile + "' <<'EOF'\n" +
		"{\"tokens\":{\"account_id\":\"acct-1\",\"access_token\":\"new\",\"id_token\":\"new\"}}\n" +
		"EOF\n"
	if err := os.WriteFile(loginScript, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile script: %v", err)
	}
	cfg.CodexBin = loginScript

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"login"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("sync failed")) {
		t.Fatalf("expected login to surface sync warning, got %q", out.String())
	}
}

func TestSyncAllRecordsOnlySuccessfulChecks(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode refresh payload: %v", err)
			}
			switch payload["refresh_token"] {
			case "current-rt", "good-rt":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token":  "refreshed-token",
					"id_token":      "refreshed-id",
					"refresh_token": payload["refresh_token"],
				})
			case "bad-rt":
				http.Error(w, "bad refresh", http.StatusBadRequest)
			default:
				http.Error(w, "unexpected refresh token", http.StatusBadRequest)
			}
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "test@example.com",
				"plan_type": "plus",
				"rate_limit": map[string]any{
					"allowed": true,
					"primary_window": map[string]any{
						"used_percent": 10,
						"reset_at":     float64(200),
					},
					"secondary_window": map[string]any{
						"used_percent": 30,
						"reset_at":     float64(400),
					},
				},
			})
		default:
			http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	cfg.Network.RefreshURL = server.URL
	cfg.Network.UsageURL = server.URL
	cfg.Network.RefreshClientID = "test-client"

	currentDoc := `{"tokens":{"account_id":"acct-current","access_token":"current-token","refresh_token":"current-rt"}}`
	if err := os.WriteFile(paths.AuthFile, []byte(currentDoc), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}

	goodDoc := `{"tokens":{"account_id":"acct-good","access_token":"good-token","refresh_token":"good-rt"}}`
	badDoc := `{"tokens":{"account_id":"acct-bad","access_token":"bad-token","refresh_token":"bad-rt"}}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "good.json"), []byte(goodDoc), 0o600); err != nil {
		t.Fatalf("WriteFile good alias: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "bad.json"), []byte(badDoc), 0o600); err != nil {
		t.Fatalf("WriteFile bad alias: %v", err)
	}

	app := &App{
		Paths:          paths,
		Client:         server.Client(),
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"sync", "--all", "--force"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	metaBytes, err := os.ReadFile(paths.SyncMetaFile)
	if err != nil {
		t.Fatalf("ReadFile sync meta: %v", err)
	}
	if !bytes.Contains(metaBytes, []byte(`"good"`)) {
		t.Fatalf("expected successful alias to be recorded, got %q", string(metaBytes))
	}
	if bytes.Contains(metaBytes, []byte(`"bad"`)) {
		t.Fatalf("expected failed alias not to be recorded, got %q", string(metaBytes))
	}
}

func TestSyncAllStreamsAliasProgressAsRefreshesFinish(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode refresh payload: %v", err)
			}
			switch payload["refresh_token"] {
			case "current-rt":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token":  "current-refreshed",
					"id_token":      "current-id",
					"refresh_token": payload["refresh_token"],
				})
			case "fast-rt":
				time.Sleep(10 * time.Millisecond)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token":  "fast-refreshed",
					"id_token":      "fast-id",
					"refresh_token": payload["refresh_token"],
				})
			case "slow-rt":
				time.Sleep(120 * time.Millisecond)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token":  "slow-refreshed",
					"id_token":      "slow-id",
					"refresh_token": payload["refresh_token"],
				})
			default:
				http.Error(w, "unexpected refresh token", http.StatusBadRequest)
			}
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"email":     "test@example.com",
				"plan_type": "plus",
				"rate_limit": map[string]any{
					"allowed": true,
				},
			})
		default:
			http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	cfg.Network.RefreshURL = server.URL
	cfg.Network.UsageURL = server.URL
	cfg.Network.RefreshClientID = "test-client"
	cfg.Network.MaxUsageWorkers = 2

	currentDoc := `{"tokens":{"account_id":"acct-current","access_token":"current-token","refresh_token":"current-rt","id_token":"` + rootTestTokenWithClaims(map[string]any{
		"https://api.openai.com/profile": map[string]any{
			"email": "current@example.com",
		},
	}) + `"}}`
	if err := os.WriteFile(paths.AuthFile, []byte(currentDoc), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	fastDoc := `{"tokens":{"account_id":"acct-fast","access_token":"fast-token","refresh_token":"fast-rt","id_token":"` + rootTestTokenWithClaims(map[string]any{
		"https://api.openai.com/profile": map[string]any{
			"email": "fast@example.com",
		},
	}) + `"}}`
	slowDoc := `{"tokens":{"account_id":"acct-slow","access_token":"slow-token","refresh_token":"slow-rt","id_token":"` + rootTestTokenWithClaims(map[string]any{
		"https://api.openai.com/profile": map[string]any{
			"email": "slow@example.com",
		},
	}) + `"}}`
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "fast.json"), []byte(fastDoc), 0o600); err != nil {
		t.Fatalf("WriteFile fast alias: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "slow.json"), []byte(slowDoc), 0o600); err != nil {
		t.Fatalf("WriteFile slow alias: %v", err)
	}

	app := &App{
		Paths:          paths,
		Client:         server.Client(),
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"sync", "--all", "--force"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	output := out.String()
	currentIndex := strings.Index(output, "current <current@example.com>")
	fastIndex := strings.Index(output, "fast <fast@example.com>")
	slowIndex := strings.Index(output, "slow <slow@example.com>")
	if currentIndex == -1 || fastIndex == -1 || slowIndex == -1 {
		t.Fatalf("expected streamed sync progress in output, got %q", output)
	}
	if !(currentIndex < fastIndex && fastIndex < slowIndex) {
		t.Fatalf("expected current, then fast, then slow progress order, got %q", output)
	}
}

func TestSyncReportsLastCheckedWriteWarnings(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}

	doc := `{"tokens":{"account_id":"acct-1","access_token":"a","id_token":"b"}}`
	if err := os.WriteFile(paths.AuthFile, []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}
	if err := os.WriteFile(paths.SyncMetaFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile sync meta: %v", err)
	}
	if err := os.Chmod(paths.SyncMetaFile, 0o400); err != nil {
		t.Fatalf("Chmod sync meta: %v", err)
	}
	defer os.Chmod(paths.SyncMetaFile, 0o600)

	app := &App{
		Paths:          paths,
		Client:         &http.Client{},
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"sync"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("last-checked update skipped")) {
		t.Fatalf("expected last-checked warning, got %q", out.String())
	}
}

func TestDoctorStreamsAccountChecksAsRequestsFinish(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts: %v", err)
	}

	for name, token := range map[string]string{
		"fast": "fast-token",
		"slow": "slow-token",
	} {
		content := fmt.Sprintf(`{"tokens":{"account_id":"acct-%s","access_token":"%s","refresh_token":"rt-%s","id_token":"%s"}}`, name, token, name, rootTestTokenWithClaims(map[string]any{
			"https://api.openai.com/profile": map[string]any{
				"email": fmt.Sprintf("%s@example.com", name),
			},
		}))
		if err := os.WriteFile(filepath.Join(paths.AccountsDir, name+".json"), []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
		case "fast-token":
			time.Sleep(10 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "slow-token":
			time.Sleep(120 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.Error(w, "unknown token", http.StatusUnauthorized)
		}
	}))
	defer server.Close()

	cfg.Network.UsageURL = server.URL
	cfg.Network.MaxUsageWorkers = 2

	app := &App{
		Paths:          paths,
		Client:         server.Client(),
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return time.Unix(100, 0) },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"doctor"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	output := out.String()
	fastIndex := strings.Index(output, "fast <fast@example.com>")
	slowIndex := strings.Index(output, "slow <slow@example.com>")
	if fastIndex == -1 || slowIndex == -1 {
		t.Fatalf("expected doctor detail rows in output, got %q", output)
	}
	if fastIndex > slowIndex {
		t.Fatalf("expected fast detail row before slow row, got %q", output)
	}
}

func rootTestTokenWithClaims(claims map[string]any) string {
	payload, err := json.Marshal(claims)
	if err != nil {
		panic(err)
	}
	return "header." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestDeleteAcceptsPipedConfirmation(t *testing.T) {
	t.Parallel()

	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	target := filepath.Join(paths.AccountsDir, "pipe.json")
	if err := os.WriteFile(target, []byte(`{"tokens":{"account_id":"acct-1"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	if _, err := writer.WriteString("yes\n"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	_ = writer.Close()
	defer reader.Close()

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(reader)
	cmd.SetArgs([]string{"delete", "pipe"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("expected account to be removed via piped confirmation, stat err=%v", err)
	}
}

func TestUseWithRelaunchPromptsBeforeRestartingApp(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"old"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(`{"tokens":{"account_id":"new"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	logPath := filepath.Join(paths.HomeDir, "relaunch.log")
	script := "#!/bin/sh\n" +
		"printf '%s:%s\\n' \"$0\" \"$*\" >> '" + logPath + "'\n"
	for _, name := range []string{"osascript", "open"} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(script), 0o755); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("yes\n"))
	cmd.SetArgs([]string{"use", "work", "--relaunch"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	authBytes, err := os.ReadFile(paths.AuthFile)
	if err != nil {
		t.Fatalf("ReadFile auth: %v", err)
	}
	if !bytes.Contains(authBytes, []byte(`"account_id":"new"`)) {
		t.Fatalf("expected auth.json to be switched, got %q", string(authBytes))
	}
	if !bytes.Contains(out.Bytes(), []byte("Relaunch Codex App now?")) {
		t.Fatalf("expected relaunch confirmation prompt, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Relaunching Codex App...")) {
		t.Fatalf("expected relaunch notice, got %q", out.String())
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile relaunch log: %v", err)
	}
	if !bytes.Contains(logBytes, []byte("/osascript:-e if application \"Codex\" is running then tell application \"Codex\" to quit")) {
		t.Fatalf("expected osascript quit command, got %q", string(logBytes))
	}
	if !bytes.Contains(logBytes, []byte("/open:-a Codex")) {
		t.Fatalf("expected open command, got %q", string(logBytes))
	}
}

func TestHermesUseSwitchesHermesAuthWithoutPreparingCodexRuntime(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(paths.HermesAuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll Hermes auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.HermesAccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll Hermes accounts dir: %v", err)
	}
	current := `{
  "providers": {
    "anthropic": {"access_token": "keep"},
    "openai-codex": {"tokens": {"account_id": "old", "access_token": "old-access", "refresh_token": "old-refresh"}}
  }
}`
	if err := os.WriteFile(paths.HermesAuthFile, []byte(current), 0o600); err != nil {
		t.Fatalf("WriteFile Hermes auth: %v", err)
	}
	saved := `{
  "providers": {
    "openai-codex": {"tokens": {"account_id": "new", "access_token": "new-access", "refresh_token": "new-refresh"}}
  },
  "credential_pool": {
    "openai-codex": [{"access_token": "new-access", "refresh_token": "new-refresh"}]
  }
}`
	if err := os.WriteFile(filepath.Join(paths.HermesAccountsDir, "work.json"), []byte(saved), 0o600); err != nil {
		t.Fatalf("WriteFile Hermes account: %v", err)
	}
	unitDir := filepath.Join(paths.HomeDir, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		t.Fatalf("MkdirAll unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, "hermes-gateway.service"), []byte("[Service]\n"), 0o600); err != nil {
		t.Fatalf("WriteFile unit: %v", err)
	}
	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	restartLog := filepath.Join(paths.HomeDir, "restart.log")
	systemctl := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> '" + restartLog + "'\n"
	if err := os.WriteFile(filepath.Join(binDir, "systemctl"), []byte(systemctl), 0o755); err != nil {
		t.Fatalf("WriteFile systemctl: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	app := &App{
		Paths:          paths,
		Now:            time.Now,
		PrepareRuntime: true,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"hermes", "use", "work"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	doc := readRootTestJSON(t, paths.HermesAuthFile)
	if got := nestedRootTestString(doc, "providers", "openai-codex", "tokens", "account_id"); got != "new" {
		t.Fatalf("expected Hermes auth to switch, got %q", got)
	}
	if got := nestedRootTestString(doc, "providers", "anthropic", "access_token"); got != "keep" {
		t.Fatalf("expected non-Codex Hermes provider to be preserved, got %q", got)
	}
	if _, err := os.Stat(paths.ConfigFile); !os.IsNotExist(err) {
		t.Fatalf("expected Hermes command to skip Codex config preparation, stat err=%v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("Switched Hermes to work")) {
		t.Fatalf("expected switch confirmation, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Restarted Hermes gateway via systemd user service")) {
		t.Fatalf("expected restart confirmation, got %q", out.String())
	}
	restartBytes, err := os.ReadFile(restartLog)
	if err != nil {
		t.Fatalf("ReadFile restart log: %v", err)
	}
	if !bytes.Contains(restartBytes, []byte("--user restart hermes-gateway.service")) {
		t.Fatalf("expected systemd restart command, got %q", string(restartBytes))
	}
}

func TestThreadsListsActiveSessions(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	now := mustParseRootTestTime(t, "2026-04-01T10:05:00Z")
	writeActiveThreadFixtures(t, paths, now, "11111111-1111-1111-1111-111111111111", "Busy thread")

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return now },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"threads"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !bytes.Contains(out.Bytes(), []byte("Active threads")) {
		t.Fatalf("expected threads headline, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Busy thread")) {
		t.Fatalf("expected active thread in output, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("11111111-1111-1111-1111-111111111111")) {
		t.Fatalf("expected session id in output, got %q", out.String())
	}
}

func TestThreadsWithAppServerSourceListsActiveSessions(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	now := mustParseRootTestTime(t, "2026-04-01T10:05:00Z")
	payload := `{"data":[{"id":"11111111-1111-1111-1111-111111111111","name":"Busy thread","updatedAt":1775037880,"status":{"type":"notLoaded"}},{"id":"22222222-2222-2222-2222-222222222222","name":"Old thread","updatedAt":1775037830,"status":{"type":"archived"}}],"nextCursor":""}`
	cfg.CodexBin = writeFakeAppServer(t, paths.HomeDir, payload)

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return now },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"threads", "--source", "appserver"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !bytes.Contains(out.Bytes(), []byte("Active threads (appserver)")) {
		t.Fatalf("expected appserver headline, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Busy thread")) {
		t.Fatalf("expected active thread in output, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("notLoaded")) {
		t.Fatalf("expected appserver status in output, got %q", out.String())
	}
	if bytes.Contains(out.Bytes(), []byte("Old thread")) {
		t.Fatalf("expected appserver 30s window to filter stale thread, got %q", out.String())
	}
}

func TestUseWithRelaunchWarnsAboutActiveThreads(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}
	now := mustParseRootTestTime(t, "2026-04-01T10:05:00Z")
	writeActiveThreadFixtures(t, paths, now, "11111111-1111-1111-1111-111111111111", "Busy thread")

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"old"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(`{"tokens":{"account_id":"new"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	logPath := filepath.Join(paths.HomeDir, "relaunch.log")
	script := "#!/bin/sh\n" +
		"printf '%s:%s\\n' \"$0\" \"$*\" >> '" + logPath + "'\n"
	for _, name := range []string{"osascript", "open"} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(script), 0o755); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            func() time.Time { return now },
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("yes\nyes\n"))
	cmd.SetArgs([]string{"use", "work", "--relaunch"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !bytes.Contains(out.Bytes(), []byte("Detected 1 active Codex thread(s)")) {
		t.Fatalf("expected active thread warning, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Busy thread")) {
		t.Fatalf("expected active thread listing, got %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("Relaunch anyway?")) {
		t.Fatalf("expected second relaunch confirmation, got %q", out.String())
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile relaunch log: %v", err)
	}
	if !bytes.Contains(logBytes, []byte("/open:-a Codex")) {
		t.Fatalf("expected open command, got %q", string(logBytes))
	}
}

func TestUseWithRelaunchSkipsRestartWhenNotConfirmed(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"old"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(`{"tokens":{"account_id":"new"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	logPath := filepath.Join(paths.HomeDir, "relaunch.log")
	script := "#!/bin/sh\n" +
		"printf '%s:%s\\n' \"$0\" \"$*\" >> '" + logPath + "'\n"
	for _, name := range []string{"osascript", "open"} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(script), 0o755); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("n\n"))
	cmd.SetArgs([]string{"use", "work", "--relaunch"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	authBytes, err := os.ReadFile(paths.AuthFile)
	if err != nil {
		t.Fatalf("ReadFile auth: %v", err)
	}
	if !bytes.Contains(authBytes, []byte(`"account_id":"new"`)) {
		t.Fatalf("expected auth.json to be switched, got %q", string(authBytes))
	}
	if !bytes.Contains(out.Bytes(), []byte("Skipped Codex App relaunch")) {
		t.Fatalf("expected relaunch skip notice, got %q", out.String())
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("expected relaunch commands not to run, stat err=%v", err)
	}
}

func TestUseWithForceRequiresRelaunch(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"old"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(`{"tokens":{"account_id":"new"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"use", "work", "--force"})

	err = cmd.Execute()
	if err == nil {
		t.Fatal("expected force without relaunch to fail")
	}
	if err.Error() != "--force requires --relaunch" {
		t.Fatalf("expected force/relaunch validation error, got %v", err)
	}
}

func TestUseWithForceRelaunchUsesPkill(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	cfg, err := config.Load(paths)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.AuthFile), 0o755); err != nil {
		t.Fatalf("MkdirAll auth dir: %v", err)
	}
	if err := os.MkdirAll(paths.AccountsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll accounts dir: %v", err)
	}

	if err := os.WriteFile(paths.AuthFile, []byte(`{"tokens":{"account_id":"old"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.AccountsDir, "work.json"), []byte(`{"tokens":{"account_id":"new"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile alias: %v", err)
	}

	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	logPath := filepath.Join(paths.HomeDir, "relaunch.log")
	logScript := "#!/bin/sh\n" +
		"printf '%s:%s\\n' \"$0\" \"$*\" >> '" + logPath + "'\n"
	if err := os.WriteFile(filepath.Join(binDir, "pkill"), []byte(logScript), 0o755); err != nil {
		t.Fatalf("WriteFile pkill: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "open"), []byte(logScript), 0o755); err != nil {
		t.Fatalf("WriteFile open: %v", err)
	}
	osascript := "#!/bin/sh\n" +
		"printf '%s:%s\\n' \"$0\" \"$*\" >> '" + logPath + "'\n" +
		"printf 'false\\n'\n"
	if err := os.WriteFile(filepath.Join(binDir, "osascript"), []byte(osascript), 0o755); err != nil {
		t.Fatalf("WriteFile osascript: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	app := &App{
		Paths:          paths,
		Config:         cfg,
		ConfigLoaded:   true,
		Now:            time.Now,
		PrepareRuntime: false,
	}
	cmd := app.newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewBufferString("yes\n"))
	cmd.SetArgs([]string{"use", "work", "--relaunch", "--force"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile relaunch log: %v", err)
	}
	if !bytes.Contains(logBytes, []byte("/pkill:-x Codex")) {
		t.Fatalf("expected force relaunch to use pkill, got %q", string(logBytes))
	}
	if bytes.Contains(logBytes, []byte(`tell application "Codex" to quit`)) {
		t.Fatalf("expected force relaunch to skip osascript quit, got %q", string(logBytes))
	}
	if !bytes.Contains(logBytes, []byte("/open:-a Codex")) {
		t.Fatalf("expected open command, got %q", string(logBytes))
	}
}

func TestRelaunchCodexAppWaitsForQuitBeforeOpening(t *testing.T) {
	homeDir := t.TempDir()
	binDir := filepath.Join(homeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}

	logPath := filepath.Join(homeDir, "relaunch.log")
	counterPath := filepath.Join(homeDir, "running.count")

	osascript := `#!/bin/sh
printf 'osascript:%s\n' "$*" >> '` + logPath + `'
case "$*" in
  *'tell application "Codex" to quit'*)
    exit 0
    ;;
  *'if application "Codex" is running then return "true"'*)
    count=0
    if [ -f '` + counterPath + `' ]; then
      count=$(cat '` + counterPath + `')
    fi
    count=$((count + 1))
    printf '%s' "$count" > '` + counterPath + `'
    if [ "$count" -lt 3 ]; then
      printf 'true\n'
    else
      printf 'false\n'
    fi
    ;;
esac
`
	open := `#!/bin/sh
count=0
if [ -f '` + counterPath + `' ]; then
  count=$(cat '` + counterPath + `')
fi
printf 'open:%s:checks=%s\n' "$*" "$count" >> '` + logPath + `'
`

	if err := os.WriteFile(filepath.Join(binDir, "osascript"), []byte(osascript), 0o755); err != nil {
		t.Fatalf("WriteFile osascript: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "open"), []byte(open), 0o755); err != nil {
		t.Fatalf("WriteFile open: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := relaunchCodexApp(false); err != nil {
		t.Fatalf("relaunchCodexApp: %v", err)
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile relaunch log: %v", err)
	}
	logText := string(logBytes)
	if strings.Count(logText, `osascript:-e if application "Codex" is running then return "true" -e return "false"`) != 3 {
		t.Fatalf("expected three running-state checks before open, got %q", logText)
	}
	if !strings.Contains(logText, "open:-a Codex:checks=3") {
		t.Fatalf("expected open to happen after the third check, got %q", logText)
	}
}

func readRootTestJSON(t *testing.T, path string) map[string]any {
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

func nestedRootTestString(value any, path ...string) string {
	current := value
	for _, part := range path {
		typed, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = typed[part]
	}
	text, _ := current.(string)
	return text
}
