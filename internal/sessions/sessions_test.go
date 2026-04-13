package sessions

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-switch/internal/config"

	_ "modernc.org/sqlite"
)

func TestListActiveThreadsUsesStateDBRecentUpdates(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	now := mustParseTime(t, "2026-04-01T10:05:00Z")
	updatedAt := now.Add(-30 * time.Second).Unix()
	stateDBPath := filepath.Join(paths.CodexDir, "state_5.sqlite")
	writeStateDB(t, stateDBPath, []stateThreadRecord{
		{
			ID:        "11111111-1111-1111-1111-111111111111",
			Title:     "Active thread",
			Rollout:   filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01", "rollout-2026-04-01T18-00-00-11111111-1111-1111-1111-111111111111.jsonl"),
			UpdatedAt: updatedAt,
			Archived:  0,
		},
		{
			ID:        "22222222-2222-2222-2222-222222222222",
			Title:     "Archived thread",
			Rollout:   filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01", "rollout-2026-04-01T18-01-00-22222222-2222-2222-2222-222222222222.jsonl"),
			UpdatedAt: updatedAt,
			Archived:  1,
		},
	})

	activeThreads, err := ListActiveThreads(paths, now)
	if err != nil {
		t.Fatalf("ListActiveThreads: %v", err)
	}
	if len(activeThreads) != 1 {
		t.Fatalf("expected one active thread, got %d (%+v)", len(activeThreads), activeThreads)
	}
	if activeThreads[0].ThreadName != "Active thread" {
		t.Fatalf("unexpected thread name: %+v", activeThreads[0])
	}
	if activeThreads[0].LastActiveAt == nil || !activeThreads[0].LastActiveAt.Equal(time.Unix(updatedAt, 0).UTC()) {
		t.Fatalf("unexpected last active time: %+v", activeThreads[0])
	}
	if activeThreads[0].SessionFile == "" {
		t.Fatalf("unexpected session file: %+v", activeThreads[0])
	}
}

func TestListActiveThreadsReturnsEmptyWhenSessionIndexMissing(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	now := mustParseTime(t, "2026-04-01T10:05:00Z")
	activeThreads, err := ListActiveThreads(paths, now)
	if err != nil {
		t.Fatalf("ListActiveThreads: %v", err)
	}
	if len(activeThreads) != 0 {
		t.Fatalf("expected no active threads, got %+v", activeThreads)
	}
}

func TestListActiveThreadsSkipsStaleStateThreads(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	now := mustParseTime(t, "2026-04-01T10:30:00Z")
	stateDBPath := filepath.Join(paths.CodexDir, "state_5.sqlite")
	writeStateDB(t, stateDBPath, []stateThreadRecord{
		{
			ID:        "11111111-1111-1111-1111-111111111111",
			Title:     "Old thread",
			Rollout:   filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01", "rollout-2026-04-01T18-00-00-11111111-1111-1111-1111-111111111111.jsonl"),
			UpdatedAt: now.Add(-3 * time.Minute).Unix(),
			Archived:  0,
		},
	})

	activeThreads, err := ListActiveThreads(paths, now)
	if err != nil {
		t.Fatalf("ListActiveThreads: %v", err)
	}
	if len(activeThreads) != 0 {
		t.Fatalf("expected stale thread to be skipped, got %+v", activeThreads)
	}
}

func TestListActiveThreadsFallsBackToSessionLogsWhenStateDBMissing(t *testing.T) {
	paths := config.PathsFromHome(t.TempDir())
	now := mustParseTime(t, "2026-04-01T10:30:00Z")
	if err := os.MkdirAll(filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01"), 0o755); err != nil {
		t.Fatalf("MkdirAll sessions: %v", err)
	}

	index := `{"id":"11111111-1111-1111-1111-111111111111","thread_name":"Long thread","updated_at":"2026-04-01T10:00:00Z"}` + "\n"
	if err := os.WriteFile(filepath.Join(paths.CodexDir, "session_index.jsonl"), []byte(index), 0o600); err != nil {
		t.Fatalf("WriteFile session index: %v", err)
	}

	sessionPath := filepath.Join(paths.CodexDir, "sessions", "2026", "04", "01", "rollout-2026-04-01T18-00-00-11111111-1111-1111-1111-111111111111.jsonl")
	session := "" +
		`{"timestamp":"2026-04-01T10:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-long"}}` + "\n" +
		`{"timestamp":"2026-04-01T10:29:30Z","type":"event_msg","payload":{"type":"token_count"}}` + "\n"
	if err := os.WriteFile(sessionPath, []byte(session), 0o600); err != nil {
		t.Fatalf("WriteFile session: %v", err)
	}

	activeThreads, err := ListActiveThreads(paths, now)
	if err != nil {
		t.Fatalf("ListActiveThreads: %v", err)
	}
	if len(activeThreads) != 1 {
		t.Fatalf("expected fallback thread to stay active, got %+v", activeThreads)
	}
	if activeThreads[0].LastActiveAt == nil || !activeThreads[0].LastActiveAt.Equal(mustParseTime(t, "2026-04-01T10:29:30Z")) {
		t.Fatalf("unexpected last active time: %+v", activeThreads[0])
	}
}

func TestFormatThreadLabelCleansAndTruncates(t *testing.T) {
	thread := ActiveThread{
		SessionID:  "11111111-1111-1111-1111-111111111111",
		ThreadName: "读取test里的test.go\n读取 stdout 出错: bufio.Scanner: token too long\nfatal error: all goroutines are asleep - deadlock!",
	}

	got := FormatThreadLabel(thread)
	if strings.Contains(got, "\n") {
		t.Fatalf("expected single-line label, got %q", got)
	}
	if len([]rune(got)) > maxThreadLabelRunes {
		t.Fatalf("expected truncated label, got %q", got)
	}
	if !strings.Contains(got, "bufio.Scanner") {
		t.Fatalf("expected preserved summary text, got %q", got)
	}
}

type stateThreadRecord struct {
	ID        string
	Title     string
	Rollout   string
	UpdatedAt int64
	Archived  int
}

func writeStateDB(t *testing.T, path string, records []stateThreadRecord) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll state db dir: %v", err)
	}

	db, err := sql.Open("sqlite", path)
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

	for _, record := range records {
		if _, err := db.Exec(`
			INSERT INTO threads (
				id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
				sandbox_policy, approval_mode, tokens_used, has_user_event, archived
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			record.ID,
			record.Rollout,
			record.UpdatedAt,
			record.UpdatedAt,
			"vscode",
			"openai",
			"/tmp/project",
			record.Title,
			"workspace-write",
			"default",
			0,
			1,
			record.Archived,
		); err != nil {
			t.Fatalf("INSERT thread: %v", err)
		}
	}
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("time.Parse: %v", err)
	}
	return parsed
}
