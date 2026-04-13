package sessions

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"codex-switch/internal/config"

	_ "modernc.org/sqlite"
)

var sessionIDPattern = regexp.MustCompile(`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})`)

const ActiveThreadMaxIdle = 2 * time.Minute
const AppServerActiveThreadMaxIdle = 30 * time.Second
const maxThreadLabelRunes = 96

type ThreadSource string

const (
	ThreadSourceLocal     ThreadSource = "local"
	ThreadSourceAppServer ThreadSource = "appserver"
)

type ActiveThread struct {
	SessionID         string
	ThreadName        string
	Status            string
	SessionFile       string
	UpdatedAt         *time.Time
	LastActiveAt      *time.Time
	LastTurnID        string
	LastTaskStartedAt *time.Time
}

type sessionIndexRecord struct {
	ID         string `json:"id"`
	ThreadName string `json:"thread_name"`
	UpdatedAt  string `json:"updated_at"`
}

type sessionFileRecord struct {
	Path     string
	Archived bool
}

type sessionInspection struct {
	Thinking          bool
	LastEventAt       *time.Time
	LastTurnID        string
	LastTaskStartedAt *time.Time
	LastTaskEndedAt   *time.Time
}

func ListActiveThreads(paths config.Paths, now time.Time) ([]ActiveThread, error) {
	return ListActiveThreadsWithSource(paths, now, ThreadSourceLocal, "")
}

func ListActiveThreadsWithSource(paths config.Paths, now time.Time, source ThreadSource, codexBin string) ([]ActiveThread, error) {
	switch source {
	case ThreadSourceLocal:
		return listActiveThreadsLocal(paths, now)
	case ThreadSourceAppServer:
		return listActiveThreadsFromAppServer(codexBin, now)
	default:
		return nil, fmt.Errorf("unsupported thread source %q", source)
	}
}

func NormalizeThreadSource(value string) (ThreadSource, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(ThreadSourceLocal):
		return ThreadSourceLocal, nil
	case string(ThreadSourceAppServer), "app-server", "server":
		return ThreadSourceAppServer, nil
	default:
		return "", fmt.Errorf("unsupported thread source %q", value)
	}
}

func listActiveThreadsLocal(paths config.Paths, now time.Time) ([]ActiveThread, error) {
	activeThreads, fromStateDB, err := listActiveThreadsFromStateDB(paths, now)
	if err != nil {
		return nil, err
	}
	if fromStateDB {
		return activeThreads, nil
	}

	return listActiveThreadsFromSessionLogs(paths, now)
}

func listActiveThreadsFromStateDB(paths config.Paths, now time.Time) ([]ActiveThread, bool, error) {
	stateDBPath, err := findLatestStateDB(paths.CodexDir)
	if err != nil {
		return nil, false, err
	}
	if stateDBPath == "" {
		return nil, false, nil
	}

	db, err := sql.Open("sqlite", stateDBPath)
	if err != nil {
		return nil, false, err
	}
	defer db.Close()

	hasThreadsTable, err := sqliteTableExists(db, "threads")
	if err != nil {
		return nil, false, err
	}
	if !hasThreadsTable {
		return nil, false, nil
	}

	rows, err := db.Query(`
		SELECT id, title, rollout_path, updated_at
		FROM threads
		WHERE archived = 0 AND updated_at >= ?
		ORDER BY updated_at DESC, id DESC
	`, now.Add(-ActiveThreadMaxIdle).Unix())
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	active := []ActiveThread{}
	for rows.Next() {
		var sessionID, title, rolloutPath string
		var updatedAtUnix int64
		if err := rows.Scan(&sessionID, &title, &rolloutPath, &updatedAtUnix); err != nil {
			return nil, false, err
		}

		updatedAt := time.Unix(updatedAtUnix, 0).UTC()
		active = append(active, ActiveThread{
			SessionID:    sessionID,
			ThreadName:   strings.TrimSpace(title),
			SessionFile:  rolloutPath,
			UpdatedAt:    &updatedAt,
			LastActiveAt: &updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	return active, true, nil
}

func sqliteTableExists(db *sql.DB, name string) (bool, error) {
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		name,
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func findLatestStateDB(codexDir string) (string, error) {
	candidates, err := filepath.Glob(filepath.Join(codexDir, "state_*.sqlite"))
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", nil
	}

	type datedPath struct {
		path    string
		modTime time.Time
	}

	var latest datedPath
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		if latest.path == "" || info.ModTime().After(latest.modTime) {
			latest = datedPath{path: candidate, modTime: info.ModTime()}
		}
	}

	return latest.path, nil
}

func listActiveThreadsFromSessionLogs(paths config.Paths, now time.Time) ([]ActiveThread, error) {
	indexRecords, err := readLatestSessionIndexRecords(filepath.Join(paths.CodexDir, "session_index.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(indexRecords) == 0 {
		return []ActiveThread{}, nil
	}

	sessionFiles, err := buildSessionFileLookup(paths.CodexDir)
	if err != nil {
		return nil, err
	}

	active := make([]ActiveThread, 0, len(indexRecords))
	for _, record := range indexRecords {
		fileRecord, ok := sessionFiles[record.ID]
		if !ok || fileRecord.Archived {
			continue
		}

		inspection, err := inspectSessionFile(fileRecord.Path)
		if err != nil || !inspection.Thinking {
			continue
		}

		if isStaleThread(record, inspection, now) {
			continue
		}

		active = append(active, ActiveThread{
			SessionID:         record.ID,
			ThreadName:        strings.TrimSpace(record.ThreadName),
			SessionFile:       fileRecord.Path,
			UpdatedAt:         parseTimestamp(record.UpdatedAt),
			LastActiveAt:      inspection.LastEventAt,
			LastTurnID:        inspection.LastTurnID,
			LastTaskStartedAt: inspection.LastTaskStartedAt,
		})
	}

	sort.Slice(active, func(i, j int) bool {
		left := active[i].LastActiveAt
		right := active[j].LastActiveAt
		switch {
		case left == nil && right == nil:
			left = active[i].UpdatedAt
			right = active[j].UpdatedAt
			switch {
			case left == nil && right == nil:
				return active[i].SessionID < active[j].SessionID
			case left == nil:
				return false
			case right == nil:
				return true
			default:
				return left.After(*right)
			}
		case left == nil:
			return false
		case right == nil:
			return true
		default:
			return left.After(*right)
		}
	})

	return active, nil
}

func isStaleThread(record sessionIndexRecord, inspection sessionInspection, now time.Time) bool {
	cutoff := now.Add(-ActiveThreadMaxIdle)
	candidates := []*time.Time{
		inspection.LastEventAt,
		parseTimestamp(record.UpdatedAt),
	}
	for _, candidate := range candidates {
		if candidate != nil && !candidate.Before(cutoff) {
			return false
		}
	}
	return true
}

func readLatestSessionIndexRecords(path string) ([]sessionIndexRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []sessionIndexRecord{}, nil
		}
		return nil, err
	}
	defer file.Close()

	type datedRecord struct {
		record    sessionIndexRecord
		updatedAt *time.Time
	}

	latestByID := map[string]datedRecord{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record sessionIndexRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if strings.TrimSpace(record.ID) == "" {
			continue
		}

		updatedAt := parseTimestamp(record.UpdatedAt)
		existing, ok := latestByID[record.ID]
		if !ok {
			latestByID[record.ID] = datedRecord{record: record, updatedAt: updatedAt}
			continue
		}
		if updatedAt == nil {
			continue
		}
		if existing.updatedAt == nil || updatedAt.After(*existing.updatedAt) {
			latestByID[record.ID] = datedRecord{record: record, updatedAt: updatedAt}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	records := make([]sessionIndexRecord, 0, len(latestByID))
	for _, record := range latestByID {
		records = append(records, record.record)
	}
	return records, nil
}

func buildSessionFileLookup(codexDir string) (map[string]sessionFileRecord, error) {
	lookup := map[string]sessionFileRecord{}
	for _, candidate := range []sessionFileRecord{
		{Path: filepath.Join(codexDir, "sessions"), Archived: false},
		{Path: filepath.Join(codexDir, "archived_sessions"), Archived: true},
	} {
		if err := filepath.WalkDir(candidate.Path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
				return nil
			}

			sessionID := extractSessionID(path)
			if sessionID == "" {
				return nil
			}

			existing, ok := lookup[sessionID]
			if !ok || shouldReplaceSessionFile(existing, path, candidate.Archived) {
				lookup[sessionID] = sessionFileRecord{Path: path, Archived: candidate.Archived}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return lookup, nil
}

func shouldReplaceSessionFile(existing sessionFileRecord, candidatePath string, candidateArchived bool) bool {
	if existing.Archived && !candidateArchived {
		return true
	}
	if existing.Archived == candidateArchived && candidatePath > existing.Path {
		return true
	}
	return false
}

func inspectSessionFile(path string) (sessionInspection, error) {
	file, err := os.Open(path)
	if err != nil {
		return sessionInspection{}, err
	}
	defer file.Close()

	var lastTurnID string
	var lastStartedAt *time.Time
	var lastEventAt *time.Time
	completedByTurn := map[string]*time.Time{}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 2*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var item struct {
			Timestamp string `json:"timestamp"`
			Type      string `json:"type"`
			Payload   struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		if item.Type != "event_msg" {
			continue
		}

		timestamp := parseTimestamp(item.Timestamp)
		if timestamp != nil {
			lastEventAt = timestamp
		}
		if strings.TrimSpace(item.Payload.TurnID) == "" {
			continue
		}
		switch item.Payload.Type {
		case "task_started":
			lastTurnID = item.Payload.TurnID
			lastStartedAt = timestamp
		case "task_complete":
			completedByTurn[item.Payload.TurnID] = timestamp
		}
	}
	if err := scanner.Err(); err != nil {
		return sessionInspection{}, err
	}

	lastEndedAt := completedByTurn[lastTurnID]
	return sessionInspection{
		Thinking:          lastTurnID != "" && lastEndedAt == nil,
		LastEventAt:       lastEventAt,
		LastTurnID:        lastTurnID,
		LastTaskStartedAt: lastStartedAt,
		LastTaskEndedAt:   lastEndedAt,
	}, nil
}

func extractSessionID(path string) string {
	matches := sessionIDPattern.FindAllString(filepath.Base(path), -1)
	if len(matches) == 0 {
		return ""
	}
	return matches[len(matches)-1]
}

func parseTimestamp(value string) *time.Time {
	text := strings.TrimSpace(value)
	if text == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.ReplaceAll(text, "Z", "+00:00"))
	if err != nil {
		return nil
	}
	return &parsed
}

func FormatThreadLabel(thread ActiveThread) string {
	if cleaned := cleanThreadLabel(thread.ThreadName); cleaned != "" {
		return cleaned
	}
	return fmt.Sprintf("Session %s", thread.SessionID)
}

func cleanThreadLabel(value string) string {
	text := strings.TrimSpace(value)
	if text == "" {
		return ""
	}

	parts := strings.Fields(text)
	if len(parts) == 0 {
		return ""
	}

	singleLine := strings.Join(parts, " ")
	runes := []rune(singleLine)
	if len(runes) <= maxThreadLabelRunes {
		return singleLine
	}
	return string(runes[:maxThreadLabelRunes-1]) + "…"
}
