package accounts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"codex-switch/internal/auth"
	"codex-switch/internal/config"
	"codex-switch/internal/subscription"
	"codex-switch/internal/support"
	"codex-switch/internal/usage"
)

type Snapshot struct {
	Path            string
	Name            string
	Tokens          map[string]string
	AccountID       string
	Email           string
	Plan            string
	TokenExpiry     string
	LastRefreshTime *time.Time
}

type ListRow struct {
	Marker      string
	Ready       string
	Account     string
	Plan        string
	FiveHour    string
	Weekly      string
	LastChecked string
}

type ListRowsResult struct {
	Rows           []ListRow
	CurrentIndices map[int]struct{}
	Notes          []string
}

type ListRowEvent struct {
	Row       ListRow
	IsCurrent bool
	Note      string
}

type PrunePair struct {
	Keep   string
	Remove string
}

type SyncMeta struct {
	LastChecked  map[string]string `json:"last_checked"`
	CurrentAlias string            `json:"current_alias,omitempty"`
}

const secureFileMode = 0o600

func EnsureAccountsDir(paths config.Paths) error {
	return os.MkdirAll(paths.AccountsDir, 0o755)
}

func ListAccountNames(paths config.Paths) []string {
	files, err := filepath.Glob(filepath.Join(paths.AccountsDir, "*.json"))
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(files))
	for _, path := range files {
		names = append(names, strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	}
	sort.Strings(names)
	return names
}

func DetectCurrentAccountName(paths config.Paths) string {
	files := listAccountFiles(paths)
	if _, err := os.Stat(paths.AuthFile); err != nil {
		debugCurrentAccount("auth file missing: %s", paths.AuthFile)
		return ""
	}

	meta := loadSyncMeta(paths)
	debugCurrentAccount("start auth=%s current_alias=%q saved_files=%d", paths.AuthFile, meta.CurrentAlias, len(files))
	if meta.CurrentAlias != "" {
		currentPath, aliasName, ok := resolveAccountPathByName(paths, meta.CurrentAlias)
		if ok {
			same, sameErr := sameFile(currentPath, paths.AuthFile)
			if sameErr == nil && same {
				debugCurrentAccount("matched current_alias=%q by identical file content -> alias=%q", meta.CurrentAlias, aliasName)
				return aliasName
			}
			if sameErr != nil {
				debugCurrentAccount("current_alias=%q sameFile error: %v", meta.CurrentAlias, sameErr)
			} else {
				debugCurrentAccount("current_alias=%q sameFile=false", meta.CurrentAlias)
			}

			currentID := AccountIDFromFile(paths.AuthFile)
			aliasID := AccountIDFromFile(currentPath)
			debugCurrentAccount("current_alias=%q auth_account_id=%q alias_account_id=%q", meta.CurrentAlias, currentID, aliasID)
			if currentID != "" && aliasID == currentID {
				debugCurrentAccount("matched current_alias=%q by account_id -> alias=%q", meta.CurrentAlias, aliasName)
				return aliasName
			}
		} else {
			debugCurrentAccount("current_alias=%q file missing", meta.CurrentAlias)
		}
	}

	for _, path := range files {
		same, err := sameFile(path, paths.AuthFile)
		if err == nil && same {
			debugCurrentAccount("matched alias=%q by identical file content", stem(path))
			return stem(path)
		}
		if err != nil {
			debugCurrentAccount("sameFile error alias=%q: %v", stem(path), err)
		}
	}

	currentID := AccountIDFromFile(paths.AuthFile)
	if currentID == "" {
		debugCurrentAccount("auth account_id missing; no alias match")
		return ""
	}
	debugCurrentAccount("falling back to account_id matching auth_account_id=%q", currentID)

	for _, path := range files {
		aliasID := AccountIDFromFile(path)
		debugCurrentAccount("compare alias=%q alias_account_id=%q auth_account_id=%q", stem(path), aliasID, currentID)
		if aliasID == currentID {
			debugCurrentAccount("matched alias=%q by account_id", stem(path))
			return stem(path)
		}
	}

	debugCurrentAccount("no current alias matched")
	return ""
}

func ReadSnapshot(path string, now time.Time) (*Snapshot, error) {
	doc, err := auth.LoadDocument(path)
	if err != nil {
		return nil, err
	}

	tokens := auth.Tokens(doc)
	email, plan := auth.ExtractEmailAndPlan(tokens)
	expiration := auth.ExpirationUnix(tokens["access_token"])
	if expiration == nil {
		expiration = auth.ExpirationUnix(tokens["id_token"])
	}

	return &Snapshot{
		Path:            path,
		Name:            stem(path),
		Tokens:          tokens,
		AccountID:       auth.AccountID(tokens),
		Email:           email,
		Plan:            plan,
		TokenExpiry:     support.FormatUnix(expiration, now),
		LastRefreshTime: support.ParseISOToTime(stringValue(doc["last_refresh"])),
	}, nil
}

func BuildListRows(paths config.Paths, cfg config.Config, client *http.Client, localOnly bool, now time.Time) ListRowsResult {
	prepared := prepareListRows(paths, now)
	result := ListRowsResult{
		Rows:           []ListRow{},
		CurrentIndices: map[int]struct{}{},
		Notes:          []string{},
	}

	for _, item := range prepared.items {
		if item.event == nil {
			continue
		}
		appendListRowEvent(&result, *item.event)
	}

	snapshots := map[string]*Snapshot{}
	for _, item := range prepared.items {
		if item.snapshot == nil {
			continue
		}
		snapshots[item.snapshot.Name] = item.snapshot
	}

	usageByToken := map[string]usageResult{}
	subscriptionByAccount := map[string]subscriptionResult{}
	if !localOnly {
		usageByToken = fetchUsageConcurrently(cfg, client, snapshots)
		subscriptionByAccount = fetchSubscriptionsConcurrently(cfg, client, snapshots)
	}

	for _, item := range prepared.items {
		if item.snapshot == nil {
			continue
		}

		fetched := usageResult{}
		subscriptionFetched := subscriptionResult{}
		if !localOnly {
			fetched = usageResultForSnapshot(usageByToken, item.snapshot)
			subscriptionFetched = subscriptionResultForSnapshot(subscriptionByAccount, item.snapshot)
		}
		appendListRowEvent(&result, buildListRowEvent(item.snapshot, prepared.currentName, prepared.meta, localOnly, now, fetched, subscriptionFetched))
	}

	return result
}

func StreamListRows(paths config.Paths, cfg config.Config, client *http.Client, localOnly bool, now time.Time, emit func(ListRowEvent)) {
	prepared := prepareListRows(paths, now)
	for _, item := range prepared.items {
		if item.event != nil {
			emit(*item.event)
		}
	}

	if localOnly {
		for _, item := range prepared.items {
			if item.snapshot == nil {
				continue
			}
			emit(buildListRowEvent(item.snapshot, prepared.currentName, prepared.meta, true, now, usageResult{}, subscriptionResult{}))
		}
		return
	}

	snapshots := []*Snapshot{}
	for _, item := range prepared.items {
		if item.snapshot == nil {
			continue
		}
		token := strings.TrimSpace(item.snapshot.Tokens["access_token"])
		if token == "" {
			emit(buildListRowEvent(item.snapshot, prepared.currentName, prepared.meta, false, now, usageResult{err: fmt.Errorf("missing access_token")}, subscriptionResult{err: fmt.Errorf("missing access_token")}))
			continue
		}
		snapshots = append(snapshots, item.snapshot)
	}

	if len(snapshots) == 0 {
		return
	}

	workers := cfg.Network.MaxUsageWorkers
	if workers <= 0 {
		workers = 1
	}

	type fetchResult struct {
		snapshot *Snapshot
		usage    usageResult
		sub      subscriptionResult
	}

	jobs := make(chan *Snapshot)
	results := make(chan fetchResult)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for snapshot := range jobs {
				usageData, usageErr := usage.Fetch(client, cfg, snapshot.Tokens["access_token"])
				subscriptionData, subscriptionErr := subscription.Fetch(client, cfg, snapshot.Tokens["access_token"], snapshot.AccountID)
				results <- fetchResult{
					snapshot: snapshot,
					usage:    usageResult{data: usageData, err: usageErr},
					sub:      subscriptionResult{data: subscriptionData, err: subscriptionErr},
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		for _, snapshot := range snapshots {
			jobs <- snapshot
		}
		close(jobs)
	}()

	for fetched := range results {
		emit(buildListRowEvent(fetched.snapshot, prepared.currentName, prepared.meta, false, now, fetched.usage, fetched.sub))
	}
}

func SyncSavedAliases(paths config.Paths) ([]string, []string, []string) {
	if _, err := os.Stat(paths.AuthFile); err != nil {
		return nil, nil, []string{"Not logged in."}
	}

	currentID := AccountIDFromFile(paths.AuthFile)
	if currentID == "" {
		return nil, nil, []string{"Current auth.json has no account_id."}
	}

	matching := []string{}
	for _, path := range listAccountFiles(paths) {
		if AccountIDFromFile(path) == currentID {
			matching = append(matching, path)
		}
	}
	if len(matching) == 0 {
		return nil, nil, []string{"No saved aliases match the current account."}
	}

	updated := []string{}
	warnings := []string{}
	checkedNames := make([]string, 0, len(matching))
	for _, path := range matching {
		same, err := sameFile(paths.AuthFile, path)
		if err == nil && same {
			checkedNames = append(checkedNames, stem(path))
			continue
		}
		if err := copyFile(paths.AuthFile, path); err == nil {
			updated = append(updated, stem(path))
			checkedNames = append(checkedNames, stem(path))
		} else {
			warnings = append(warnings, fmt.Sprintf("%s: sync failed: %v", stem(path), err))
		}
	}

	return updated, checkedNames, warnings
}

func Save(paths config.Paths, name string, force bool) error {
	if err := validateAccountName(name); err != nil {
		return err
	}
	if _, err := os.Stat(paths.AuthFile); err != nil {
		return fmt.Errorf("Not logged in to Codex.")
	}
	if err := EnsureAccountsDir(paths); err != nil {
		return err
	}

	target := filepath.Join(paths.AccountsDir, name+".json")
	if _, err := os.Stat(target); err == nil && !force {
		return fmt.Errorf("Account already exists: %s\nUse --force to overwrite it.", name)
	}
	if existingPath, existingAlias, ok := resolveAccountPathByName(paths, name); ok {
		target = existingPath
		name = existingAlias
	}

	if err := copyFile(paths.AuthFile, target); err != nil {
		return err
	}
	return SetCurrentAlias(paths, name)
}

func Use(paths config.Paths, name string) error {
	if err := validateAccountName(name); err != nil {
		return err
	}

	source, aliasName, ok := resolveAccountPathByName(paths, name)
	if !ok {
		return fmt.Errorf("Account does not exist.")
	}

	if err := copyFile(source, paths.AuthFile); err != nil {
		return err
	}
	return SetCurrentAlias(paths, aliasName)
}

func Rename(paths config.Paths, oldName, newName string) error {
	if err := validateAccountName(oldName); err != nil {
		return fmt.Errorf("invalid old account name: %w", err)
	}
	if err := validateAccountName(newName); err != nil {
		return fmt.Errorf("invalid new account name: %w", err)
	}

	oldPath, oldAlias, ok := resolveAccountPathByName(paths, oldName)
	if !ok {
		return fmt.Errorf("Account does not exist: %s", oldName)
	}
	newPath := filepath.Join(paths.AccountsDir, newName+".json")
	oldInfo, err := os.Stat(oldPath)
	if err != nil {
		return fmt.Errorf("Account does not exist: %s", oldName)
	}
	if newInfo, err := os.Stat(newPath); err == nil {
		if !os.SameFile(oldInfo, newInfo) {
			return fmt.Errorf("Account already exists: %s", newName)
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	meta := loadSyncMeta(paths)
	if value, ok := meta.LastChecked[oldAlias]; ok {
		meta.LastChecked[newName] = value
		delete(meta.LastChecked, oldAlias)
	}
	if strings.EqualFold(meta.CurrentAlias, oldAlias) {
		meta.CurrentAlias = newName
	}
	_ = saveSyncMeta(paths, meta)

	return nil
}

func Delete(paths config.Paths, name string) error {
	if err := validateAccountName(name); err != nil {
		return err
	}

	target, aliasName, ok := resolveAccountPathByName(paths, name)
	if !ok {
		return fmt.Errorf("Account does not exist: %s", name)
	}
	if err := os.Remove(target); err != nil {
		return err
	}
	meta := loadSyncMeta(paths)
	delete(meta.LastChecked, aliasName)
	if strings.EqualFold(meta.CurrentAlias, aliasName) {
		meta.CurrentAlias = ""
	}
	_ = saveSyncMeta(paths, meta)
	return nil
}

func Prune(paths config.Paths, apply bool) ([]PrunePair, error) {
	files := listAccountFiles(paths)
	if len(files) == 0 {
		return nil, nil
	}

	currentName := DetectCurrentAccountName(paths)
	groups := map[string][]string{}
	for _, path := range files {
		snapshot, err := ReadSnapshot(path, time.Now())
		key := ""
		if err == nil && snapshot.AccountID != "" {
			key = "id:" + snapshot.AccountID
		} else {
			bytes, readErr := os.ReadFile(path)
			if readErr != nil {
				key = "path:" + path
			} else {
				key = "sha:" + string(bytes)
			}
		}
		groups[key] = append(groups[key], path)
	}

	pairs := []PrunePair{}
	removeErrors := []error{}
	successfullyRemoved := []string{}
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}

		sort.Slice(group, func(i, j int) bool {
			left := stem(group[i])
			right := stem(group[j])
			if left == currentName {
				return true
			}
			if right == currentName {
				return false
			}
			leftInfo, _ := os.Stat(group[i])
			rightInfo, _ := os.Stat(group[j])
			if leftInfo != nil && rightInfo != nil && !leftInfo.ModTime().Equal(rightInfo.ModTime()) {
				return leftInfo.ModTime().After(rightInfo.ModTime())
			}
			return left < right
		})

		keep := stem(group[0])
		for _, path := range group[1:] {
			removeName := stem(path)
			pairs = append(pairs, PrunePair{Keep: keep, Remove: removeName})
			if apply {
				if err := os.Remove(path); err != nil {
					removeErrors = append(removeErrors, fmt.Errorf("%s: %w", removeName, err))
				} else {
					successfullyRemoved = append(successfullyRemoved, removeName)
				}
			}
		}
	}

	if apply {
		meta := loadSyncMeta(paths)
		for _, name := range successfullyRemoved {
			delete(meta.LastChecked, name)
			if meta.CurrentAlias == name {
				meta.CurrentAlias = ""
			}
		}
		_ = saveSyncMeta(paths, meta)
	}

	if len(removeErrors) > 0 {
		return pairs, errors.Join(removeErrors...)
	}

	return pairs, nil
}

func RecordLastChecked(paths config.Paths, names []string, now time.Time) error {
	if len(names) == 0 {
		return nil
	}

	meta := loadSyncMeta(paths)
	for _, name := range names {
		meta.LastChecked[name] = now.UTC().Format(time.RFC3339Nano)
	}
	return saveSyncMeta(paths, meta)
}

func SetCurrentAlias(paths config.Paths, name string) error {
	if err := validateAccountName(name); err != nil {
		return err
	}

	meta := loadSyncMeta(paths)
	meta.CurrentAlias = name
	return saveSyncMeta(paths, meta)
}

func AccountIDFromFile(path string) string {
	doc, err := auth.LoadDocument(path)
	if err != nil {
		return ""
	}
	return auth.AccountID(auth.Tokens(doc))
}

func fetchUsageConcurrently(cfg config.Config, client *http.Client, snapshots map[string]*Snapshot) map[string]usageResult {
	unique := map[string]struct{}{}
	for _, snapshot := range snapshots {
		if token := snapshot.Tokens["access_token"]; token != "" {
			unique[token] = struct{}{}
		}
	}

	results := map[string]usageResult{}
	if len(unique) == 0 {
		return results
	}

	workers := cfg.Network.MaxUsageWorkers
	if workers <= 0 {
		workers = 1
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan string)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for token := range jobs {
				data, err := usage.Fetch(client, cfg, token)
				mu.Lock()
				results[token] = usageResult{data: data, err: err}
				mu.Unlock()
			}
		}()
	}

	for token := range unique {
		jobs <- token
	}
	close(jobs)
	wg.Wait()

	return results
}

func listAccountFiles(paths config.Paths) []string {
	files, err := filepath.Glob(filepath.Join(paths.AccountsDir, "*.json"))
	if err != nil {
		return nil
	}
	sort.Strings(files)
	return files
}

func resolveAccountPathByName(paths config.Paths, name string) (string, string, bool) {
	for _, path := range listAccountFiles(paths) {
		alias := stem(path)
		if strings.EqualFold(alias, name) {
			return path, alias, true
		}
	}
	return "", "", false
}

func sameFile(left, right string) (bool, error) {
	leftBytes, err := os.ReadFile(left)
	if err != nil {
		return false, err
	}
	rightBytes, err := os.ReadFile(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}

func copyFile(source, target string) error {
	bytes, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(target, bytes, secureFileMode)
}

func loadSyncMeta(paths config.Paths) SyncMeta {
	meta := SyncMeta{LastChecked: map[string]string{}}
	bytes, err := os.ReadFile(paths.SyncMetaFile)
	if err != nil {
		return meta
	}
	_ = jsonUnmarshal(bytes, &meta)
	if meta.LastChecked == nil {
		meta.LastChecked = map[string]string{}
	}
	return meta
}

func saveSyncMeta(paths config.Paths, meta SyncMeta) error {
	bytes, err := jsonMarshalIndent(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(paths.SyncMetaFile, append(bytes, '\n'), secureFileMode)
}

func jsonMarshalIndent(value any) ([]byte, error) {
	return json.MarshalIndent(value, "", "  ")
}

func jsonUnmarshal(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func stem(path string) string {
	return strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func debugCurrentAccountEnabled() bool {
	return strings.TrimSpace(os.Getenv("CODEX_SWITCH_DEBUG_CURRENT_ACCOUNT")) != ""
}

func debugCurrentAccount(format string, args ...any) {
	if !debugCurrentAccountEnabled() {
		return
	}
	fmt.Fprintf(os.Stderr, "[current-account] "+format+"\n", args...)
}

func validateAccountName(name string) error {
	text := strings.TrimSpace(name)
	if text == "" {
		return fmt.Errorf("Please provide an account name.")
	}
	if text == "." || text == ".." {
		return fmt.Errorf("account name may only contain letters, numbers, dot, underscore, and hyphen")
	}
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf("account name may only contain letters, numbers, dot, underscore, and hyphen")
		}
	}
	return nil
}

type usageResult struct {
	data map[string]any
	err  error
}

type subscriptionResult struct {
	data map[string]any
	err  error
}

type listRowPreparation struct {
	currentName string
	meta        SyncMeta
	items       []preparedListItem
}

type preparedListItem struct {
	snapshot *Snapshot
	event    *ListRowEvent
}

func prepareListRows(paths config.Paths, now time.Time) listRowPreparation {
	files := listAccountFiles(paths)
	currentName := DetectCurrentAccountName(paths)
	meta := loadSyncMeta(paths)
	items := make([]preparedListItem, 0, len(files))

	for _, path := range files {
		snapshot, err := ReadSnapshot(path, now)
		if err != nil {
			item := preparedListItem{
				event: &ListRowEvent{
					Row: ListRow{
						Marker:      " ",
						Ready:       "UNK",
						Account:     stem(path),
						Plan:        "-",
						FiveHour:    "unavailable (-)",
						Weekly:      "unavailable (-)",
						LastChecked: support.FormatRelativeAge(support.ParseISOToTime(meta.LastChecked[stem(path)]), now),
					},
					Note: fmt.Sprintf("%s: read failed: %v", stem(path), err),
				},
			}
			items = append(items, item)
			continue
		}
		items = append(items, preparedListItem{snapshot: snapshot})
	}

	return listRowPreparation{
		currentName: currentName,
		meta:        meta,
		items:       items,
	}
}

func buildListRowEvent(snapshot *Snapshot, currentName string, meta SyncMeta, localOnly bool, now time.Time, fetched usageResult, subscriptionFetched subscriptionResult) ListRowEvent {
	row := ListRow{
		Marker:   " ",
		Ready:    "UNK",
		Account:  fmt.Sprintf("%s <%s>", snapshot.Name, snapshot.Email),
		Plan:     snapshot.Plan,
		FiveHour: "local only (-)",
		Weekly:   "local only (-)",
	}
	if snapshot.Name == currentName {
		row.Marker = "*"
	}

	event := ListRowEvent{
		Row:       row,
		IsCurrent: snapshot.Name == currentName,
	}

	if localOnly {
		row.LastChecked = support.FormatRelativeAge(lastCheckedTime(snapshot, meta), now)
		event.Row = row
		return event
	}

	if fetched.data != nil {
		if email, ok := fetched.data["email"].(string); ok && email != "" {
			row.Account = fmt.Sprintf("%s <%s>", snapshot.Name, email)
		}
		if plan, ok := fetched.data["plan_type"].(string); ok && plan != "" {
			row.Plan = plan
		}
		rateLimit, _ := fetched.data["rate_limit"].(map[string]any)
		if allowed, ok := rateLimit["allowed"].(bool); ok && allowed {
			row.Ready = "YES"
		} else {
			row.Ready = "NO"
		}
		fiveHour, resetFiveHour, weekly, resetWeekly := usage.SummarizeRateLimit(rateLimit, now)
		row.FiveHour = fmt.Sprintf("%s (%s)", fiveHour, resetFiveHour)
		row.Weekly = fmt.Sprintf("%s (%s)", weekly, resetWeekly)
	} else {
		row.Ready = "UNK"
		row.FiveHour = fmt.Sprintf("unavailable (%s)", snapshot.TokenExpiry)
		row.Weekly = "unavailable (-)"
		if fetched.err != nil {
			event.Note = fmt.Sprintf("%s: usage lookup failed: %v", snapshot.Name, fetched.err)
		}
	}
	if subscriptionFetched.data != nil {
		summary := subscription.Summarize(subscriptionFetched.data)
		if summary != "" && summary != "-" {
			row.Plan = summary
		}
	} else if subscriptionFetched.err != nil {
		if event.Note == "" {
			event.Note = fmt.Sprintf("%s: subscription lookup failed: %v", snapshot.Name, subscriptionFetched.err)
		} else {
			event.Note += fmt.Sprintf("; subscription lookup failed: %v", subscriptionFetched.err)
		}
	}

	row.LastChecked = support.FormatRelativeAge(lastCheckedTime(snapshot, meta), now)
	event.Row = row
	return event
}

func lastCheckedTime(snapshot *Snapshot, meta SyncMeta) *time.Time {
	if snapshot.LastRefreshTime != nil {
		return snapshot.LastRefreshTime
	}
	return support.ParseISOToTime(meta.LastChecked[snapshot.Name])
}

func usageResultForSnapshot(usageByToken map[string]usageResult, snapshot *Snapshot) usageResult {
	token := strings.TrimSpace(snapshot.Tokens["access_token"])
	if token == "" {
		return usageResult{err: fmt.Errorf("missing access_token")}
	}
	if fetched, ok := usageByToken[token]; ok {
		return fetched
	}
	return usageResult{err: fmt.Errorf("usage response missing")}
}

func subscriptionResultForSnapshot(subscriptionByAccount map[string]subscriptionResult, snapshot *Snapshot) subscriptionResult {
	accountID := strings.TrimSpace(snapshot.AccountID)
	if accountID == "" {
		return subscriptionResult{err: fmt.Errorf("missing account_id")}
	}
	if fetched, ok := subscriptionByAccount[accountID]; ok {
		return fetched
	}
	return subscriptionResult{err: fmt.Errorf("subscription response missing")}
}

func fetchSubscriptionsConcurrently(cfg config.Config, client *http.Client, snapshots map[string]*Snapshot) map[string]subscriptionResult {
	unique := map[string]*Snapshot{}
	for _, snapshot := range snapshots {
		accountID := strings.TrimSpace(snapshot.AccountID)
		if accountID == "" {
			continue
		}
		unique[accountID] = snapshot
	}

	results := map[string]subscriptionResult{}
	if len(unique) == 0 {
		return results
	}

	workers := cfg.Network.MaxUsageWorkers
	if workers <= 0 {
		workers = 1
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan *Snapshot)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for snapshot := range jobs {
				data, err := subscription.Fetch(client, cfg, snapshot.Tokens["access_token"], snapshot.AccountID)
				mu.Lock()
				results[strings.TrimSpace(snapshot.AccountID)] = subscriptionResult{data: data, err: err}
				mu.Unlock()
			}
		}()
	}

	for _, snapshot := range unique {
		jobs <- snapshot
	}
	close(jobs)
	wg.Wait()

	return results
}

func appendListRowEvent(result *ListRowsResult, event ListRowEvent) {
	result.Rows = append(result.Rows, event.Row)
	if event.IsCurrent {
		result.CurrentIndices[len(result.Rows)-1] = struct{}{}
	}
	if event.Note != "" {
		result.Notes = append(result.Notes, event.Note)
	}
}
