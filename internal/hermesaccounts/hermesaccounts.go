package hermesaccounts

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"codex-switch/internal/config"
)

const (
	secureFileMode      = 0o600
	providerID          = "openai-codex"
	authStoreVersion    = 1
	defaultCodexBaseURL = "https://chatgpt.com/backend-api/codex"
)

type Document map[string]any

type Snapshot struct {
	Path      string
	Name      string
	AccountID string
	Email     string
}

type AccountEntry struct {
	Name     string
	Snapshot *Snapshot
	Imported bool
	Err      error
}

func EnsureAccountsDir(paths config.Paths) error {
	return os.MkdirAll(paths.HermesAccountsDir, 0o755)
}

func ListAccountNames(paths config.Paths) []string {
	files, err := filepath.Glob(filepath.Join(paths.HermesAccountsDir, "*.json"))
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

func ListAvailableAccountNames(paths config.Paths) []string {
	nameByKey := map[string]string{}
	for _, name := range ListAccountNames(paths) {
		nameByKey[strings.ToLower(name)] = name
	}
	for _, name := range listImportableCodexAccountNames(paths) {
		key := strings.ToLower(name)
		if _, ok := nameByKey[key]; !ok {
			nameByKey[key] = name
		}
	}

	names := make([]string, 0, len(nameByKey))
	for _, name := range nameByKey {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func ListAvailableAccounts(paths config.Paths) []AccountEntry {
	names := ListAvailableAccountNames(paths)
	entries := make([]AccountEntry, 0, len(names))
	for _, name := range names {
		hermesPath := filepath.Join(paths.HermesAccountsDir, name+".json")
		if _, err := os.Stat(hermesPath); err == nil {
			snapshot, snapshotErr := ReadSnapshot(hermesPath)
			entries = append(entries, AccountEntry{
				Name:     name,
				Snapshot: snapshot,
				Imported: true,
				Err:      snapshotErr,
			})
			continue
		}

		codexPath, aliasName, ok := resolveCodexAccountPathByName(paths, name)
		if !ok {
			entries = append(entries, AccountEntry{Name: name, Err: os.ErrNotExist})
			continue
		}
		snapshot, snapshotErr := ReadCodexSnapshot(codexPath)
		if snapshot != nil {
			snapshot.Name = aliasName
		}
		entries = append(entries, AccountEntry{
			Name:     aliasName,
			Snapshot: snapshot,
			Imported: false,
			Err:      snapshotErr,
		})
	}
	return entries
}

func Save(paths config.Paths, name string, force bool) error {
	if err := validateAccountName(name); err != nil {
		return err
	}
	doc, err := loadDocument(paths.HermesAuthFile)
	if err != nil {
		return fmt.Errorf("not logged in to Hermes Codex auth: %w", err)
	}
	if _, err := codexProviderState(doc); err != nil {
		return err
	}
	if err := EnsureAccountsDir(paths); err != nil {
		return err
	}

	target := filepath.Join(paths.HermesAccountsDir, name+".json")
	if existingPath, existingAlias, ok := resolveAccountPathByName(paths, name); ok {
		if !force {
			return fmt.Errorf("Hermes account already exists: %s\nUse --force to overwrite it.", existingAlias)
		}
		target = existingPath
		name = existingAlias
	}

	if err := saveDocument(target, doc); err != nil {
		return err
	}
	return nil
}

func Use(paths config.Paths, name string) error {
	if err := validateAccountName(name); err != nil {
		return err
	}

	snapshot, err := loadHermesAccountOrImportCodex(paths, name)
	if err != nil {
		return err
	}
	state, err := codexProviderState(snapshot)
	if err != nil {
		return err
	}

	current, err := loadDocument(paths.HermesAuthFile)
	if err != nil {
		current = Document{}
	}
	mergeCodexProvider(current, snapshot, state, name)
	return saveDocument(paths.HermesAuthFile, current)
}

func loadHermesAccountOrImportCodex(paths config.Paths, name string) (Document, error) {
	source, _, ok := resolveAccountPathByName(paths, name)
	if ok {
		return loadDocument(source)
	}

	source, aliasName, ok := resolveCodexAccountPathByName(paths, name)
	if !ok {
		return nil, fmt.Errorf("Hermes account does not exist.")
	}
	codexDoc, err := loadDocument(source)
	if err != nil {
		return nil, err
	}
	snapshot, err := hermesDocumentFromCodexAccount(codexDoc, aliasName)
	if err != nil {
		return nil, err
	}
	if err := EnsureAccountsDir(paths); err != nil {
		return nil, err
	}
	if err := saveDocument(filepath.Join(paths.HermesAccountsDir, aliasName+".json"), snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func CurrentName(paths config.Paths) string {
	current, err := ReadSnapshot(paths.HermesAuthFile)
	if err != nil || current.AccountID == "" {
		return ""
	}
	for _, entry := range ListAvailableAccounts(paths) {
		if entry.Err == nil && entry.Snapshot != nil && entry.Snapshot.AccountID == current.AccountID {
			return entry.Name
		}
	}
	return ""
}

func ReadSnapshot(path string) (*Snapshot, error) {
	doc, err := loadDocument(path)
	if err != nil {
		return nil, err
	}
	state, err := codexProviderState(doc)
	if err != nil {
		return nil, err
	}
	tokens := stringMap(valueMap(state["tokens"]))
	return &Snapshot{
		Path:      path,
		Name:      strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		AccountID: accountIDFromTokens(tokens),
		Email:     emailFromTokens(tokens),
	}, nil
}

func ReadCodexSnapshot(path string) (*Snapshot, error) {
	doc, err := loadDocument(path)
	if err != nil {
		return nil, err
	}
	tokens := stringMap(valueMap(doc["tokens"]))
	if strings.TrimSpace(tokens["access_token"]) == "" {
		return nil, fmt.Errorf("Codex account is missing access_token.")
	}
	if strings.TrimSpace(tokens["refresh_token"]) == "" {
		return nil, fmt.Errorf("Codex account is missing refresh_token.")
	}
	return &Snapshot{
		Path:      path,
		Name:      strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		AccountID: accountIDFromTokens(tokens),
		Email:     emailFromTokens(tokens),
	}, nil
}

func loadDocument(path string) (Document, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	doc := Document{}
	if err := json.Unmarshal(bytes, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func saveDocument(path string, doc Document) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	doc["version"] = authStoreVersion
	doc["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	bytes, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(bytes, '\n'), secureFileMode)
}

func mergeCodexProvider(current Document, snapshot Document, state map[string]any, accountName string) {
	providers := ensureObject(current, "providers")
	providers[providerID] = state
	current["active_provider"] = providerID

	sourcePool := valueMap(snapshot["credential_pool"])
	sourceEntries, _ := sourcePool[providerID].([]any)
	if len(sourceEntries) == 0 {
		sourceEntries = synthesizePoolEntries(state, accountName)
	}
	pool := ensureObject(current, "credential_pool")
	pool[providerID] = sourceEntries
}

func synthesizePoolEntries(state map[string]any, accountName string) []any {
	tokens := stringMap(valueMap(state["tokens"]))
	accessToken := strings.TrimSpace(tokens["access_token"])
	if accessToken == "" {
		return []any{}
	}
	refreshToken := strings.TrimSpace(tokens["refresh_token"])
	label := accountName
	if email := emailFromTokens(tokens); email != "" {
		label = email
	}
	entry := map[string]any{
		"id":            accountName,
		"label":         label,
		"auth_type":     "oauth",
		"priority":      0,
		"source":        "manual:codex-switch",
		"access_token":  accessToken,
		"base_url":      defaultCodexBaseURL,
		"last_refresh":  stringValue(state["last_refresh"]),
		"request_count": 0,
	}
	if refreshToken != "" {
		entry["refresh_token"] = refreshToken
	}
	return []any{entry}
}

func hermesDocumentFromCodexAccount(doc Document, accountName string) (Document, error) {
	tokens := valueMap(doc["tokens"])
	if strings.TrimSpace(stringValue(tokens["access_token"])) == "" {
		return nil, fmt.Errorf("Codex account %q is missing access_token.", accountName)
	}
	if strings.TrimSpace(stringValue(tokens["refresh_token"])) == "" {
		return nil, fmt.Errorf("Codex account %q is missing refresh_token.", accountName)
	}
	state := map[string]any{
		"tokens":       tokens,
		"auth_mode":    firstNonEmpty(stringValue(doc["auth_mode"]), "chatgpt"),
		"last_refresh": stringValue(doc["last_refresh"]),
	}
	result := Document{
		"version":         authStoreVersion,
		"providers":       map[string]any{providerID: state},
		"active_provider": providerID,
		"credential_pool": map[string]any{
			providerID: synthesizePoolEntries(state, accountName),
		},
	}
	return result, nil
}

func codexProviderState(doc Document) (map[string]any, error) {
	providers := valueMap(doc["providers"])
	state := valueMap(providers[providerID])
	if len(state) == 0 {
		return nil, fmt.Errorf("Hermes auth state has no %s provider.", providerID)
	}
	tokens := valueMap(state["tokens"])
	if strings.TrimSpace(stringValue(tokens["access_token"])) == "" {
		return nil, fmt.Errorf("Hermes %s provider is missing access_token.", providerID)
	}
	if strings.TrimSpace(stringValue(tokens["refresh_token"])) == "" {
		return nil, fmt.Errorf("Hermes %s provider is missing refresh_token.", providerID)
	}
	return state, nil
}

func ensureObject(doc Document, key string) map[string]any {
	existing, ok := doc[key].(map[string]any)
	if ok {
		return existing
	}
	created := map[string]any{}
	doc[key] = created
	return created
}

func valueMap(value any) map[string]any {
	typed, _ := value.(map[string]any)
	return typed
}

func stringMap(raw map[string]any) map[string]string {
	result := map[string]string{}
	for key, value := range raw {
		if text, ok := value.(string); ok {
			result[key] = text
		}
	}
	return result
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func emailFromTokens(tokens map[string]string) string {
	for _, key := range []string{"id_token", "access_token"} {
		payload := decodeJWTPayload(tokens[key])
		for _, claim := range []string{"email", "preferred_username", "upn"} {
			if email := strings.TrimSpace(stringValue(payload[claim])); email != "" {
				return email
			}
		}
	}
	return ""
}

func accountIDFromTokens(tokens map[string]string) string {
	if accountID := strings.TrimSpace(tokens["account_id"]); accountID != "" {
		return accountID
	}
	for _, key := range []string{"id_token", "access_token"} {
		payload := decodeJWTPayload(tokens[key])
		authClaims := valueMap(payload["https://api.openai.com/auth"])
		if accountID := strings.TrimSpace(stringValue(authClaims["chatgpt_account_id"])); accountID != "" {
			return accountID
		}
	}
	return ""
}

func decodeJWTPayload(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) < 3 {
		return map[string]any{}
	}
	payload := parts[1]
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return map[string]any{}
	}
	result := map[string]any{}
	_ = json.Unmarshal(decoded, &result)
	return result
}

func resolveAccountPathByName(paths config.Paths, name string) (string, string, bool) {
	target := filepath.Join(paths.HermesAccountsDir, name+".json")
	if _, err := os.Stat(target); err == nil {
		return target, name, true
	}
	for _, existingName := range ListAccountNames(paths) {
		if strings.EqualFold(existingName, name) {
			return filepath.Join(paths.HermesAccountsDir, existingName+".json"), existingName, true
		}
	}
	return "", "", false
}

func resolveCodexAccountPathByName(paths config.Paths, name string) (string, string, bool) {
	target := filepath.Join(paths.AccountsDir, name+".json")
	if _, err := os.Stat(target); err == nil {
		return target, name, true
	}
	files, err := filepath.Glob(filepath.Join(paths.AccountsDir, "*.json"))
	if err != nil {
		return "", "", false
	}
	for _, path := range files {
		alias := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		if strings.EqualFold(alias, name) {
			return path, alias, true
		}
	}
	return "", "", false
}

func listImportableCodexAccountNames(paths config.Paths) []string {
	files, err := filepath.Glob(filepath.Join(paths.AccountsDir, "*.json"))
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(files))
	for _, path := range files {
		if _, err := ReadCodexSnapshot(path); err != nil {
			continue
		}
		names = append(names, strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	}
	sort.Strings(names)
	return names
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
