package subscription

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"codex-switch/internal/config"
)

func TestFetch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("expected bearer auth header, got %q", got)
		}
		if got := r.URL.Query().Get("account_id"); got != "acct-1" {
			t.Fatalf("expected account_id acct-1, got %q", got)
		}
		w.Write([]byte(`{"plan_type":"plus","active_until":"2026-04-14T11:17:05Z","will_renew":true}`))
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.Network.SubscriptionURL = server.URL
	data, err := Fetch(server.Client(), cfg, "token", "acct-1")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if data["plan_type"] != "plus" {
		t.Fatalf("unexpected response: %+v", data)
	}
}

func TestSummarize(t *testing.T) {
	t.Parallel()

	got := Summarize(map[string]any{
		"plan_type":    "plus",
		"active_until": "2026-04-14T11:17:05Z",
		"will_renew":   true,
	})
	if got != "plus renews 2026-04-14" {
		t.Fatalf("unexpected summary: %q", got)
	}
}
