package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"os"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"

	"codex-switch/internal/config"
)

const chrome133UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"

func Fetch(client *stdhttp.Client, cfg config.Config, accessToken string, accountID string) (map[string]any, error) {
	if strings.TrimSpace(accessToken) == "" {
		return nil, fmt.Errorf("missing access_token")
	}
	if strings.TrimSpace(accountID) == "" {
		return nil, fmt.Errorf("missing account_id")
	}

	endpoint, err := url.Parse(cfg.Network.SubscriptionURL)
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("account_id", accountID)
	endpoint.RawQuery = query.Encode()

	if shouldUseTLSClient(endpoint) {
		return fetchWithTLSClient(cfg, accessToken, accountID, endpoint)
	}
	return fetchWithStandardClient(client, cfg, accessToken, accountID, endpoint)
}

func fetchWithStandardClient(client *stdhttp.Client, cfg config.Config, accessToken string, accountID string, endpoint *url.URL) (map[string]any, error) {
	requestCtx, cancel := context.WithTimeout(context.Background(), cfg.UsageTimeoutDuration())
	defer cancel()

	req, err := stdhttp.NewRequestWithContext(requestCtx, stdhttp.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "codex-switch")
	debugRequest("std", req.Method, req.URL.String(), req.URL.Host, accountID, req.Header.Get("Authorization"), req.Header.Get("Accept"), req.Header.Get("Accept-Language"), req.Header.Get("User-Agent"))

	resp, err := client.Do(req)
	if err != nil {
		debugTransportError("std", req.Method, req.URL.String(), err)
		return nil, err
	}
	defer resp.Body.Close()
	debugResponse("std", resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Cf-Ray"), resp.Header.Get("Location"), resp.Header.Get("Server"))

	return decodeResponse(resp.StatusCode, resp.Status, resp.Body)
}

func fetchWithTLSClient(cfg config.Config, accessToken string, accountID string, endpoint *url.URL) (map[string]any, error) {
	jar := tls_client.NewCookieJar()
	timeoutSeconds := cfg.Network.UsageTimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = config.DefaultUsageTimeoutSeconds
	}

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(timeoutSeconds),
		tls_client.WithClientProfile(profiles.Chrome_133),
		tls_client.WithRandomTLSExtensionOrder(),
		tls_client.WithCookieJar(jar),
	}
	if proxyURL := proxyURLFromEnvironment(endpoint); proxyURL != "" {
		options = append(options, tls_client.WithProxyUrl(proxyURL))
		debugProxy("tls-client", proxyURL)
	}

	client, err := tls_client.NewHttpClient(
		tls_client.NewNoopLogger(),
		options...,
	)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header = http.Header{
		"accept":          {"application/json"},
		"accept-language": {"en-US,en;q=0.9"},
		"authorization":   {"Bearer " + accessToken},
		"user-agent":      {chrome133UserAgent},
		http.HeaderOrderKey: {
			"accept",
			"accept-language",
			"authorization",
			"user-agent",
		},
	}
	debugRequest("tls-client", req.Method, req.URL.String(), req.URL.Host, accountID, req.Header.Get("authorization"), req.Header.Get("accept"), req.Header.Get("accept-language"), req.Header.Get("user-agent"))

	resp, err := client.Do(req)
	if err != nil {
		debugTransportError("tls-client", req.Method, req.URL.String(), err)
		return nil, err
	}
	defer resp.Body.Close()
	debugResponse("tls-client", resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Cf-Ray"), resp.Header.Get("Location"), resp.Header.Get("Server"))

	return decodeResponse(resp.StatusCode, resp.Status, resp.Body)
}

func decodeResponse(statusCode int, status string, bodyReader io.Reader) (map[string]any, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return nil, err
	}

	if statusCode < 200 || statusCode >= 300 {
		detail := strings.TrimSpace(string(body))
		if detail == "" {
			detail = status
		}
		return nil, fmt.Errorf("HTTP %d: %s", statusCode, detail)
	}

	result := map[string]any{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("invalid json response: %w", err)
	}

	return result, nil
}

func shouldUseTLSClient(endpoint *url.URL) bool {
	if endpoint == nil || !strings.EqualFold(endpoint.Scheme, "https") {
		return false
	}
	host := strings.ToLower(endpoint.Hostname())
	return host == "chatgpt.com" || strings.HasSuffix(host, ".chatgpt.com")
}

func debugEnabled() bool {
	return strings.TrimSpace(os.Getenv("CODEX_SWITCH_DEBUG_SUBSCRIPTION")) != ""
}

func debugRequest(mode string, method string, rawURL string, host string, accountID string, authorization string, accept string, acceptLanguage string, userAgent string) {
	if !debugEnabled() {
		return
	}
	fmt.Fprintf(
		os.Stderr,
		"[subscriptions] mode=%s request method=%s url=%s host=%s account_id=%s authorization=%s accept=%s accept-language=%s user-agent=%s\n",
		mode,
		method,
		rawURL,
		host,
		accountID,
		maskAuthorization(authorization),
		accept,
		acceptLanguage,
		userAgent,
	)
}

func debugTransportError(mode string, method string, rawURL string, err error) {
	if !debugEnabled() {
		return
	}
	fmt.Fprintf(os.Stderr, "[subscriptions] mode=%s transport_error method=%s url=%s err=%v\n", mode, method, rawURL, err)
}

func debugResponse(mode string, statusCode int, contentType string, cfRay string, location string, server string) {
	if !debugEnabled() {
		return
	}
	fmt.Fprintf(
		os.Stderr,
		"[subscriptions] mode=%s response status=%d content-type=%s cf-ray=%s location=%s server=%s\n",
		mode,
		statusCode,
		contentType,
		cfRay,
		location,
		server,
	)
}

func debugProxy(mode string, proxyURL string) {
	if !debugEnabled() {
		return
	}
	fmt.Fprintf(os.Stderr, "[subscriptions] mode=%s proxy=%s\n", mode, proxyURL)
}

func maskAuthorization(value string) string {
	if !strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return value
	}
	token := strings.TrimSpace(value[7:])
	if len(token) <= 12 {
		return "Bearer " + token
	}
	return "Bearer " + token[:8] + "..." + token[len(token)-4:]
}

func proxyURLFromEnvironment(endpoint *url.URL) string {
	if endpoint == nil {
		return ""
	}
	req := &stdhttp.Request{
		URL: endpoint,
	}
	proxyURL, err := stdhttp.ProxyFromEnvironment(req)
	if err != nil || proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}

func Summarize(payload map[string]any) string {
	planType, _ := payload["plan_type"].(string)
	activeUntil, _ := payload["active_until"].(string)
	willRenew, _ := payload["will_renew"].(bool)

	planType = strings.TrimSpace(planType)
	activeUntil = strings.TrimSpace(activeUntil)
	if planType == "" && activeUntil == "" {
		return "-"
	}
	if activeUntil == "" {
		return planType
	}

	date := activeUntil
	if len(activeUntil) >= 10 {
		date = activeUntil[:10]
	}
	if planType == "" {
		return date
	}
	if willRenew {
		return fmt.Sprintf("%s renews %s", planType, date)
	}
	return fmt.Sprintf("%s until %s", planType, date)
}
