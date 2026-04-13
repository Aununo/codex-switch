package sessions

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

type rpcRequest struct {
	Method string      `json:"method"`
	ID     *int        `json:"id,omitempty"`
	Params interface{} `json:"params,omitempty"`
}

type rpcResponse struct {
	ID     *int            `json:"id,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

type appServerThreadSummary struct {
	ID        string  `json:"id"`
	Name      *string `json:"name"`
	UpdatedAt int64   `json:"updatedAt"`
	Status    struct {
		Type string `json:"type"`
	} `json:"status"`
}

type appServerThreadListResult struct {
	Data       []appServerThreadSummary `json:"data"`
	NextCursor string                   `json:"nextCursor"`
}

type appServerClient struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *json.Decoder
	stderr bytes.Buffer

	mu      sync.Mutex
	nextID  int
	pending map[int]chan rpcResponse
	readErr error
}

func listActiveThreadsFromAppServer(codexBin string, now time.Time) ([]ActiveThread, error) {
	bin := strings.TrimSpace(codexBin)
	if bin == "" {
		return nil, fmt.Errorf("unable to find `codex` in PATH")
	}

	client, err := newAppServerClient(bin)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.initialize(); err != nil {
		return nil, err
	}

	cutoff := now.Add(-AppServerActiveThreadMaxIdle)
	active := []ActiveThread{}
	cursor := ""

	for {
		result, err := client.listThreads(cursor, 100)
		if err != nil {
			return nil, err
		}

		stop := false
		for _, thread := range result.Data {
			if strings.TrimSpace(thread.ID) == "" || thread.UpdatedAt == 0 {
				continue
			}

			updatedAt := time.Unix(thread.UpdatedAt, 0).UTC()
			if updatedAt.Before(cutoff) {
				stop = true
				continue
			}

			active = append(active, ActiveThread{
				SessionID:    thread.ID,
				ThreadName:   strings.TrimSpace(derefString(thread.Name)),
				Status:       strings.TrimSpace(thread.Status.Type),
				UpdatedAt:    &updatedAt,
				LastActiveAt: &updatedAt,
			})
		}

		if stop || result.NextCursor == "" || len(result.Data) == 0 {
			break
		}
		cursor = result.NextCursor
	}

	sort.Slice(active, func(i, j int) bool {
		left := active[i].LastActiveAt
		right := active[j].LastActiveAt
		switch {
		case left == nil && right == nil:
			return active[i].SessionID > active[j].SessionID
		case left == nil:
			return false
		case right == nil:
			return true
		default:
			if left.Equal(*right) {
				return active[i].SessionID > active[j].SessionID
			}
			return left.After(*right)
		}
	})

	return active, nil
}

func newAppServerClient(codexBin string) (*appServerClient, error) {
	cmd := exec.Command(codexBin, "app-server")

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("get app-server stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("get app-server stdout pipe: %w", err)
	}

	client := &appServerClient{
		cmd:     cmd,
		stdin:   bufio.NewWriter(stdinPipe),
		stdout:  json.NewDecoder(stdoutPipe),
		nextID:  1,
		pending: make(map[int]chan rpcResponse),
	}
	cmd.Stderr = &client.stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start codex app-server: %w", err)
	}

	go client.readLoop()
	return client, nil
}

func (c *appServerClient) initialize() error {
	if _, err := c.send("initialize", map[string]interface{}{
		"clientInfo": map[string]interface{}{
			"name":    "codex-switch",
			"title":   "codex-switch",
			"version": "0.1.0",
		},
	}); err != nil {
		return err
	}
	return c.notify("initialized", map[string]interface{}{})
}

func (c *appServerClient) listThreads(cursor string, limit int) (appServerThreadListResult, error) {
	params := map[string]interface{}{
		"limit": limit,
	}
	if strings.TrimSpace(cursor) != "" {
		params["cursor"] = cursor
	}

	raw, err := c.send("thread/list", params)
	if err != nil {
		return appServerThreadListResult{}, err
	}

	var result appServerThreadListResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return appServerThreadListResult{}, fmt.Errorf("decode thread/list result: %w", err)
	}
	return result, nil
}

func (c *appServerClient) readLoop() {
	for {
		var resp rpcResponse
		if err := c.stdout.Decode(&resp); err != nil {
			c.failPending(err)
			return
		}

		if resp.ID == nil {
			continue
		}

		c.mu.Lock()
		ch, ok := c.pending[*resp.ID]
		if ok {
			delete(c.pending, *resp.ID)
		}
		c.mu.Unlock()
		if ok {
			ch <- resp
		}
	}
}

func (c *appServerClient) failPending(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	pending := c.pending
	c.pending = make(map[int]chan rpcResponse)
	c.mu.Unlock()

	for _, ch := range pending {
		close(ch)
	}
}

func (c *appServerClient) send(method string, params interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	if c.readErr != nil {
		err := c.readErr
		c.mu.Unlock()
		return nil, fmt.Errorf("app-server reader unavailable for %s: %w", method, err)
	}
	id := c.nextID
	c.nextID++
	ch := make(chan rpcResponse, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	payload, err := json.Marshal(rpcRequest{Method: method, ID: &id, Params: params})
	if err != nil {
		return nil, fmt.Errorf("marshal app-server request %s: %w", method, err)
	}
	if _, err := c.stdin.Write(append(payload, '\n')); err != nil {
		return nil, fmt.Errorf("write app-server request %s: %w", method, err)
	}
	if err := c.stdin.Flush(); err != nil {
		return nil, fmt.Errorf("flush app-server request %s: %w", method, err)
	}

	resp, ok := <-ch
	if !ok {
		return nil, fmt.Errorf("app-server response stream closed while waiting for %s: %w%s",
			method,
			c.readerError(),
			c.stderrSuffix(),
		)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("app-server %s failed: %s%s", method, string(resp.Error), c.stderrSuffix())
	}
	return resp.Result, nil
}

func (c *appServerClient) notify(method string, params interface{}) error {
	payload, err := json.Marshal(rpcRequest{Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("marshal app-server notify %s: %w", method, err)
	}
	if _, err := c.stdin.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write app-server notify %s: %w", method, err)
	}
	if err := c.stdin.Flush(); err != nil {
		return fmt.Errorf("flush app-server notify %s: %w", method, err)
	}
	return nil
}

func (c *appServerClient) readerError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr == nil {
		return io.EOF
	}
	return c.readErr
}

func (c *appServerClient) stderrSuffix() string {
	text := strings.TrimSpace(c.stderr.String())
	if text == "" {
		return ""
	}
	return fmt.Sprintf(" (stderr: %s)", text)
}

func (c *appServerClient) Close() error {
	if c == nil || c.cmd == nil {
		return nil
	}
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	done := make(chan struct{}, 1)
	go func() {
		_ = c.cmd.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
	}
	return nil
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
