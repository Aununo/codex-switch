package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"codex-switch/internal/config"
)

const (
	hermesGatewayServiceName = "hermes-gateway.service"
	hermesRestartTimeout     = 2 * time.Minute
)

func restartHermesGateway(paths config.Paths) (string, error) {
	if hasUserSystemdUnit(paths) {
		if _, err := execLookPath("systemctl"); err != nil {
			return "", fmt.Errorf("systemd unit exists but systemctl was not found in PATH")
		}
		if err := runCommandWithTimeout(hermesRestartTimeout, "systemctl", "--user", "restart", hermesGatewayServiceName); err != nil {
			return "", fmt.Errorf("systemctl --user restart %s: %w", hermesGatewayServiceName, err)
		}
		return "systemd user service", nil
	}

	if _, err := execLookPath("hermes"); err != nil {
		return "", fmt.Errorf("Hermes gateway service not found and hermes CLI was not found in PATH")
	}
	if err := runCommandWithTimeout(hermesRestartTimeout, "hermes", "gateway", "restart"); err != nil {
		return "", fmt.Errorf("hermes gateway restart: %w", err)
	}
	return "hermes CLI", nil
}

func hasUserSystemdUnit(paths config.Paths) bool {
	unitPath := filepath.Join(paths.HomeDir, ".config", "systemd", "user", hermesGatewayServiceName)
	if _, err := os.Stat(unitPath); err == nil {
		return true
	}
	return false
}

func runCommandWithTimeout(timeout time.Duration, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := execCommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("timed out after %s", timeout)
	}
	if err != nil {
		var exitErr *exec.ExitError
		detail := strings.TrimSpace(string(output))
		if errors.As(err, &exitErr) {
			if detail != "" {
				return fmt.Errorf("exit %d: %s", exitErr.ExitCode(), detail)
			}
			return fmt.Errorf("exit %d", exitErr.ExitCode())
		}
		if detail != "" {
			return fmt.Errorf("%w: %s", err, detail)
		}
		return err
	}
	return nil
}
