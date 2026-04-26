package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"codex-switch/internal/config"
)

func TestRestartHermesGatewayFallsBackToHermesCLI(t *testing.T) {
	t.Setenv("HERMES_HOME", "")
	paths := config.PathsFromHome(t.TempDir())
	binDir := filepath.Join(paths.HomeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("MkdirAll bin dir: %v", err)
	}
	restartLog := filepath.Join(paths.HomeDir, "restart.log")
	hermes := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> '" + restartLog + "'\n"
	if err := os.WriteFile(filepath.Join(binDir, "hermes"), []byte(hermes), 0o755); err != nil {
		t.Fatalf("WriteFile hermes: %v", err)
	}
	t.Setenv("PATH", binDir)

	source, err := restartHermesGateway(paths)
	if err != nil {
		t.Fatalf("restartHermesGateway: %v", err)
	}
	if source != "hermes CLI" {
		t.Fatalf("expected hermes CLI source, got %q", source)
	}
	restartBytes, err := os.ReadFile(restartLog)
	if err != nil {
		t.Fatalf("ReadFile restart log: %v", err)
	}
	if !bytes.Contains(restartBytes, []byte("gateway restart")) {
		t.Fatalf("expected hermes gateway restart, got %q", string(restartBytes))
	}
}
