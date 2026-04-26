package cli

import (
	"context"
	"os"
	"os/exec"
)

func execCommand(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}

func execCommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}

func execLookPath(file string) (string, error) {
	return exec.LookPath(file)
}

func isTTY() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
