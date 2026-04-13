package cli

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const codexAppName = "Codex"

const (
	relaunchQuitPollInterval = 200 * time.Millisecond
	relaunchQuitTimeout      = 10 * time.Second
)

func relaunchCodexApp(force bool) error {
	if _, err := execLookPath("osascript"); err != nil {
		return fmt.Errorf("unable to relaunch Codex App: osascript not found in PATH")
	}
	if _, err := execLookPath("open"); err != nil {
		return fmt.Errorf("unable to relaunch Codex App: open not found in PATH")
	}
	if force {
		if _, err := execLookPath("pkill"); err != nil {
			return fmt.Errorf("unable to force relaunch Codex App: pkill not found in PATH")
		}
	}

	if err := quitCodexApp(force); err != nil {
		return fmt.Errorf("quit Codex App: %w", err)
	}

	if err := waitForCodexAppToQuit(); err != nil {
		return err
	}

	openCmd := execCommand("open", "-a", codexAppName)
	if err := openCmd.Run(); err != nil {
		return fmt.Errorf("open Codex App: %w", err)
	}
	return nil
}

func quitCodexApp(force bool) error {
	if force {
		return forceQuitCodexApp()
	}

	quitCmd := execCommand(
		"osascript",
		"-e",
		fmt.Sprintf(`if application "%s" is running then tell application "%s" to quit`, codexAppName, codexAppName),
	)
	return quitCmd.Run()
}

func forceQuitCodexApp() error {
	killCmd := execCommand("pkill", "-x", codexAppName)
	if err := killCmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil
		}
		return err
	}
	return nil
}

func waitForCodexAppToQuit() error {
	deadline := time.Now().Add(relaunchQuitTimeout)
	for {
		running, err := isCodexAppRunning()
		if err != nil {
			return fmt.Errorf("check Codex App state: %w", err)
		}
		if !running {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s to quit", codexAppName)
		}
		time.Sleep(relaunchQuitPollInterval)
	}
}

func isCodexAppRunning() (bool, error) {
	checkCmd := execCommand(
		"osascript",
		"-e",
		fmt.Sprintf(`if application "%s" is running then return "true"`, codexAppName),
		"-e",
		`return "false"`,
	)
	output, err := checkCmd.Output()
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(output)) == "true", nil
}
