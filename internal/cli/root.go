package cli

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"codex-switch/internal/accounts"
	"codex-switch/internal/auth"
	"codex-switch/internal/config"
	"codex-switch/internal/hermesaccounts"
	"codex-switch/internal/sessions"
	"codex-switch/internal/support"
	"codex-switch/internal/usage"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type App struct {
	Paths          config.Paths
	Config         config.Config
	ConfigLoaded   bool
	Client         *http.Client
	Now            func() time.Time
	PrepareRuntime bool
}

func NewRootCmd() (*cobra.Command, error) {
	paths, err := config.DefaultPaths()
	if err != nil {
		return nil, err
	}

	app := &App{
		Paths:          paths,
		Client:         &http.Client{},
		Now:            time.Now,
		PrepareRuntime: true,
	}

	return app.newRootCmd(), nil
}

func (a *App) newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:           "codex-switch",
		Short:         "Codex Account Switcher",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if !a.PrepareRuntime || shouldSkipRuntimePreparation(cmd) {
				return nil
			}
			if err := a.ensureConfig(); err != nil {
				return err
			}
			now := a.Now()
			if _, err := os.Stat(a.Paths.AuthFile); err == nil {
				if _, err := auth.RefreshAuthFileIfNeeded(a.Client, a.Config, a.Paths.AuthFile, false, now); err != nil {
					printInfoWarning(cmd.ErrOrStderr(), fmt.Sprintf("startup refresh skipped: %v", err))
				}
			}
			_, checked, warnings := accounts.SyncSavedAliases(a.Paths)
			for _, warning := range filterStartupWarnings(warnings) {
				printInfoWarning(cmd.ErrOrStderr(), warning)
			}
			if len(checked) > 0 {
				if err := accounts.RecordLastChecked(a.Paths, checked, now); err != nil {
					printInfoWarning(cmd.ErrOrStderr(), fmt.Sprintf("last-checked update skipped: %v", err))
				}
			}
			return nil
		},
	}

	rootCmd.AddCommand(a.newLoginCmd())
	rootCmd.AddCommand(a.newTokenInfoCmd())
	rootCmd.AddCommand(a.newSaveCmd())
	rootCmd.AddCommand(a.newUseCmd())
	rootCmd.AddCommand(a.newHermesCmd())
	rootCmd.AddCommand(a.newListCmd())
	rootCmd.AddCommand(a.newCurrentCmd())
	rootCmd.AddCommand(a.newThreadsCmd())
	rootCmd.AddCommand(a.newSyncCmd())
	rootCmd.AddCommand(a.newRenameCmd())
	rootCmd.AddCommand(a.newDoctorCmd())
	rootCmd.AddCommand(a.newPruneCmd())
	rootCmd.AddCommand(a.newDeleteCmd())
	rootCmd.AddCommand(a.newInstallCompletionCmd())

	rootCmd.InitDefaultCompletionCmd()
	rootCmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		renderHelp(cmd.OutOrStdout(), cmd)
	})
	rootCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		renderHelp(cmd.OutOrStdout(), cmd)
		return nil
	})
	return rootCmd
}

func (a *App) newLoginCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "login [name]",
		Short: "Run `codex login` and optionally save it",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			return a.runLogin(cmd, name, force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing saved alias")
	return cmd
}

func (a *App) newTokenInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "token-info",
		Short: "Show token timestamps and refresh state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runTokenInfo(cmd)
		},
	}
}

func (a *App) newSaveCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:               "save <name>",
		Short:             "Save the current account",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeAccountNames(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := accounts.Save(a.Paths, name, force); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Saved %s", name)))
			fmt.Fprintln(cmd.OutOrStdout())
			return a.showNamedRows(cmd, []string{name}, false)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing saved alias")
	return cmd
}

func (a *App) newUseCmd() *cobra.Command {
	var relaunch bool
	var force bool
	cmd := &cobra.Command{
		Use:               "use <name>",
		Short:             "Switch to a saved account",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeAccountNames(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := accounts.Use(a.Paths, name); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Switched to %s", name)))
			fmt.Fprintln(cmd.OutOrStdout())
			if force && !relaunch {
				return fmt.Errorf("--force requires --relaunch")
			}
			if !relaunch {
				return a.runCurrent(cmd)
			}

			return a.runRelaunch(cmd, force)
		},
	}
	cmd.Flags().BoolVar(&relaunch, "relaunch", false, "Prompt to relaunch Codex App after switching")
	cmd.Flags().BoolVar(&force, "force", false, "Force Codex App to quit during --relaunch")
	return cmd
}

func (a *App) newHermesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hermes",
		Short: "Manage Hermes Codex accounts",
	}
	cmd.AddCommand(a.newHermesSaveCmd())
	cmd.AddCommand(a.newHermesUseCmd())
	cmd.AddCommand(a.newHermesListCmd())
	cmd.AddCommand(a.newHermesCurrentCmd())
	return cmd
}

func (a *App) newHermesSaveCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:               "save <name>",
		Short:             "Save the current Hermes Codex account",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeHermesAccountNames(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := hermesaccounts.Save(a.Paths, name, force); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Saved Hermes %s", name)))
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing saved Hermes alias")
	return cmd
}

func (a *App) newHermesUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "use <name>",
		Short:             "Switch Hermes to a saved Codex account",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeHermesAccountNames(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := hermesaccounts.Use(a.Paths, name); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Switched Hermes to %s", name)))
			source, err := restartHermesGateway(a.Paths)
			if err != nil {
				return fmt.Errorf("Hermes account switched, but restart failed: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Restarted Hermes gateway via %s", source)))
			return nil
		},
	}
}

func (a *App) newHermesListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List Hermes-compatible Codex accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			entries := hermesaccounts.ListAvailableAccounts(a.Paths)
			if len(entries) == 0 {
				printInfo(cmd.OutOrStdout(), "No Hermes-compatible accounts found.")
				return nil
			}
			current := hermesaccounts.CurrentName(a.Paths)
			for _, entry := range entries {
				marker := " "
				if current != "" && strings.EqualFold(current, entry.Name) {
					marker = "*"
				}
				detail := ""
				if entry.Err == nil && entry.Snapshot != nil {
					switch {
					case entry.Snapshot.Email != "":
						detail = " <" + entry.Snapshot.Email + ">"
					case entry.Snapshot.AccountID != "":
						detail = " <" + entry.Snapshot.AccountID + ">"
					}
				}
				if !entry.Imported {
					detail += " [codex]"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s %s%s\n", marker, entry.Name, detail)
			}
			return nil
		},
	}
}

func (a *App) newHermesCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Show the current Hermes Codex account",
		RunE: func(cmd *cobra.Command, _ []string) error {
			name := hermesaccounts.CurrentName(a.Paths)
			if name == "" {
				printInfo(cmd.OutOrStdout(), "Current Hermes account is unnamed (not saved).")
				return nil
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(name))
			return nil
		},
	}
}

func (a *App) newListCmd() *cobra.Command {
	var localOnly bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runList(cmd, localOnly)
		},
	}
	cmd.Flags().BoolVar(&localOnly, "local", false, "Skip live usage lookups")
	return cmd
}

func (a *App) newCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Show the current account",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runCurrent(cmd)
		},
	}
}

func (a *App) newThreadsCmd() *cobra.Command {
	var source string
	cmd := &cobra.Command{
		Use:   "threads",
		Short: "List active Codex threads",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runThreads(cmd, source)
		},
	}
	cmd.Flags().StringVar(&source, "source", string(sessions.ThreadSourceLocal), "thread source: local or appserver")
	return cmd
}

func (a *App) newSyncCmd() *cobra.Command {
	var force bool
	var syncAll bool
	var currentOnly bool
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Refresh near-expiry tokens and sync aliases",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if syncAll && currentOnly {
				return fmt.Errorf("use either --current or --all, not both")
			}
			scope := "current"
			if syncAll {
				scope = "all"
			}
			return a.runSync(cmd, scope, force)
		},
	}
	cmd.Flags().BoolVar(&currentOnly, "current", false, "Sync only the current account")
	cmd.Flags().BoolVar(&syncAll, "all", false, "Sync every saved account")
	cmd.Flags().BoolVar(&force, "force", false, "Force a refresh even if tokens are not near expiry")
	return cmd
}

func (a *App) newRenameCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "rename <old-name> <new-name>",
		Short:             "Rename a saved account",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeRenameArgs(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldName := args[0]
			newName := args[1]
			if err := accounts.Rename(a.Paths, oldName, newName); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Renamed %s -> %s", oldName, newName)))
			fmt.Fprintln(cmd.OutOrStdout())
			return a.showNamedRows(cmd, []string{newName}, false)
		},
	}
}

func (a *App) newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check auth and saved accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runDoctor(cmd)
		},
	}
}

func (a *App) newPruneCmd() *cobra.Command {
	var apply bool
	var assumeYes bool
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Preview or remove duplicate accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runPrune(cmd, apply, assumeYes)
		},
	}
	cmd.Flags().BoolVar(&apply, "apply", false, "Delete duplicate aliases")
	cmd.Flags().BoolVar(&assumeYes, "yes", false, "Skip the confirmation prompt")
	return cmd
}

func (a *App) newDeleteCmd() *cobra.Command {
	var assumeYes bool
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Short:             "Delete an account",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeAccountNames(a.Paths),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := confirmAction(cmd, assumeYes, fmt.Sprintf("Delete saved account %q?", name)); err != nil {
				return err
			}
			if err := accounts.Delete(a.Paths, name); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Deleted %s", name)))
			return nil
		},
	}
	cmd.Flags().BoolVar(&assumeYes, "yes", false, "Skip the confirmation prompt")
	return cmd
}

func (a *App) newInstallCompletionCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "install-completion <zsh|bash>",
		Short:             "Install shell completion for the current user",
		Args:              cobra.ExactArgs(1),
		ValidArgs:         []string{"zsh", "bash"},
		ValidArgsFunction: cobra.FixedCompletions([]string{"zsh", "bash"}, cobra.ShellCompDirectiveNoFileComp),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runInstallCompletion(cmd, args[0])
		},
	}
}

func (a *App) runLogin(cmd *cobra.Command, name string, force bool) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	codexBin := resolveCodexBin(a.Config)
	if codexBin == "" {
		return fmt.Errorf("unable to find `codex` in PATH")
	}

	loginCmd := execCommand(codexBin, "login")
	loginCmd.Stdout = cmd.OutOrStdout()
	loginCmd.Stderr = cmd.ErrOrStderr()
	loginCmd.Stdin = os.Stdin
	if err := loginCmd.Run(); err != nil {
		return err
	}

	if _, err := os.Stat(a.Paths.AuthFile); err != nil {
		return fmt.Errorf("login completed, but auth.json was not created")
	}

	updated, checked, warnings := accounts.SyncSavedAliases(a.Paths)
	if len(checked) > 0 {
		if err := accounts.RecordLastChecked(a.Paths, checked, a.Now()); err != nil {
			printInfoWarning(cmd.OutOrStdout(), fmt.Sprintf("last-checked update skipped: %v", err))
			fmt.Fprintln(cmd.OutOrStdout())
		}
	}
	if len(updated) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Synced aliases: %s", strings.Join(updated, ", "))))
		fmt.Fprintln(cmd.OutOrStdout())
	}
	for _, warning := range warnings {
		printInfoWarning(cmd.OutOrStdout(), warning)
	}
	if len(warnings) > 0 {
		fmt.Fprintln(cmd.OutOrStdout())
	}

	if name != "" {
		if err := accounts.Save(a.Paths, name, force); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Saved %s", name)))
		fmt.Fprintln(cmd.OutOrStdout())
		return a.showNamedRows(cmd, []string{name}, false)
	}

	return a.runCurrent(cmd)
}

func (a *App) runTokenInfo(cmd *cobra.Command) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	doc, err := auth.LoadDocument(a.Paths.AuthFile)
	if err != nil {
		if os.IsNotExist(err) {
			printInfo(cmd.OutOrStdout(), "Not logged in.")
			return nil
		}
		return err
	}

	tokens := auth.Tokens(doc)
	now := a.Now()
	rows := [][]string{
		{"id_token", present(tokens["id_token"]), support.FormatUnix(auth.IssuedAtUnix(tokens["id_token"]), now), support.FormatUnix(auth.ExpirationUnix(tokens["id_token"]), now), lifetime(tokens["id_token"])},
		{"access_token", present(tokens["access_token"]), support.FormatUnix(auth.IssuedAtUnix(tokens["access_token"]), now), support.FormatUnix(auth.ExpirationUnix(tokens["access_token"]), now), lifetime(tokens["access_token"])},
		{"refresh_token", present(tokens["refresh_token"]), "-", "-", refreshTokenDetails(tokens["refresh_token"])},
	}

	printHeadline(cmd.OutOrStdout(), "Token info")
	printTable(cmd.OutOrStdout(), []string{"TOKEN", "PRESENT", "ISSUED", "EXPIRES", "LIFETIME"}, rows)
	fmt.Fprintln(cmd.OutOrStdout())
	printKeyValue(cmd.OutOrStdout(), "config file", a.Paths.ConfigFile)
	printKeyValue(cmd.OutOrStdout(), "refresh margin", a.Config.Refresh.Margin)
	printKeyValue(cmd.OutOrStdout(), "usage timeout", a.Config.UsageTimeoutDuration().String())
	printKeyValue(cmd.OutOrStdout(), "max usage workers", fmt.Sprintf("%d", a.Config.Network.MaxUsageWorkers))
	printKeyValue(cmd.OutOrStdout(), "refresh timeout", a.Config.RefreshTimeoutDuration().String())
	printKeyValue(cmd.OutOrStdout(), "wham usage url", a.Config.Network.UsageURL)
	printKeyValue(cmd.OutOrStdout(), "refresh url", a.Config.Network.RefreshURL)
	printKeyValue(cmd.OutOrStdout(), "refresh client id", fallback(auth.ResolveRefreshClientID(tokens, a.Config), "-"))
	printKeyValue(cmd.OutOrStdout(), "codex bin", fallback(resolveCodexBin(a.Config), "-"))
	printKeyValue(cmd.OutOrStdout(), "refresh token", present(tokens["refresh_token"]))
	printKeyValue(cmd.OutOrStdout(), "refresh token type", refreshTokenDetails(tokens["refresh_token"]))
	printKeyValue(cmd.OutOrStdout(), "access token needs refresh", yesNo(auth.ShouldRefreshAccessToken(tokens["access_token"], a.Config.RefreshMarginDuration(), now)))
	printKeyValue(cmd.OutOrStdout(), "last refresh", support.FormatISO8601(stringValue(doc["last_refresh"])))
	return nil
}

func (a *App) runList(cmd *cobra.Command, localOnly bool) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	if len(accounts.ListAccountNames(a.Paths)) == 0 {
		printInfo(cmd.OutOrStdout(), "No saved accounts.")
		return nil
	}
	if !localOnly {
		if isTTY() {
			renderer := newStreamingListRenderer(cmd)
			renderer.Render()
			accounts.StreamListRows(a.Paths, a.Config, a.Client, false, a.Now(), func(event accounts.ListRowEvent) {
				renderer.Append(event)
			})
			renderer.PrintNotes()
			return nil
		}
		notes := []string{}
		printStreamingListHeader(cmd.OutOrStdout())
		accounts.StreamListRows(a.Paths, a.Config, a.Client, false, a.Now(), func(event accounts.ListRowEvent) {
			printStreamingListRow(cmd.OutOrStdout(), event.Row, event.IsCurrent)
			if event.Note != "" {
				notes = append(notes, event.Note)
			}
		})
		printNotes(cmd, notes)
		return nil
	}
	rows := accounts.BuildListRows(a.Paths, a.Config, a.Client, localOnly, a.Now())
	printListRows(cmd, rows)
	return nil
}

func (a *App) runCurrent(cmd *cobra.Command) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	if _, err := os.Stat(a.Paths.AuthFile); err != nil {
		printInfo(cmd.OutOrStdout(), "Not logged in.")
		return nil
	}

	rows := accounts.BuildListRows(a.Paths, a.Config, a.Client, false, a.Now())
	filtered := filterCurrentRows(rows)
	if len(filtered.Rows) == 0 {
		printInfo(cmd.OutOrStdout(), "Current account is unnamed (not saved).")
		return nil
	}
	printListRows(cmd, filtered)
	return nil
}

func (a *App) runThreads(cmd *cobra.Command, sourceValue string) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}

	source, err := sessions.NormalizeThreadSource(sourceValue)
	if err != nil {
		return err
	}
	codexBin := ""
	if source == sessions.ThreadSourceAppServer {
		codexBin = resolveCodexBin(a.Config)
	}

	activeThreads, err := sessions.ListActiveThreadsWithSource(a.Paths, a.Now(), source, codexBin)
	if err != nil {
		return err
	}
	if len(activeThreads) == 0 {
		printInfo(cmd.OutOrStdout(), fmt.Sprintf("No active Codex threads from %s.", source))
		return nil
	}

	printHeadline(cmd.OutOrStdout(), fmt.Sprintf("Active threads (%s)", source))
	printActiveThreads(cmd.OutOrStdout(), activeThreads)
	return nil
}

func (a *App) runSync(cmd *cobra.Command, scope string, force bool) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	now := a.Now()
	refreshedNames := []string{}
	refreshNotes := []string{}
	checkedNames := []string{}

	if scope == "all" {
		var syncRenderer *syncProgressRenderer
		currentLabel := accountLabelFromPath("current", a.Paths.AuthFile, now)
		if isTTY() {
			fmt.Fprintln(cmd.OutOrStdout())
			printSectionHeader(cmd.OutOrStdout(), "Sync Progress")
			syncRenderer = newSyncProgressRenderer(cmd)
			syncRenderer.Render()
		} else {
			fmt.Fprintln(cmd.OutOrStdout())
			printSectionHeader(cmd.OutOrStdout(), "Sync Progress")
			printSyncProgressHeader(cmd.OutOrStdout())
		}

		if refreshed, err := auth.RefreshAuthFileIfNeeded(a.Client, a.Config, a.Paths.AuthFile, force, now); err == nil && refreshed {
			refreshedNames = append(refreshedNames, "current")
			emitSyncProgress(cmd.OutOrStdout(), syncRenderer, currentLabel, "refreshed", "token updated", ansiFeatureSuccessStyle)
		} else if err != nil {
			refreshNotes = append(refreshNotes, err.Error())
			emitSyncProgress(cmd.OutOrStdout(), syncRenderer, currentLabel, "failed", err.Error(), ansiFeatureWarningStyle)
		} else {
			emitSyncProgress(cmd.OutOrStdout(), syncRenderer, currentLabel, "skipped", "already up to date", ansiFeatureInfoStyle)
		}

		currentAlias := accounts.DetectCurrentAccountName(a.Paths)
		currentID := accounts.AccountIDFromFile(a.Paths.AuthFile)
		results := a.refreshSavedAliasesAsync(currentAlias, currentID, force, now)
		for _, result := range results {
			if result.err != nil {
				refreshNotes = append(refreshNotes, fmt.Sprintf("%s: %v", result.name, result.err))
				emitSyncProgress(cmd.OutOrStdout(), syncRenderer, result.label, "failed", result.err.Error(), ansiFeatureWarningStyle)
				continue
			}
			checkedNames = append(checkedNames, result.name)
			if result.refreshed {
				refreshedNames = append(refreshedNames, result.name)
				emitSyncProgress(cmd.OutOrStdout(), syncRenderer, result.label, "refreshed", "token updated", ansiFeatureSuccessStyle)
				continue
			}
			emitSyncProgress(cmd.OutOrStdout(), syncRenderer, result.label, "skipped", "already up to date", ansiFeatureInfoStyle)
		}

		if syncRenderer != nil {
			fmt.Fprintln(cmd.OutOrStdout())
		}

		updated, checked, warnings := accounts.SyncSavedAliases(a.Paths)
		checkedNames = append(checkedNames, checked...)
		if len(warnings) > 0 {
			for _, warning := range warnings {
				printInfoWarning(cmd.OutOrStdout(), warning)
			}
		} else if len(refreshNotes) == 0 {
			if err := printSummaryOnly(cmd, refreshedNames, updated, force); err != nil {
				return err
			}
		}
		if err := accounts.RecordLastChecked(a.Paths, checkedNames, now); err != nil {
			printInfoWarning(cmd.OutOrStdout(), fmt.Sprintf("last-checked update skipped: %v", err))
			fmt.Fprintln(cmd.OutOrStdout())
		}
		return a.runCurrent(cmd)
	}

	refreshed, err := auth.RefreshAuthFileIfNeeded(a.Client, a.Config, a.Paths.AuthFile, force, now)
	if err != nil {
		refreshNotes = append(refreshNotes, err.Error())
	} else if refreshed {
		refreshedNames = append(refreshedNames, "current")
	}
	updated, checked, warnings := accounts.SyncSavedAliases(a.Paths)
	checkedNames = append(checkedNames, checked...)
	if err := printWarningsOrSummary(cmd, warnings, refreshNotes, refreshedNames, updated, force); err != nil {
		return err
	}
	if err := accounts.RecordLastChecked(a.Paths, checkedNames, now); err != nil {
		printInfoWarning(cmd.OutOrStdout(), fmt.Sprintf("last-checked update skipped: %v", err))
		fmt.Fprintln(cmd.OutOrStdout())
	}
	return a.runCurrent(cmd)
}

type aliasRefreshResult struct {
	name      string
	label     string
	refreshed bool
	err       error
}

func (a *App) refreshSavedAliasesAsync(currentAlias, currentID string, force bool, now time.Time) []aliasRefreshResult {
	type aliasRefreshJob struct {
		name  string
		label string
		path  string
	}

	jobsList := []aliasRefreshJob{}
	for _, name := range accounts.ListAccountNames(a.Paths) {
		path := filepath.Join(a.Paths.AccountsDir, name+".json")
		if name == currentAlias {
			continue
		}
		if currentID != "" && accounts.AccountIDFromFile(path) == currentID {
			continue
		}
		jobsList = append(jobsList, aliasRefreshJob{
			name:  name,
			label: accountLabelFromPath(name, path, now),
			path:  path,
		})
	}
	if len(jobsList) == 0 {
		return nil
	}

	workers := a.Config.Network.MaxUsageWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(jobsList) {
		workers = len(jobsList)
	}

	jobs := make(chan aliasRefreshJob)
	results := make(chan aliasRefreshResult)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				refreshed, err := auth.RefreshAuthFileIfNeeded(a.Client, a.Config, job.path, force, now)
				results <- aliasRefreshResult{
					name:      job.name,
					label:     job.label,
					refreshed: refreshed,
					err:       err,
				}
			}
		}()
	}

	go func() {
		for _, job := range jobsList {
			jobs <- job
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	collected := make([]aliasRefreshResult, 0, len(jobsList))
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

func (a *App) runDoctor(cmd *cobra.Command) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	files := accounts.ListAccountNames(a.Paths)
	currentName := accounts.DetectCurrentAccountName(a.Paths)
	rows := [][]string{
		{"codex dir", status(dirExists(a.Paths.CodexDir)), a.Paths.CodexDir},
		{"auth.json", status(fileExists(a.Paths.AuthFile)), a.Paths.AuthFile},
		{"accounts", status(dirExists(a.Paths.AccountsDir)), a.Paths.AccountsDir},
		{"saved accounts", boolStatus(len(files) > 0, "ok", "empty"), fmt.Sprintf("%d", len(files))},
		{"current alias", boolStatus(currentName != "", "ok", "unknown"), fallback(currentName, "-")},
	}

	printHeadline(cmd.OutOrStdout(), "Doctor")
	printTable(cmd.OutOrStdout(), []string{"CHECK", "STATUS", "DETAIL"}, rows)
	if len(files) == 0 {
		return nil
	}

	fmt.Fprintln(cmd.OutOrStdout())
	printSectionHeader(cmd.OutOrStdout(), "Saved Account Health")
	if isTTY() {
		renderer := newDoctorDetailRenderer(cmd)
		renderer.Render()
		a.streamDoctorDetails(func(result doctorDetailResult) {
			renderer.Append(result)
		})
		renderer.PrintNotes()
		return nil
	}

	notes := []string{}
	printDoctorDetailHeader(cmd.OutOrStdout())
	a.streamDoctorDetails(func(result doctorDetailResult) {
		printDoctorDetailRow(cmd.OutOrStdout(), result.Row)
		if result.Note != "" {
			notes = append(notes, result.Note)
		}
	})
	printNotes(cmd, notes)
	return nil
}

type doctorDetailResult struct {
	Row  []string
	Note string
}

func (a *App) streamDoctorDetails(emit func(doctorDetailResult)) {
	type doctorJob struct {
		name string
		path string
	}

	files := accounts.ListAccountNames(a.Paths)
	if len(files) == 0 {
		return
	}

	jobsList := make([]doctorJob, 0, len(files))
	for _, name := range files {
		jobsList = append(jobsList, doctorJob{
			name: name,
			path: filepath.Join(a.Paths.AccountsDir, name+".json"),
		})
	}

	workers := a.Config.Network.MaxUsageWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(jobsList) {
		workers = len(jobsList)
	}

	jobs := make(chan doctorJob)
	results := make(chan doctorDetailResult)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				snapshot, err := accounts.ReadSnapshot(job.path, a.Now())
				if err != nil {
					results <- doctorDetailResult{
						Row:  []string{job.name, "bad", "no", "no", "read failed"},
						Note: fmt.Sprintf("%s: %v", job.name, err),
					}
					continue
				}

				liveUsage, usageErr := usage.Fetch(a.Client, a.Config, snapshot.Tokens["access_token"])
				live := "fail"
				if usageErr == nil && liveUsage != nil {
					live = "ok"
				}
				result := doctorDetailResult{
					Row: []string{
						formatAccountLabel(job.name, snapshot.Email),
						"ok",
						yesNo(snapshot.AccountID != ""),
						yesNo(snapshot.Tokens["access_token"] != ""),
						live,
					},
				}
				if usageErr != nil {
					result.Note = fmt.Sprintf("%s: live usage check failed: %v", job.name, usageErr)
				}
				results <- result
			}
		}()
	}

	go func() {
		for _, job := range jobsList {
			jobs <- job
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for result := range results {
		emit(result)
	}
}

func (a *App) runPrune(cmd *cobra.Command, apply bool, assumeYes bool) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	pairs, err := accounts.Prune(a.Paths, false)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		printInfo(cmd.OutOrStdout(), "No duplicates found.")
		return nil
	}

	if apply {
		pruneErr := error(nil)
		if err := confirmAction(
			cmd,
			assumeYes,
			fmt.Sprintf("Delete %d duplicate saved account(s)?", len(pairs)),
		); err != nil {
			return err
		}
		pairs, pruneErr = accounts.Prune(a.Paths, true)
		if pruneErr == nil {
			fmt.Fprintln(cmd.OutOrStdout(), colorize("Prune applied"))
		} else {
			printInfoWarning(cmd.OutOrStdout(), "Prune partially applied")
		}
		fmt.Fprintln(cmd.OutOrStdout())
		rows := make([][]string, 0, len(pairs))
		for _, pair := range pairs {
			rows = append(rows, []string{pair.Keep, pair.Remove})
		}
		header := []string{"KEEP", "REMOVED"}
		if pruneErr != nil {
			header = []string{"KEEP", "REMOVE"}
		}
		printTable(cmd.OutOrStdout(), header, rows)
		if pruneErr != nil {
			return pruneErr
		}
		return nil
	}

	printHeadline(cmd.OutOrStdout(), "Prune preview")
	rows := make([][]string, 0, len(pairs))
	for _, pair := range pairs {
		rows = append(rows, []string{pair.Keep, pair.Remove})
	}
	printTable(cmd.OutOrStdout(), []string{"KEEP", "REMOVE"}, rows)
	fmt.Fprintln(cmd.OutOrStdout())
	printCommand(cmd.OutOrStdout(), "Run `prune --apply` to remove the duplicate aliases above.")
	return nil
}

func (a *App) runInstallCompletion(cmd *cobra.Command, shell string) error {
	paths := a.Paths
	root := cmd.Root()
	switch shell {
	case "zsh":
		dir := filepath.Join(paths.HomeDir, ".zsh", "completions")
		target := filepath.Join(dir, "_codex-switch")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		file, err := os.Create(target)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := root.GenZshCompletion(file); err != nil {
			return err
		}
		printHeadline(cmd.OutOrStdout(), "Completion installed")
		printKeyValue(cmd.OutOrStdout(), "shell", "zsh")
		printKeyValue(cmd.OutOrStdout(), "path", target)
		fmt.Fprintln(cmd.OutOrStdout())
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle("Run these if your ~/.zshrc does not already configure zsh completions:", ansiFeatureLabelStyle))
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle(`echo 'fpath=(~/.zsh/completions $fpath)' >> ~/.zshrc`, ansiFeatureCommandStyle))
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle(`echo 'autoload -U compinit && compinit' >> ~/.zshrc`, ansiFeatureCommandStyle))
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle(`source ~/.zshrc`, ansiFeatureCommandStyle))
		return nil
	case "bash":
		dir := filepath.Join(paths.HomeDir, ".local", "share", "bash-completion", "completions")
		target := filepath.Join(dir, "codex-switch")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		file, err := os.Create(target)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := root.GenBashCompletionV2(file, true); err != nil {
			return err
		}
		printHeadline(cmd.OutOrStdout(), "Completion installed")
		printKeyValue(cmd.OutOrStdout(), "shell", "bash")
		printKeyValue(cmd.OutOrStdout(), "path", target)
		fmt.Fprintln(cmd.OutOrStdout())
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle("Run this to reload your shell config:", ansiFeatureLabelStyle))
		fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle(`source ~/.bashrc`, ansiFeatureCommandStyle))
		return nil
	default:
		return fmt.Errorf("unsupported shell %q", shell)
	}
}

func (a *App) runRelaunch(cmd *cobra.Command, force bool) error {
	activeThreads, err := sessions.ListActiveThreads(a.Paths, a.Now())
	if err != nil {
		printInfoWarning(cmd.OutOrStdout(), fmt.Sprintf("Unable to inspect active Codex threads: %v", err))
		fmt.Fprintln(cmd.OutOrStdout())
		activeThreads = nil
	}

	confirmed, err := confirmOptionalAction(cmd, "Relaunch Codex App now?")
	if err != nil {
		return err
	}
	if !confirmed {
		printInfoWarning(cmd.OutOrStdout(), "Skipped Codex App relaunch. Restart the app manually to apply the new account.")
		fmt.Fprintln(cmd.OutOrStdout())
		return a.runCurrent(cmd)
	}

	if len(activeThreads) > 0 {
		printInfoWarning(cmd.OutOrStdout(), fmt.Sprintf("Detected %d active Codex thread(s). Relaunching now may interrupt them.", len(activeThreads)))
		fmt.Fprintln(cmd.OutOrStdout())
		printActiveThreads(cmd.OutOrStdout(), activeThreads)
		fmt.Fprintln(cmd.OutOrStdout())

		confirmed, err = confirmOptionalAction(cmd, "Relaunch anyway?")
		if err != nil {
			return err
		}
		if !confirmed {
			printInfoWarning(cmd.OutOrStdout(), "Skipped Codex App relaunch to avoid interrupting active threads.")
			fmt.Fprintln(cmd.OutOrStdout())
			return a.runCurrent(cmd)
		}
	}

	fmt.Fprintln(cmd.OutOrStdout(), colorize("Relaunching Codex App..."))
	return relaunchCodexApp(force)
}

func (a *App) showNamedRows(cmd *cobra.Command, names []string, localOnly bool) error {
	if err := a.ensureConfig(); err != nil {
		return err
	}
	rows := accounts.BuildListRows(a.Paths, a.Config, a.Client, localOnly, a.Now())
	filtered := accounts.ListRowsResult{
		Rows:           []accounts.ListRow{},
		CurrentIndices: map[int]struct{}{},
		Notes:          []string{},
	}
	nameSet := map[string]struct{}{}
	for _, name := range names {
		nameSet[name] = struct{}{}
	}
	for index, row := range rows.Rows {
		accountName := row.Account
		if strings.Contains(accountName, " <") {
			accountName = strings.SplitN(accountName, " <", 2)[0]
		}
		if _, ok := nameSet[accountName]; !ok {
			continue
		}
		filtered.Rows = append(filtered.Rows, row)
		if _, ok := rows.CurrentIndices[index]; ok {
			filtered.CurrentIndices[len(filtered.Rows)-1] = struct{}{}
		}
	}
	for _, note := range rows.Notes {
		name := strings.SplitN(note, ":", 2)[0]
		if _, ok := nameSet[name]; ok {
			filtered.Notes = append(filtered.Notes, note)
		}
	}
	printListRows(cmd, filtered)
	return nil
}

func completeAccountNames(paths config.Paths) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		all := accounts.ListAccountNames(paths)
		filtered := []string{}
		for _, name := range all {
			if strings.HasPrefix(name, toComplete) {
				filtered = append(filtered, name)
			}
		}
		return filtered, cobra.ShellCompDirectiveNoFileComp
	}
}

func completeHermesAccountNames(paths config.Paths) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		names := hermesaccounts.ListAvailableAccountNames(paths)
		filtered := []string{}
		for _, name := range names {
			if strings.HasPrefix(strings.ToLower(name), strings.ToLower(toComplete)) {
				filtered = append(filtered, name)
			}
		}
		return filtered, cobra.ShellCompDirectiveNoFileComp
	}
}

func completeRenameArgs(paths config.Paths) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return completeAccountNames(paths)(nil, nil, toComplete)
	}
}

func shouldSkipRuntimePreparation(cmd *cobra.Command) bool {
	skipped := []string{"completion", "install-completion", "hermes", "__complete", "__completeNoDesc"}
	for current := cmd; current != nil; current = current.Parent() {
		if slices.Contains(skipped, current.Name()) {
			return true
		}
	}
	return false
}

func resolveCodexBin(cfg config.Config) string {
	if fromEnv := strings.TrimSpace(os.Getenv("CODEX_SWITCH_CODEX_BIN")); fromEnv != "" {
		return fromEnv
	}
	if fromConfig := strings.TrimSpace(cfg.CodexBin); fromConfig != "" {
		return fromConfig
	}
	path, _ := execLookPath("codex")
	return path
}

func (a *App) ensureConfig() error {
	if a.Client == nil {
		a.Client = &http.Client{}
	}
	if a.Now == nil {
		a.Now = time.Now
	}
	if a.ConfigLoaded {
		return nil
	}
	cfg, err := config.Load(a.Paths)
	if err != nil {
		return err
	}
	a.Config = cfg
	a.ConfigLoaded = true
	return nil
}

func printWarningsOrSummary(cmd *cobra.Command, warnings, refreshNotes, refreshedNames, updated []string, force bool) error {
	allNotes := append([]string{}, warnings...)
	allNotes = append(allNotes, refreshNotes...)
	if len(allNotes) > 0 {
		for _, note := range allNotes {
			fmt.Fprintln(cmd.OutOrStdout(), colorizeWithStyle(note, ansiFeatureWarningStyle))
		}
		return nil
	}

	if len(refreshedNames) > 0 {
		prefix := "Refreshed"
		if force {
			prefix = "Force refreshed"
		}
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("%s: %s", prefix, strings.Join(refreshedNames, ", "))))
	}
	if len(updated) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Synced aliases: %s", strings.Join(updated, ", "))))
	}
	if len(refreshedNames) == 0 && len(updated) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), colorize("Already up to date"))
	}
	fmt.Fprintln(cmd.OutOrStdout())
	return nil
}

func printSummaryOnly(cmd *cobra.Command, refreshedNames, updated []string, force bool) error {
	if len(refreshedNames) > 0 {
		prefix := "Refreshed"
		if force {
			prefix = "Force refreshed"
		}
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("%s: %s", prefix, strings.Join(refreshedNames, ", "))))
	}
	if len(updated) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), colorize(fmt.Sprintf("Synced aliases: %s", strings.Join(updated, ", "))))
	}
	if len(refreshedNames) == 0 && len(updated) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), colorize("Already up to date"))
	}
	fmt.Fprintln(cmd.OutOrStdout())
	return nil
}

func emitSyncProgress(writer io.Writer, renderer *syncProgressRenderer, account, status, detail, style string) {
	if renderer != nil {
		renderer.Append([]string{account, status, detail}, style)
		return
	}
	printSyncProgressRow(writer, []string{account, status, detail}, style)
}

func printSyncProgressHeader(writer io.Writer) {
	fmt.Fprintln(writer, colorizeWithStyle(formatSyncProgressLine("ACCOUNT", "STATUS", "DETAIL"), ansiListHeaderStyle))
}

func printSyncProgressRow(writer io.Writer, row []string, style string) {
	if len(row) < 3 {
		return
	}
	fmt.Fprintln(writer, colorizeWithStyle(formatSyncProgressLine(row[0], row[1], row[2]), style))
}

func formatSyncProgressLine(account, status, detail string) string {
	cells := []string{
		padRight(account, 32),
		padRight(status, 10),
		detail,
	}
	return strings.TrimRight(strings.Join(cells, "  "), " ")
}

func accountLabelFromPath(name, path string, now time.Time) string {
	snapshot, err := accounts.ReadSnapshot(path, now)
	if err != nil {
		return name
	}
	return formatAccountLabel(name, snapshot.Email)
}

func formatAccountLabel(name, email string) string {
	if strings.TrimSpace(email) == "" || strings.TrimSpace(email) == "-" {
		return name
	}
	return fmt.Sprintf("%s <%s>", name, email)
}

func filterStartupWarnings(warnings []string) []string {
	filtered := make([]string, 0, len(warnings))
	for _, warning := range warnings {
		switch warning {
		case "Not logged in.", "No saved aliases match the current account.":
			continue
		default:
			filtered = append(filtered, warning)
		}
	}
	return filtered
}

func confirmAction(cmd *cobra.Command, assumeYes bool, prompt string) error {
	if assumeYes {
		return nil
	}

	confirmed, err := confirmOptionalAction(cmd, prompt)
	if err != nil {
		return err
	}
	if confirmed {
		return nil
	}
	return fmt.Errorf("cancelled")
}

func confirmOptionalAction(cmd *cobra.Command, prompt string) (bool, error) {
	input := cmd.InOrStdin()
	fmt.Fprintf(cmd.OutOrStdout(), "%s %s ", colorizeWithStyle(prompt, ansiFeatureWarningStyle), colorizeWithStyle("[y/N]", ansiFeatureLabelStyle))
	reply, err := readConfirmationLine(input)
	if err != nil && len(reply) == 0 {
		if file, ok := input.(*os.File); ok {
			if info, statErr := file.Stat(); statErr == nil && info.Mode()&os.ModeCharDevice == 0 {
				return false, fmt.Errorf("%s rerun with --yes or pipe `yes` to confirm", prompt)
			}
		}
		return false, fmt.Errorf("confirmation cancelled")
	}
	reply = strings.ToLower(strings.TrimSpace(reply))
	if reply == "y" || reply == "yes" {
		return true, nil
	}
	return false, nil
}

func readConfirmationLine(reader io.Reader) (string, error) {
	var builder strings.Builder
	buffer := make([]byte, 1)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			builder.WriteByte(buffer[0])
			if buffer[0] == '\n' {
				return builder.String(), nil
			}
		}
		if err != nil {
			if err == io.EOF && builder.Len() > 0 {
				return builder.String(), err
			}
			return builder.String(), err
		}
	}
}

func filterCurrentRows(rows accounts.ListRowsResult) accounts.ListRowsResult {
	filtered := accounts.ListRowsResult{
		Rows:           []accounts.ListRow{},
		CurrentIndices: map[int]struct{}{},
		Notes:          []string{},
	}
	for index, row := range rows.Rows {
		if _, ok := rows.CurrentIndices[index]; !ok {
			continue
		}
		filtered.Rows = append(filtered.Rows, row)
		filtered.CurrentIndices[len(filtered.Rows)-1] = struct{}{}
	}
	for _, note := range rows.Notes {
		name := strings.SplitN(note, ":", 2)[0]
		for _, row := range filtered.Rows {
			if strings.HasPrefix(row.Account, name+" <") || row.Account == name {
				filtered.Notes = append(filtered.Notes, note)
			}
		}
	}
	return filtered
}

func printListRows(cmd *cobra.Command, result accounts.ListRowsResult) {
	printListRowsWithoutNotes(cmd, result)
	printNotes(cmd, result.Notes)
}

func printListRowsWithoutNotes(cmd *cobra.Command, result accounts.ListRowsResult) {
	rows := make([][]string, 0, len(result.Rows))
	rowStyles := make([][]string, 0, len(result.Rows))
	for index, row := range result.Rows {
		rows = append(rows, []string{row.Marker, row.Ready, row.Account, row.Plan, row.FiveHour, row.Weekly, row.LastChecked})
		style := ansiListRowStyle
		if _, ok := result.CurrentIndices[index]; ok {
			style = ansiListCurrentRowStyle
		}
		rowStyles = append(rowStyles, []string{style, style, style, style, style, style, style})
	}
	printColorTable(
		cmd.OutOrStdout(),
		[]string{"", "READY", "ACCOUNT", "PLAN", "5H USAGE", "WEEKLY USAGE", "LAST CHECKED"},
		rows,
		rowStyles,
		ansiListHeaderStyle,
	)
}

func printStreamingListHeader(writer io.Writer) {
	line := formatStreamingListLine("", "READY", "ACCOUNT", "PLAN", "5H USAGE", "WEEKLY USAGE", "LAST CHECKED")
	fmt.Fprintln(writer, colorizeWithStyle(line, ansiListHeaderStyle))
}

func printStreamingListRow(writer io.Writer, row accounts.ListRow, isCurrent bool) {
	style := ansiListRowStyle
	if isCurrent {
		style = ansiListCurrentRowStyle
	}
	line := formatStreamingListLine(row.Marker, row.Ready, row.Account, row.Plan, row.FiveHour, row.Weekly, row.LastChecked)
	fmt.Fprintln(writer, colorizeWithStyle(line, style))
}

func formatStreamingListLine(marker, ready, account, plan, fiveHour, weekly, lastChecked string) string {
	cells := []string{
		padRight(marker, 1),
		padRight(ready, 5),
		padRight(account, 32),
		padRight(plan, 24),
		padRight(fiveHour, 18),
		padRight(weekly, 20),
		lastChecked,
	}
	return strings.TrimRight(strings.Join(cells, " "), " ")
}

type streamingListRenderer struct {
	cmd           *cobra.Command
	result        accounts.ListRowsResult
	renderedLines int
}

func newStreamingListRenderer(cmd *cobra.Command) *streamingListRenderer {
	return &streamingListRenderer{
		cmd: cmd,
		result: accounts.ListRowsResult{
			Rows:           []accounts.ListRow{},
			CurrentIndices: map[int]struct{}{},
			Notes:          []string{},
		},
	}
}

func (r *streamingListRenderer) Append(event accounts.ListRowEvent) {
	r.result.Rows = append(r.result.Rows, event.Row)
	if event.IsCurrent {
		r.result.CurrentIndices[len(r.result.Rows)-1] = struct{}{}
	}
	if event.Note != "" {
		r.result.Notes = append(r.result.Notes, event.Note)
	}
	r.Render()
}

func (r *streamingListRenderer) Render() {
	if r.renderedLines > 0 {
		fmt.Fprintf(r.cmd.OutOrStdout(), "\033[%dA\033[J", r.renderedLines)
	}
	printListRowsWithoutNotes(r.cmd, r.result)
	r.renderedLines = 1 + len(r.result.Rows)
}

func (r *streamingListRenderer) PrintNotes() {
	printNotes(r.cmd, r.result.Notes)
}

func printDoctorDetailRowsWithoutNotes(cmd *cobra.Command, rows [][]string) {
	printTable(cmd.OutOrStdout(), []string{"ACCOUNT", "JSON", "ACCOUNT ID", "ACCESS TOKEN", "LIVE USAGE"}, rows)
}

func printDoctorDetailHeader(writer io.Writer) {
	line := formatDoctorDetailLine("ACCOUNT", "JSON", "ACCOUNT ID", "ACCESS TOKEN", "LIVE USAGE")
	fmt.Fprintln(writer, colorizeWithStyle(line, ansiListHeaderStyle))
}

func printDoctorDetailRow(writer io.Writer, row []string) {
	if len(row) < 5 {
		return
	}
	line := formatDoctorDetailLine(row[0], row[1], row[2], row[3], row[4])
	fmt.Fprintln(writer, colorizeWithStyle(line, ansiListRowStyle))
}

func formatDoctorDetailLine(account, jsonStatus, accountID, accessToken, liveUsage string) string {
	cells := []string{
		padRight(account, 16),
		padRight(jsonStatus, 4),
		padRight(accountID, 10),
		padRight(accessToken, 12),
		liveUsage,
	}
	return strings.TrimRight(strings.Join(cells, "  "), " ")
}

type doctorDetailRenderer struct {
	cmd           *cobra.Command
	rows          [][]string
	notes         []string
	renderedLines int
}

func newDoctorDetailRenderer(cmd *cobra.Command) *doctorDetailRenderer {
	return &doctorDetailRenderer{
		cmd:   cmd,
		rows:  [][]string{},
		notes: []string{},
	}
}

func (r *doctorDetailRenderer) Append(result doctorDetailResult) {
	r.rows = append(r.rows, result.Row)
	if result.Note != "" {
		r.notes = append(r.notes, result.Note)
	}
	r.Render()
}

func (r *doctorDetailRenderer) Render() {
	if r.renderedLines > 0 {
		fmt.Fprintf(r.cmd.OutOrStdout(), "\033[%dA\033[J", r.renderedLines)
	}
	printDoctorDetailRowsWithoutNotes(r.cmd, r.rows)
	r.renderedLines = 1 + len(r.rows)
}

func (r *doctorDetailRenderer) PrintNotes() {
	printNotes(r.cmd, r.notes)
}

type syncProgressRenderer struct {
	cmd           *cobra.Command
	rows          [][]string
	styles        []string
	renderedLines int
}

func newSyncProgressRenderer(cmd *cobra.Command) *syncProgressRenderer {
	return &syncProgressRenderer{
		cmd:    cmd,
		rows:   [][]string{},
		styles: []string{},
	}
}

func (r *syncProgressRenderer) Append(row []string, style string) {
	r.rows = append(r.rows, row)
	r.styles = append(r.styles, style)
	r.Render()
}

func (r *syncProgressRenderer) Render() {
	if r.renderedLines > 0 {
		fmt.Fprintf(r.cmd.OutOrStdout(), "\033[%dA\033[J", r.renderedLines)
	}
	printColorTable(
		r.cmd.OutOrStdout(),
		[]string{"ACCOUNT", "STATUS", "DETAIL"},
		r.rows,
		r.syncCellStyles(),
		ansiListHeaderStyle,
	)
	r.renderedLines = 1 + len(r.rows)
}

func (r *syncProgressRenderer) syncCellStyles() [][]string {
	cellStyles := make([][]string, 0, len(r.rows))
	for index := range r.rows {
		style := ansiListRowStyle
		if index < len(r.styles) && strings.TrimSpace(r.styles[index]) != "" {
			style = r.styles[index]
		}
		cellStyles = append(cellStyles, []string{style, style, style})
	}
	return cellStyles
}

func printActiveThreads(writer interface{ Write([]byte) (int, error) }, activeThreads []sessions.ActiveThread) {
	rows := make([][]string, 0, len(activeThreads))
	for _, thread := range activeThreads {
		rows = append(rows, []string{
			sessions.FormatThreadLabel(thread),
			fallback(strings.TrimSpace(thread.Status), "-"),
			support.FormatISO8601(formatTime(thread.LastActiveAt)),
			support.FormatISO8601(formatTime(thread.LastTaskStartedAt)),
			thread.SessionID,
		})
	}
	printTable(writer, []string{"THREAD", "STATUS", "LAST ACTIVE", "TURN STARTED", "SESSION ID"}, rows)
}

func lifetime(token string) string {
	issued := auth.IssuedAtUnix(token)
	expires := auth.ExpirationUnix(token)
	if issued == nil || expires == nil {
		return "-"
	}
	return support.MustDurationString(time.Unix(*expires, 0).Sub(time.Unix(*issued, 0)))
}

func refreshTokenDetails(token string) string {
	if strings.TrimSpace(token) == "" {
		return "-"
	}
	if auth.IssuedAtUnix(token) != nil || auth.ExpirationUnix(token) != nil {
		return "jwt"
	}
	return "opaque"
}

func present(value string) string {
	if strings.TrimSpace(value) == "" {
		return "missing"
	}
	return "present"
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func status(ok bool) string {
	if ok {
		return "ok"
	}
	return "missing"
}

func boolStatus(value bool, truthy, falsy string) string {
	if value {
		return truthy
	}
	return falsy
}

func colorize(text string) string {
	return colorizeWithStyle(text, ansiFeatureSuccessStyle)
}

func formatTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

const (
	ansiReset               = "\033[0m"
	ansiFeatureLabelStyle   = "\033[38;5;110m"
	ansiFeatureValueStyle   = "\033[38;5;245m"
	ansiFeatureInfoStyle    = "\033[38;5;245m"
	ansiFeatureSuccessStyle = "\033[38;5;151m"
	ansiFeatureCommandStyle = "\033[38;5;151m"
	ansiFeatureWarningStyle = "\033[38;5;214m"
	ansiHelpTitleStyle      = "\033[1;97m"
	ansiHelpSectionStyle    = "\033[1;4;97m"
	ansiHelpLeftStyle       = "\033[1;97m"
	ansiHelpRightStyle      = "\033[38;5;245m"
	ansiHelpUsageStyle      = "\033[37m"
	ansiListHeaderStyle     = "\033[38;5;110m"
	ansiListRowStyle        = "\033[38;5;245m"
	ansiListCurrentRowStyle = "\033[38;5;151m"
)

func colorizeWithStyle(text, style string) string {
	if !isTTY() || strings.TrimSpace(style) == "" {
		return text
	}
	return style + text + ansiReset
}

func fallback(value, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return value
}

func printHeadline(writer interface{ Write([]byte) (int, error) }, text string) {
	fmt.Fprintln(writer, colorizeWithStyle(text, ansiFeatureLabelStyle))
}

func printSuccess(writer interface{ Write([]byte) (int, error) }, text string) {
	fmt.Fprintln(writer, colorizeWithStyle(text, ansiFeatureSuccessStyle))
}

func printInfo(writer interface{ Write([]byte) (int, error) }, text string) {
	fmt.Fprintln(writer, colorizeWithStyle(text, ansiFeatureInfoStyle))
}

func printInfoWarning(writer interface{ Write([]byte) (int, error) }, text string) {
	fmt.Fprintln(writer, colorizeWithStyle(text, ansiFeatureWarningStyle))
}

func printCommand(writer interface{ Write([]byte) (int, error) }, text string) {
	fmt.Fprintln(writer, colorizeWithStyle(text, ansiFeatureCommandStyle))
}

func printKeyValue(writer interface{ Write([]byte) (int, error) }, key, value string) {
	line := fmt.Sprintf("%s: %s", colorizeWithStyle(key, ansiFeatureLabelStyle), colorizeWithStyle(value, ansiFeatureValueStyle))
	if !isTTY() {
		line = fmt.Sprintf("%s: %s", key, value)
	}
	fmt.Fprintln(writer, line)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func renderHelp(writer interface{ Write([]byte) (int, error) }, cmd *cobra.Command) {
	title := strings.TrimSpace(cmd.Short)
	if title == "" {
		title = cmd.CommandPath()
	}
	if cmd.Parent() == nil {
		title = "Codex Switch CLI"
	}
	printHelpTitle(writer, title)
	if long := strings.TrimSpace(cmd.Long); long != "" && long != title {
		fmt.Fprintln(writer, colorizeWithStyle(long, ansiHelpRightStyle))
	}
	fmt.Fprintln(writer)

	printHelpSectionHeader(writer, "Usage:")
	printHelpUsageLines(writer, helpUsageLines(cmd))

	commands := availableCommandRows(cmd)
	if len(commands) > 0 {
		fmt.Fprintln(writer)
		printHelpSectionHeader(writer, "Commands:")
		printHelpRows(writer, commands)
	}

	options := optionRows(cmd)
	if len(options) > 0 {
		fmt.Fprintln(writer)
		printHelpSectionHeader(writer, "Options:")
		printHelpRows(writer, options)
	}
}

type helpRow struct {
	Left  string
	Right string
}

func helpUsageLines(cmd *cobra.Command) []string {
	lines := []string{normalizeHelpUsage(cmd.UseLine())}
	parent := cmd.Parent()
	if parent == nil && cmd.HasAvailableSubCommands() {
		lines = append(lines, normalizeHelpUsage(fmt.Sprintf("%s [OPTIONS] <COMMAND> [ARGS]", cmd.CommandPath())))
	}
	return lines
}

func normalizeHelpUsage(line string) string {
	replacer := strings.NewReplacer(
		"[flags]", "[OPTIONS]",
		"[Flags]", "[OPTIONS]",
		"[flags...]", "[OPTIONS]",
		"[Flags...]", "[OPTIONS]",
	)
	return replacer.Replace(line)
}

func availableCommandRows(cmd *cobra.Command) []helpRow {
	rows := []helpRow{}
	for _, child := range cmd.Commands() {
		if !child.IsAvailableCommand() || child.Hidden {
			continue
		}
		right := child.Short
		if len(child.Aliases) > 0 {
			right = fmt.Sprintf("%s [aliases: %s]", right, strings.Join(child.Aliases, ", "))
		}
		rows = append(rows, helpRow{Left: child.Name(), Right: right})
	}
	sortHelpRows(cmd, rows)
	return rows
}

func sortHelpRows(cmd *cobra.Command, rows []helpRow) {
	if len(rows) < 2 {
		return
	}

	order := map[string]int{}
	if cmd.Parent() == nil {
		order = map[string]int{
			"login":              0,
			"list":               1,
			"current":            2,
			"use":                3,
			"save":               4,
			"hermes":             5,
			"sync":               6,
			"token-info":         7,
			"rename":             8,
			"delete":             9,
			"prune":              10,
			"doctor":             11,
			"install-completion": 12,
			"completion":         13,
			"help":               14,
		}
	}

	slices.SortStableFunc(rows, func(left, right helpRow) int {
		leftRank, leftOk := order[left.Left]
		rightRank, rightOk := order[right.Left]
		switch {
		case leftOk && rightOk:
			return leftRank - rightRank
		case leftOk:
			return -1
		case rightOk:
			return 1
		default:
			return strings.Compare(left.Left, right.Left)
		}
	})
}

func optionRows(cmd *cobra.Command) []helpRow {
	flags := []*pflag.Flag{}
	cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, flag)
	})
	cmd.InheritedFlags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, flag)
	})

	rows := make([]helpRow, 0, len(flags))
	for _, flag := range flags {
		rows = append(rows, helpRow{
			Left:  formatFlagLabel(flag),
			Right: formatFlagUsage(flag),
		})
	}
	return rows
}

func formatFlagLabel(flag *pflag.Flag) string {
	parts := []string{}
	if flag.Shorthand != "" {
		parts = append(parts, "-"+flag.Shorthand)
	}
	parts = append(parts, "--"+flag.Name)
	label := strings.Join(parts, ", ")
	if flag.Value.Type() != "bool" {
		label += " <" + strings.ToUpper(flag.Value.Type()) + ">"
	}
	return label
}

func formatFlagUsage(flag *pflag.Flag) string {
	usage := flag.Usage
	if flag.DefValue == "" || flag.DefValue == "false" || flag.DefValue == "[]" {
		return usage
	}
	return fmt.Sprintf("%s (default: %s)", usage, flag.DefValue)
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
