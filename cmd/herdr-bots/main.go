package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/adapter"
	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/engine"
	"github.com/terry-li-hm/herdr-bots/internal/herdr"
	"github.com/terry-li-hm/herdr-bots/internal/pane"
	"github.com/terry-li-hm/herdr-bots/internal/service"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return flag.ErrHelp
	}
	switch args[0] {
	case "daemon":
		return daemonCmd(args[1:])
	case "list":
		return listCmd(args[1:])
	case "runs":
		return runsCmd(args[1:])
	case "show":
		return showCmd(args[1:])
	case "pane":
		return paneCmd(args[1:])
	case "run":
		return runCmd(args[1:])
	case "enqueue":
		return enqueueCmd(args[1:])
	case "cancel":
		return cancelCmd(args[1:])
	case "pause":
		return pauseCmd(args[1:], true)
	case "resume":
		return pauseCmd(args[1:], false)
	case "doctor":
		return doctorCmd(args[1:])
	case "service":
		return serviceCmd(args[1:])
	case "version", "--version", "-v":
		fmt.Println(currentVersion())
		return nil
	case "help", "--help", "-h":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// resolveVersion is pure so version fallback behavior can be tested without
// depending on how the test binary itself was linked. An explicit linker
// version wins; otherwise a module release version wins over VCS provenance.
func resolveVersion(linked string, info *debug.BuildInfo, ok bool) string {
	if linked != "" && linked != "dev" {
		return linked
	}
	if ok && info != nil {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
		var revision string
		modified := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
		if revision != "" {
			if modified {
				return revision + "-dirty"
			}
			return revision
		}
	}
	if linked != "" {
		return linked
	}
	return "dev"
}

func currentVersion() string {
	info, ok := debug.ReadBuildInfo()
	return resolveVersion(version, info, ok)
}

func usage() {
	fmt.Println(`herdr-bots - durable local coding-agent schedules

Usage:
  herdr-bots daemon [--config PATH] [--state PATH]
  herdr-bots list [--config PATH] [--state PATH]
  herdr-bots runs [JOB] [--state PATH]
  herdr-bots show RUN [--state PATH]
  herdr-bots pane [--state PATH]
  herdr-bots run JOB [--config PATH] [--state PATH] [--canary]
  herdr-bots enqueue JOB --event-id ID [--config PATH] [--state PATH]
  herdr-bots cancel RUN [--config PATH] [--state PATH]
  herdr-bots pause JOB|--all [--state PATH]
  herdr-bots resume JOB|--all [--state PATH]
  herdr-bots doctor [--config PATH] [--state PATH]
  herdr-bots service render [--config PATH] [--state PATH]

Service rendering is read-only. Installing, loading, or changing a launchd
service remains a separate explicit action.`)
}

func common(fs *flag.FlagSet) (*string, *string) {
	configPath := fs.String("config", defaultConfigPath(), "job definition file")
	statePath := fs.String("state", defaultStatePath(), "SQLite state file")
	return configPath, statePath
}

func openEngine(configPath, statePath string) (*engine.Engine, *store.Store, error) {
	if err := os.MkdirAll(filepath.Dir(statePath), 0o700); err != nil {
		return nil, nil, err
	}
	state, err := store.Open(statePath)
	if err != nil {
		return nil, nil, err
	}
	return engine.New(state, herdr.New(), adapter.ExecRunner{}, configPath), state, nil
}

func daemonCmd(args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	configPath, statePath := common(fs)
	interval := fs.Duration("interval", 30*time.Second, "schedule poll interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval < time.Second {
		return fmt.Errorf("interval must be at least one second")
	}
	eng, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := eng.Reconcile(ctx); err != nil {
		return err
	}
	if err := eng.Evaluate(ctx, time.Now()); err != nil {
		log.Printf("initial evaluation: %v", err)
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	log.Printf("scheduler active: config=%s state=%s", *configPath, *statePath)
	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler stopping")
			return nil
		case now := <-ticker.C:
			if err := eng.Reconcile(ctx); err != nil {
				log.Printf("reconciliation: %v", err)
				continue
			}
			if err := eng.Evaluate(ctx, now); err != nil {
				log.Printf("evaluation: %v", err)
			}
		}
	}
}

func listCmd(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	configPath, statePath := common(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	_, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	fmt.Printf("%-24s %-8s %-10s %-18s %-22s %-20s %-10s\n", "JOB", "ENABLED", "HARNESS", "PROVIDER", "MODEL", "PAUSED", "LAST")
	ctx := context.Background()
	for _, job := range cfg.Jobs {
		last := "never"
		if result, resultErr := state.LastJobResult(ctx, job.ID); resultErr == nil && result != "" {
			last = result
		}
		// The durable pause reason is reported without touching run read state.
		paused := "-"
		if jobState, stateErr := state.Job(ctx, job.ID); stateErr == nil && jobState.Paused {
			paused = jobState.PauseReason
			if paused == "" {
				paused = "paused"
			}
		}
		provider := job.Execution.Provider
		if provider == "" {
			provider = "-"
		}
		fmt.Printf("%-24s %-8t %-10s %-18s %-22s %-20s %-10s\n", job.ID, job.IsEnabled(), job.Execution.Harness, provider, job.Execution.Model, paused, last)
	}
	return nil
}

func runsCmd(args []string) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	statePath := fs.String("state", defaultStatePath(), "SQLite state file")
	limit := fs.Int("limit", 30, "maximum rows")
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	jobID := ""
	if fs.NArg() > 1 {
		return fmt.Errorf("runs takes at most one job id")
	}
	if fs.NArg() == 1 {
		jobID = fs.Arg(0)
	}
	state, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	runs, err := state.ListRunsGroupedByAcceptance(context.Background(), jobID, *limit)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("No runs.")
		return nil
	}
	fmt.Printf("%-1s %-38s %-22s %-10s %-11s %-18s %-12s %s\n", "", "RUN", "JOB", "VERDICT", "LANE", "REASON", "STATE", "UPDATED")
	for _, run := range runs {
		unread := " "
		if run.Unread {
			unread = "*"
		}
		fmt.Printf("%-1s %-38s %-22s %-10s %-11s %-18s %-12s %s\n", unread, run.ID, run.JobID, run.TaskVerdict, displayField(run.AcceptanceLane), displayField(run.AcceptanceReason), run.State, run.UpdatedAt.Local().Format("2006-01-02 15:04"))
	}
	return nil
}

func showCmd(args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	statePath := fs.String("state", defaultStatePath(), "SQLite state file")
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("show requires one run id")
	}
	state, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	run, err := state.GetRun(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	fmt.Printf("run: %s\njob: %s\nstate: %s\ninfrastructure: %s\nagent: %s\nverdict: %s\nlane: %s\nreason: %s\nsource-base: %s\nsource: %s\nworkspace: %s\npane: %s\nworktree: %s\nerror: %s %s\n", run.ID, run.JobID, run.State, run.InfrastructureResult, run.AgentResult, run.TaskVerdict, displayField(run.AcceptanceLane), displayField(run.AcceptanceReason), run.SourceBaseRevision, run.SourceRevision, run.WorkspaceID, run.PaneID, run.WorktreePath, run.ErrorCode, run.ErrorDetail)
	return state.MarkRead(context.Background(), run.ID)
}

func paneCmd(args []string) error {
	fs := flag.NewFlagSet("pane", flag.ContinueOnError)
	statePath := fs.String("state", defaultStatePath(), "SQLite state file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("pane takes no positional arguments")
	}
	state, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	return pane.Run(state, herdr.New())
}

func enqueueCmd(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	configPath, statePath := common(fs)
	eventID := fs.String("event-id", "", "stable event identity")
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("enqueue requires one job id")
	}
	if *eventID == "" {
		return fmt.Errorf("enqueue requires --event-id ID")
	}
	eng, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	result, err := eng.Enqueue(context.Background(), fs.Arg(0), *eventID, time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("event %s: %s run %s\n", *eventID, result.Outcome, result.Run.ID)
	return nil
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath, statePath := common(fs)
	canary := fs.Bool("canary", false, "label this as an attended canary")
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("run requires one job id")
	}
	eng, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run, err := eng.RunNow(ctx, fs.Arg(0), *canary, time.Now())
	if err != nil {
		return err
	}
	var snapshot config.Job
	if err := json.Unmarshal(run.Definition, &snapshot); err != nil {
		return fmt.Errorf("run %s has invalid snapshot: %w", run.ID, err)
	}
	deadline := time.Now().Add(time.Duration(snapshot.TimeoutMinutes)*time.Minute + 2*time.Minute)
	if *canary {
		fmt.Printf("canary run accepted: %s\n", run.ID)
	} else {
		fmt.Printf("run accepted: %s\n", run.ID)
	}
	return awaitRun(ctx, state, run.ID, deadline)
}

// awaitRun polls durable state until the run terminalizes. Executor ownership
// is not process-local, so another process holding the durable provisioning or
// start claim is waited on rather than falsely called stalled. The bounds are
// the accepted hold and the timeout-plus-two-minutes attended deadline.
func awaitRun(ctx context.Context, state *store.Store, runID string, deadline time.Time) error {
	for {
		current, err := state.GetRun(ctx, runID)
		if err != nil {
			return err
		}
		if terminal(current.State) {
			fmt.Printf("run %s: %s (%s)\n", current.ID, current.State, current.TaskVerdict)
			if current.State != store.StateSucceeded {
				return fmt.Errorf("run ended %s: %s", current.State, current.ErrorDetail)
			}
			return nil
		}
		if current.State == store.StateAccepted {
			return fmt.Errorf("run %s was accepted but held by pause, overlap, concurrency, or disk admission; inspect runs and doctor", runID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s exceeded the attended wait deadline in %s", runID, current.State)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func cancelCmd(args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	configPath, statePath := common(fs)
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("cancel requires one run id")
	}
	eng, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	if err := eng.Cancel(context.Background(), fs.Arg(0), time.Now()); err != nil {
		return err
	}
	fmt.Printf("cancelled %s\n", fs.Arg(0))
	return nil
}

func pauseCmd(args []string, paused bool) error {
	name := "pause"
	if !paused {
		name = "resume"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	statePath := fs.String("state", defaultStatePath(), "SQLite state file")
	all := fs.Bool("all", false, "apply the global pause")
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	state, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	if *all {
		if fs.NArg() != 0 {
			return fmt.Errorf("--all does not take a job id")
		}
		if err := state.SetGlobalPaused(context.Background(), paused); err != nil {
			return err
		}
		fmt.Printf("global pause: %t\n", paused)
		return nil
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s requires one job id or --all", name)
	}
	if err := state.SetPaused(context.Background(), fs.Arg(0), paused); err != nil {
		return err
	}
	fmt.Printf("%s: paused=%t\n", fs.Arg(0), paused)
	return nil
}

func doctorCmd(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	configPath, statePath := common(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	state, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	state.Close()
	if _, err := exec.LookPath("herdr"); err != nil {
		return fmt.Errorf("herdr is not on PATH")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, job := range cfg.Jobs {
		if _, err := os.Stat(job.Execution.Repository); err != nil {
			return fmt.Errorf("%s: repository: %w", job.ID, err)
		}
		if job.Execution.BaseRef != "" {
			if _, err := engine.ResolveSourceRevision(ctx, job.Execution.Repository, job.Execution.BaseRef); err != nil {
				return fmt.Errorf("%s: base ref: %w", job.ID, err)
			}
		}
		capacity, err := engine.ProbeDiskCapacity(job.Execution.Repository)
		if err != nil {
			return fmt.Errorf("%s: disk capacity: %w", job.ID, err)
		}
		requiredGiB := cfg.Capacity.MinFreeDisk() + job.DiskReserve()
		if capacity.FreeGiB < requiredGiB {
			return fmt.Errorf("%s: disk capacity %.2f GiB is below %.2f GiB candidate requirement", job.ID, capacity.FreeGiB, requiredGiB)
		}
		if err := adapter.Probe(ctx, adapter.ExecRunner{}, job); err != nil {
			return fmt.Errorf("%s: %w", job.ID, err)
		}
		fmt.Printf("ok  %-24s %s %s/%s disk=%.2fGiB reserve=%.2fGiB\n", job.ID, job.Execution.Harness, job.Execution.Provider, job.Execution.Model, capacity.FreeGiB, job.DiskReserve())
	}
	fmt.Printf("ok  capacity max=%d min-free=%.2fGiB\nok  config %s\nok  state %s\n", cfg.Capacity.MaxConcurrent(), cfg.Capacity.MinFreeDisk(), *configPath, *statePath)
	return nil
}

func serviceCmd(args []string) error {
	if len(args) == 0 || args[0] != "render" {
		return fmt.Errorf("service supports only the read-only render command")
	}
	fs := flag.NewFlagSet("service render", flag.ContinueOnError)
	configPath, statePath := common(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return err
	}
	home, _ := os.UserHomeDir()
	path := os.Getenv("PATH")
	if path == "" {
		path = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
	}
	logPath := filepath.Join(home, ".local", "state", "herdr-bots", "herdr-bots.log")
	raw, err := service.RenderLaunchd(service.LaunchdConfig{Binary: binary, ConfigPath: *configPath, StatePath: *statePath, LogPath: logPath, Home: home, Path: path})
	if err != nil {
		return err
	}
	fmt.Print(string(raw))
	return nil
}

func positionalFirst(args []string) []string {
	if len(args) > 1 && !strings.HasPrefix(args[0], "-") {
		out := append([]string(nil), args[1:]...)
		return append(out, args[0])
	}
	return args
}

func terminal(state string) bool {
	switch state {
	case store.StateSucceeded, store.StateFailed, store.StateBlocked, store.StateTimedOut, store.StateCancelled, store.StateInterrupted:
		return true
	}
	return false
}

func displayField(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func defaultConfigPath() string {
	if value := os.Getenv("HERDR_BOTS_CONFIG"); value != "" {
		return value
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "herdr-bots", "bots.yaml")
}
func defaultStatePath() string {
	if value := os.Getenv("HERDR_BOTS_STATE"); value != "" {
		return value
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", "herdr-bots", "state.sqlite3")
}
