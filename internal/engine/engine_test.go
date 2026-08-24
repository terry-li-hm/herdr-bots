package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/adapter"
	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/herdr"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

type fakeCommands struct {
	models       string
	probeGate    <-chan struct{} // when set, pi auth probes block until it closes
	probeStarted chan<- struct{}
}

func (f fakeCommands) LookPath(name string) (string, error) { return "/bin/" + name, nil }
func (f fakeCommands) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name == "claude" && len(args) > 1 && args[0] == "auth" && args[1] == "status" {
		return []byte(`{"loggedIn":true,"authMethod":"claude.ai"}`), nil
	}
	if name != "pi" {
		return nil, errors.New("unexpected command")
	}
	if len(args) > 0 && args[0] == "auth" {
		if f.probeStarted != nil {
			select {
			case f.probeStarted <- struct{}{}:
			default:
			}
		}
		if f.probeGate != nil {
			select {
			case <-f.probeGate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []byte(`{"status":"ready","provider":"openai-codex"}`), nil
	}
	if len(args) > 0 && args[0] == "--list-models" {
		return []byte(f.models), nil
	}
	return nil, errors.New("unexpected pi command")
}

type fakeHerdr struct {
	mu                sync.Mutex
	provisions        int
	kind              string
	args              []string
	status            string
	path              string
	command           string
	commandCode       int
	commandErr        error
	agentWaitErr      error
	waitCh            <-chan struct{}
	provisionCreated  chan<- struct{}
	provisionGate     <-chan struct{}
	provisionReceipt  herdr.Receipt
	provisionErr      error
	submitStarted     chan<- struct{}
	submitGate        <-chan struct{}
	startAgentCount   int
	startAgentErr     error
	submitCount       int
	submitErr         error
	statusCount       int
	closeCount        int
	closeErr          error
	closeStarted      chan<- struct{}
	closeGate         <-chan struct{}
	baseRef           string
	prompt            string
	foundReceipt      herdr.Receipt
	foundWorkspace    bool
	workspaceExists   bool
	workspaceExistSet bool
}

func (f *fakeHerdr) WorkspaceExists(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCount++
	if !f.workspaceExistSet {
		return true, nil
	}
	return f.workspaceExists, nil
}

func (f *fakeHerdr) FindWorkspaceByBranch(context.Context, string, string) (herdr.Receipt, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.foundReceipt, f.foundWorkspace, nil
}

func (f *fakeHerdr) Provision(ctx context.Context, repo, workspace, baseRef, branch, label string) (herdr.Receipt, error) {
	f.mu.Lock()
	f.provisions++
	f.baseRef = baseRef
	created, gate, path := f.provisionCreated, f.provisionGate, f.path
	f.mu.Unlock()
	if created != nil {
		select {
		case created <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return herdr.Receipt{}, ctx.Err()
		}
	}
	f.mu.Lock()
	override, provisionErr := f.provisionReceipt, f.provisionErr
	f.mu.Unlock()
	if override.WorkspaceID != "" || provisionErr != nil {
		if override.Branch == "" {
			override.Branch = branch
		}
		return override, provisionErr
	}
	return herdr.Receipt{WorkspaceID: "w1", PaneID: "p1", Branch: branch, Path: path}, nil
}
func (f *fakeHerdr) StartAgent(_ context.Context, name, kind, pane string, args []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startAgentCount++
	f.kind = kind
	f.args = append([]string(nil), args...)
	return f.startAgentErr
}
func (f *fakeHerdr) Submit(ctx context.Context, _ string, prompt string) error {
	f.mu.Lock()
	f.submitCount++
	f.prompt = prompt
	started, gate := f.submitStarted, f.submitGate
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.submitErr
}
func (f *fakeHerdr) Wait(ctx context.Context, _ string, _ time.Duration) (string, error) {
	if f.agentWaitErr != nil {
		return "", f.agentWaitErr
	}
	if f.waitCh != nil {
		select {
		case <-f.waitCh:
			return "idle", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if f.status == "" {
		return "idle", nil
	}
	return f.status, nil
}
func (f *fakeHerdr) Status(context.Context, string) (string, error) {
	f.mu.Lock()
	f.statusCount++
	status := f.status
	f.mu.Unlock()
	if status == "" {
		return "idle", nil
	}
	return status, nil
}
func (f *fakeHerdr) StartCommand(_ context.Context, _ string, command string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.command = command
	return nil
}
func (f *fakeHerdr) WaitCommand(context.Context, string, string, time.Duration) (int, error) {
	return f.commandCode, f.commandErr
}
func (f *fakeHerdr) CommandResult(context.Context, string, string) (int, bool, error) {
	return f.commandCode, true, nil
}
func (f *fakeHerdr) CloseWorkspace(ctx context.Context, _ string) error {
	f.mu.Lock()
	f.closeCount++
	started, gate, closeErr := f.closeStarted, f.closeGate, f.closeErr
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return closeErr
}

func writeJobs(t *testing.T, repo string, enabled bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "automations.yaml")
	body := `version: 1
jobs:
  - id: docs-drift
    revision: 1
    enabled: ` + boolText(enabled) + `
    schedule:
      kind: cron
      expression: "0 9 * * *"
      timezone: Asia/Hong_Kong
      catch_up_grace_minutes: 0
    execution:
      repository: ` + repo + `
      workspace: worktree
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    prompt: Check docs.
    timeout_minutes: 1
    overlap: forbid
    verifier:
      command: ["/usr/bin/true"]
    limits:
      max_runs_per_day: 1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func writeEventJobs(t *testing.T, repo string, enabled bool) string {
	t.Helper()
	path := writeJobs(t, repo, enabled)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(raw), "kind: cron\n      expression: \"0 9 * * *\"", "kind: event", 1)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newEventEngine(t *testing.T, enabled bool) (*Engine, *store.Store, *fakeHerdr) {
	t.Helper()
	repo := t.TempDir()
	configPath := writeEventJobs(t, repo, enabled)
	state, err := store.Open(filepath.Join(filepath.Dir(configPath), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	return eng, state, client
}

func newTestEngine(t *testing.T, enabled bool, models string) (*Engine, *store.Store, *fakeHerdr) {
	t.Helper()
	repo := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	eng := New(state, client, fakeCommands{models: models}, writeJobs(t, repo, enabled))
	// Deterministic headroom regardless of the host volume running the tests.
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	return eng, state, client
}

func TestEvaluateRunsOneVerifiedOccurrence(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateSucceeded || run.TaskVerdict != "passed" {
		t.Fatalf("run=%+v", run)
	}
	artifacts, err := os.ReadDir(filepath.Join(state.StateDir(), "verifiers"))
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("terminal verifier artifacts were retained: %v", artifacts)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 1 || client.kind != "pi" {
		t.Fatalf("client=%+v", client)
	}
	for _, arg := range client.args {
		if arg == "bash" || arg == "read,grep,find,ls,edit,write" {
			t.Fatalf("read-only route exposed write or shell: %v", client.args)
		}
	}
}

func TestEventScheduleDoesNotDispatchFromClockEvaluation(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	ctx := context.Background()
	for _, now := range []time.Time{mustTime("2020-01-01T00:00:00Z"), mustTime("2030-01-01T00:00:00Z")} {
		if err := eng.Evaluate(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := state.ListRuns(ctx, "docs-drift", 10)
	if err != nil || len(runs) != 0 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 {
		t.Fatalf("clock evaluation provisioned %d event workspaces", client.provisions)
	}
}

func TestAcceptedEventDispatchesOnceAfterRestart(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	ctx := context.Background()
	now := mustTime("2026-08-23T10:00:00+08:00")
	result, err := eng.Enqueue(ctx, "docs-drift", "health-20260823", now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Run == nil || result.Run.Trigger != "event" || result.Run.State != store.StateAccepted || result.Run.InputContext != "" {
		t.Fatalf("result=%+v", result)
	}
	client.mu.Lock()
	if client.provisions != 0 {
		client.mu.Unlock()
		t.Fatal("enqueue created a workspace before daemon dispatch")
	}
	client.mu.Unlock()

	restarted := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, eng.ConfigPath)
	restarted.DiskCapacity = eng.DiskCapacity
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.ID != result.Run.ID || run.State != store.StateSucceeded {
		t.Fatalf("run=%+v accepted=%+v", run, result.Run)
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 1 || client.submitCount != 1 {
		t.Fatalf("provisions=%d submits=%d", client.provisions, client.submitCount)
	}
}

func TestDuplicateEventIdentityReturnsOriginalRun(t *testing.T) {
	eng, state, _ := newEventEngine(t, true)
	now := mustTime("2026-08-23T10:00:00+08:00")
	first, err := eng.Enqueue(context.Background(), "docs-drift", "health-20260823", now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.Enqueue(context.Background(), "docs-drift", "health-20260823", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if first.Run == nil || second.Run == nil || first.Run.ID != second.Run.ID || !first.Inserted || second.Inserted {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	runs, err := state.ListRuns(context.Background(), "docs-drift", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestEventEnqueueRefusesDisabledAndPauseStates(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		eng, state, client := newEventEngine(t, false)
		_, err := eng.Enqueue(context.Background(), "docs-drift", "disabled", time.Now())
		if !errors.Is(err, store.ErrJobDisabled) {
			t.Fatalf("error=%v", err)
		}
		assertNoEventRunOrWorkspace(t, state, client)
	})
	for _, tc := range []struct {
		name string
		set  func(*store.Store) error
		want error
	}{
		{name: "job paused", set: func(state *store.Store) error { return state.SetPaused(context.Background(), "docs-drift", true) }, want: store.ErrJobPaused},
		{name: "globally paused", set: func(state *store.Store) error { return state.SetGlobalPaused(context.Background(), true) }, want: store.ErrGlobalPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, client := newEventEngine(t, true)
			if err := eng.Evaluate(context.Background(), mustTime("2026-08-23T09:00:00+08:00")); err != nil {
				t.Fatal(err)
			}
			if err := tc.set(state); err != nil {
				t.Fatal(err)
			}
			_, err := eng.Enqueue(context.Background(), "docs-drift", "paused", mustTime("2026-08-23T10:00:00+08:00"))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			assertNoEventRunOrWorkspace(t, state, client)
		})
	}
}

func assertNoEventRunOrWorkspace(t *testing.T, state *store.Store, client *fakeHerdr) {
	t.Helper()
	runs, err := state.ListRuns(context.Background(), "docs-drift", 10)
	if err != nil || len(runs) != 0 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 {
		t.Fatalf("provisions=%d", client.provisions)
	}
}

func TestEventEnqueueRejectsInvalidIdentityAndNonEventJob(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	for _, id := range []string{"", "Upper", "double--hyphen", "-leading", "trailing-", strings.Repeat("a", 129)} {
		if _, err := eng.Enqueue(context.Background(), "docs-drift", id, time.Now()); !errors.Is(err, ErrInvalidEventID) {
			t.Fatalf("id=%q error=%v", id, err)
		}
	}
	assertNoEventRunOrWorkspace(t, state, client)

	clockEngine, _, _ := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	if _, err := clockEngine.Enqueue(context.Background(), "docs-drift", "valid-id", time.Now()); !errors.Is(err, ErrNotEventJob) {
		t.Fatalf("error=%v", err)
	}
}

func TestEventManualRunRequiresCanary(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	if _, err := eng.RunNow(context.Background(), "docs-drift", false, time.Now()); !errors.Is(err, ErrEventCanaryRequired) {
		t.Fatalf("error=%v", err)
	}
	assertNoEventRunOrWorkspace(t, state, client)
}

func TestEventCanaryDispatchIsIsolatedFromQueuedEvent(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eng.ConfigPath, []byte(strings.Replace(string(raw), "overlap: forbid", "overlap: allow", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	now := mustTime("2026-08-23T10:00:00+08:00")
	queued, err := eng.Enqueue(context.Background(), "docs-drift", "queued-before-canary", now)
	if err != nil {
		t.Fatal(err)
	}
	canary, err := eng.RunNow(context.Background(), "docs-drift", true, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if canary.Trigger != "canary" {
		t.Fatalf("trigger=%q", canary.Trigger)
	}
	finished := waitForRunTerminal(t, state, canary.ID)
	if finished.State != store.StateSucceeded {
		t.Fatalf("canary=%+v", finished)
	}
	stillQueued, err := state.GetRun(context.Background(), queued.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if stillQueued.State != store.StateAccepted || client.provisions != 1 {
		t.Fatalf("queued=%+v provisions=%d", stillQueued, client.provisions)
	}
}

func TestHeldEventCanaryCannotLaunchAfterRestart(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-23T09:00:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := state.SetGlobalPaused(ctx, true); err != nil {
		t.Fatal(err)
	}
	canary, err := eng.RunNow(ctx, "docs-drift", true, mustTime("2026-08-23T10:00:00+08:00"))
	if err != nil {
		t.Fatal(err)
	}
	if canary.Trigger != "canary" || canary.State != store.StateBlocked || canary.ErrorCode != "global_paused" {
		t.Fatalf("canary=%+v", canary)
	}
	if err := state.SetGlobalPaused(ctx, false); err != nil {
		t.Fatal(err)
	}
	restarted := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, eng.ConfigPath)
	restarted.DiskCapacity = eng.DiskCapacity
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := state.GetRun(ctx, canary.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if after.State != store.StateBlocked || client.provisions != 0 {
		t.Fatalf("canary=%+v provisions=%d", after, client.provisions)
	}
}

func TestDaemonDispatchNeverClaimsAcceptedCanary(t *testing.T) {
	eng, state, client := newEventEngine(t, true)
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime("2026-08-23T10:00:00+08:00")
	if _, err := state.SyncJobAuthority(ctx, cfg.Jobs[0].ID, cfg.Jobs[0].Revision, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	canary, err := state.CreateCanaryRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := state.GetRun(ctx, canary.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if after.State != store.StateAccepted || client.provisions != 0 {
		t.Fatalf("canary=%+v provisions=%d", after, client.provisions)
	}
}

func TestRunIfChangedSkipsUnchangedRevisionAndSuppliesChangedPaths(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	initGitRepo(t, client.path)
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "    enabled: true\n", "    enabled: true\n    run_if_changed: true\n", 1)
	text = strings.Replace(text, "      workspace: worktree\n", "      workspace: worktree\n      base_ref: main\n", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	first := waitForTerminal(t, state, "docs-drift")
	if first.SourceRevision == "" || !strings.Contains(first.InputContext, "initial baseline") {
		t.Fatalf("first source context=%+v", first)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-23T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	runs, err := state.ListRuns(ctx, "docs-drift", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("unchanged source created %d runs", len(runs))
	}
	if err := os.WriteFile(filepath.Join(client.path, "changed.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, client.path, "add", "changed.txt")
	git(t, client.path, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "test change")
	wantHead := strings.TrimSpace(git(t, client.path, "rev-parse", "HEAD"))
	if err := eng.Evaluate(ctx, mustTime("2026-08-24T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	second := waitForRunCount(t, state, "docs-drift", 2)
	if second.State != store.StateSucceeded || second.SourceBaseRevision != first.SourceRevision || second.SourceRevision != wantHead || !strings.Contains(second.InputContext, "changed.txt") {
		t.Fatalf("second source context=%+v", second)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 2 || client.baseRef != wantHead || !strings.Contains(client.prompt, "untrusted repository data") || !strings.Contains(client.prompt, "changed.txt") {
		t.Fatalf("provisions=%d base=%q prompt=%q", client.provisions, client.baseRef, client.prompt)
	}
}

func TestRunIfChangedRunsAgainWhenJobDefinitionChanges(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	initGitRepo(t, client.path)
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "    enabled: true\n", "    enabled: true\n    run_if_changed: true\n", 1)
	text = strings.Replace(text, "      workspace: worktree\n", "      workspace: worktree\n      base_ref: main\n", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	first := waitForTerminal(t, state, "docs-drift")
	text = strings.Replace(text, "prompt: Check docs.", "prompt: Check docs and tests.", 1)
	text = strings.Replace(text, "    revision: 1\n", "    revision: 2\n", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-23T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	second := waitForRunCount(t, state, "docs-drift", 2)
	if second.SourceRevision != first.SourceRevision || second.SourceBaseRevision != "" || !strings.Contains(second.InputContext, "initial baseline") {
		t.Fatalf("definition change was not treated as a fresh baseline: %+v", second)
	}
}

func TestClaudeUsesHeadlessNativeCommandWithoutTrustApproval(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "harness: pi\n      provider: openai-codex\n      model: gpt-5.6-sol", "harness: claude-code\n      model: opus", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateSucceeded || run.TaskVerdict != "passed" || run.ExecutionMode != adapter.ModeCommand {
		t.Fatalf("run=%+v", run)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.kind != "" || !strings.Contains(client.command, "'claude' -p") || !strings.Contains(client.command, "--safe-mode") || !strings.Contains(client.command, "'--tools' 'Read,Glob,Grep'") {
		t.Fatalf("unexpected Claude launch: kind=%q command=%q", client.kind, client.command)
	}
}

func TestHeadlessTimeoutClosesItsWorkspace(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "harness: pi\n      provider: openai-codex\n      model: gpt-5.6-sol", "harness: claude-code\n      model: opus", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	client.commandErr = context.DeadlineExceeded
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	client.mu.Lock()
	defer client.mu.Unlock()
	if run.State != store.StateTimedOut || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", run, client.closeCount)
	}
}

func TestTimeoutDoesNotTerminalizeWhileWorkspaceClosureFails(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "harness: pi\n      provider: openai-codex\n      model: gpt-5.6-sol", "harness: claude-code\n      model: opus", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	client.commandErr = context.DeadlineExceeded
	client.closeErr = errors.New("close unavailable")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	runs, err := state.ListRuns(ctx, "docs-drift", 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if runs[0].State != store.StateRunning || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", runs[0], client.closeCount)
	}
}

func TestHeadlessWaitFailureClosesBeforeInterrupting(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "harness: pi\n      provider: openai-codex\n      model: gpt-5.6-sol", "harness: claude-code\n      model: opus", 1)
	if err := os.WriteFile(eng.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	client.commandErr = errors.New("pane read failed")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	client.mu.Lock()
	defer client.mu.Unlock()
	if run.State != store.StateInterrupted || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", run, client.closeCount)
	}
}

func TestRouteMismatchFailsBeforeProvisioning(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex other-model\n")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateFailed || run.ErrorCode != "route_unavailable" || run.InfrastructureResult != "failed" {
		t.Fatalf("run=%+v", run)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 {
		t.Fatalf("provisioned %d times", client.provisions)
	}
}

func TestBlockedAgentIsNotReportedAsSuccess(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	client.status = "blocked"
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateBlocked || run.AgentResult != "blocked" || run.TaskVerdict != "unverified" {
		t.Fatalf("run=%+v", run)
	}
}

func TestVerifierCanFailASettledAgentRun(t *testing.T) {
	eng, state, _ := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "/usr/bin/true", "/usr/bin/false", 1))
	if err := os.WriteFile(eng.ConfigPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateFailed || run.TaskVerdict != "failed" || run.ErrorCode != "verifier_failed" {
		t.Fatalf("run=%+v", run)
	}
}

func TestPausedJobHoldsOccurrenceWithoutStarting(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPaused(ctx, "docs-drift", true); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	runs, err := state.ListRuns(ctx, "docs-drift", 10)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(runs) != 0 || client.provisions != 0 {
		t.Fatalf("paused job produced runs=%d provisions=%d", len(runs), client.provisions)
	}
}

func TestReconcileAcceptedRunStartsItOnce(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now); err != nil {
		t.Fatal(err)
	}
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	finished := waitForTerminal(t, state, "docs-drift")
	if finished.State != store.StateSucceeded {
		t.Fatalf("run=%+v", finished)
	}
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 1 {
		t.Fatalf("accepted restart provisioned %d times", client.provisions)
	}
}

func TestReconcileRunningReceiptDoesNotProvisionAgain(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateAccepted, store.StateProvisioning, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := state.SetReceipt(ctx, run.ID, "w-existing", "p-existing", "auto/test", client.path, adapter.ModeAgent, ""); err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateProvisioning, store.StateStarting, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateStarting, store.StateRunning, "test", now); err != nil {
		t.Fatal(err)
	}
	client.status = "idle"
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	finished := waitForTerminal(t, state, "docs-drift")
	if finished.State != store.StateSucceeded || finished.TaskVerdict != "passed" {
		t.Fatalf("run=%+v", finished)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 {
		t.Fatalf("reconciliation reprovisioned %d times", client.provisions)
	}
}

func TestUncertainRestartClosesWorkspaceBeforeTerminalizing(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := cfg.Jobs[0].Snapshot()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateAccepted, store.StateProvisioning, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := state.SetReceipt(ctx, run.ID, "w-uncertain", "p-uncertain", "auto/test", client.path, adapter.ModeAgent, ""); err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateProvisioning, store.StateStarting, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateStarting, store.StateRunning, "test", now); err != nil {
		t.Fatal(err)
	}
	client.status = "unknown"
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := state.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestCancelAcceptedRunCreatesNoWorkspace(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := cfg.Jobs[0].Snapshot()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Cancel(ctx, run.ID, now); err != nil {
		t.Fatal(err)
	}
	got, err := state.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateCancelled || client.provisions != 0 {
		t.Fatalf("run=%+v provisions=%d", got, client.provisions)
	}
}

func TestDisabledTimeIsNotCaughtUpAfterReenable(t *testing.T) {
	eng, state, _ := newTestEngine(t, false, "provider model\nopenai-codex gpt-5.6-sol\n")
	now := mustTime("2026-08-22T09:00:30+08:00")
	if err := eng.Evaluate(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	job, err := state.Job(context.Background(), "docs-drift")
	if err != nil {
		t.Fatal(err)
	}
	if !job.Cursor.Equal(now) {
		t.Fatalf("disabled cursor=%s want=%s", job.Cursor, now)
	}
}

func initGitRepo(t *testing.T, repo string) {
	t.Helper()
	git(t, repo, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", "README.md")
	git(t, repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "initial")
}

func git(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}

func waitForRunCount(t *testing.T, state *store.Store, job string, count int) store.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := state.ListRuns(context.Background(), job, count)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == count && isTerminal(runs[0].State) {
			return runs[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d terminal runs", count)
	return store.Run{}
}

func TestDispatchReservesPeakDiskForActiveAndCandidateRuns(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "overlap: forbid", "overlap: allow", 1))
	if err := os.WriteFile(eng.ConfigPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now); err != nil {
			t.Fatal(err)
		}
	}
	blocked := make(chan struct{})
	client.waitCh = blocked
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 5.6, Device: 1}, nil }
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	waitForProvisions(t, client, 2)
	runs, err := state.ListRuns(ctx, "docs-drift", 10)
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for _, run := range runs {
		if run.State == store.StateAccepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d runs=%+v", accepted, runs)
	}
}

func TestDispatchFailsClosedBelowCandidateDiskReserve(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := cfg.Jobs[0].Snapshot()
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now); err != nil {
		t.Fatal(err)
	}
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 4.2, Device: 1}, nil }
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	if provisions != 0 {
		t.Fatalf("provisioned %d runs below reserved headroom", provisions)
	}
	runs, err := state.ListRuns(ctx, "docs-drift", 10)
	if err != nil || len(runs) != 1 || runs[0].State != store.StateAccepted {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestConcurrentDispatchCannotSpendCapacityTwice(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "overlap: forbid", "overlap: allow", 1))
	if err := os.WriteFile(eng.ConfigPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := cfg.Jobs[0].Snapshot()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now); err != nil {
			t.Fatal(err)
		}
	}
	blocked := make(chan struct{})
	client.waitCh = blocked
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- eng.Dispatch(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitForProvisions(t, client, 2)
}

func TestConcurrentDispatchAcrossStoreHandlesCannotOverspendReserve(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { firstStore.Close() })
	secondStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { secondStore.Close() })
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	if _, err := firstStore.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	first, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	created := make(chan struct{}, 1)
	client := &fakeHerdr{path: repo, provisionCreated: created, provisionGate: gate}
	models := "provider model\nopenai-codex gpt-5.6-sol\n"
	firstEngine := New(firstStore, client, fakeCommands{models: models}, configPath)
	secondEngine := New(secondStore, client, fakeCommands{models: models}, configPath)
	for _, eng := range []*Engine{firstEngine, secondEngine} {
		// 5.49 GiB covers the 3.0 GiB floor plus exactly one 1.25 GiB reserve;
		// admitting both runs would spend 5.50 GiB it does not have.
		eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 5.49, Device: 1}, nil }
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, eng := range []*Engine{firstEngine, secondEngine} {
		wg.Add(1)
		go func(eng *Engine) {
			defer wg.Done()
			errs <- eng.Dispatch(ctx)
		}(eng)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent dispatch returned a spurious error: %v", err)
		}
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("no workspace was provisioned")
	}
	claimed, err := secondStore.GetRun(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	held, err := secondStore.GetRun(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	owners := map[string]bool{firstEngine.owner: true, secondEngine.owner: true}
	if claimed.State != store.StateProvisioning || !owners[claimed.ProvisioningOwner] || !claimed.ProvisioningLeaseUntil.After(now) ||
		claimed.DiskDevice != "1" || claimed.DiskReserveGiB != 1.25 {
		t.Fatalf("claim was not durably persisted with device and reserve: %+v", claimed)
	}
	if held.State != store.StateAccepted || held.DiskDevice != "" || held.DiskReserveGiB != 0 || provisions != 1 {
		t.Fatalf("second run overspent reserved headroom: run=%+v provisions=%d", held, provisions)
	}
	close(gate)
	finished := waitForRunTerminal(t, secondStore, first.ID)
	if finished.State != store.StateSucceeded {
		t.Fatalf("claimed run=%+v", finished)
	}
}

func TestAdmissionNeverProbesActiveRepositories(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	candidateRepo := cfg.Jobs[0].Execution.Repository
	// A legacy active row persisted no device or reserve and points at a volume
	// that must never be probed during admission.
	legacy := []byte(`{"id":"docs-drift","execution":{"repository":` + strconv.Quote(filepath.Join(t.TempDir(), "elsewhere")) + `,"harness":"pi","model":"gpt-5.6-sol","permission_profile":"read-only-no-network"},"limits":{"max_runs_per_day":1,"disk_reserve_gib":2}}`)
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	legacyRun, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, legacy, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, legacyRun.ID, store.StateAccepted, store.StateProvisioning, "legacy active run", now); err != nil {
		t.Fatal(err)
	}
	candidate, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	free := 4.9
	eng.DiskCapacity = func(path string) (DiskCapacity, error) {
		if path != candidateRepo {
			return DiskCapacity{}, fmt.Errorf("active repository %s must not be probed", path)
		}
		return DiskCapacity{FreeGiB: free, Device: 1}, nil
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := state.GetRun(ctx, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	// 4.9 GiB covers floor 3.0 plus the candidate's 1.25 only if the legacy
	// reserve is ignored; charging the declared 2 GiB globally must hold it.
	if held.State != store.StateAccepted || provisions != 0 {
		t.Fatalf("candidate=%+v provisions=%d", held, provisions)
	}
	free = 6.25
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	finished := waitForRunTerminal(t, state, candidate.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if finished.State != store.StateSucceeded || client.provisions != 1 {
		t.Fatalf("candidate=%+v provisions=%d", finished, client.provisions)
	}
}

func TestAdmissionFailsClosedOnInvalidLegacySnapshot(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	legacyRun, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, []byte(`{"id":"docs-drift","execution":`), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, legacyRun.ID, store.StateAccepted, store.StateProvisioning, "corrupt legacy active run", now); err != nil {
		t.Fatal(err)
	}
	candidate, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err == nil || !strings.Contains(err.Error(), "invalid snapshot") {
		t.Fatalf("dispatch err=%v want invalid snapshot failure", err)
	}
	held, err := state.GetRun(ctx, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if held.State != store.StateAccepted || client.provisions != 0 {
		t.Fatalf("candidate=%+v provisions=%d", held, client.provisions)
	}
}

func TestRunNowHonoursCapacityAdmission(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 4.2, Device: 1}, nil }
	run, err := eng.RunNow(context.Background(), "docs-drift", false, mustTime("2026-08-22T09:00:00+08:00"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := state.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateAccepted || client.provisions != 0 || eng.InFlight(run.ID) {
		t.Fatalf("run=%+v provisions=%d in_flight=%t", got, client.provisions, eng.InFlight(run.ID))
	}
}

func TestDispatchRechecksAuthorityAcrossStoreHandlesBeforeProvisioning(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(context.Context, *store.Store, config.Job, time.Time) error
	}{
		{name: "global pause", mutate: func(ctx context.Context, state *store.Store, _ config.Job, _ time.Time) error {
			return state.SetGlobalPaused(ctx, true)
		}},
		{name: "job pause", mutate: func(ctx context.Context, state *store.Store, job config.Job, _ time.Time) error {
			return state.SetPaused(ctx, job.ID, true)
		}},
		{name: "job disable", mutate: func(ctx context.Context, state *store.Store, job config.Job, now time.Time) error {
			job.Revision++
			disabled := false
			job.Enabled = &disabled
			snapshot, revision, err := job.Snapshot()
			if err != nil {
				return err
			}
			_, err = state.SyncJobAuthority(ctx, job.ID, job.Revision, revision, snapshot, false, now)
			return err
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "authority.db")
			firstStore, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer firstStore.Close()
			secondStore, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer secondStore.Close()
			repo := t.TempDir()
			configPath := writeJobs(t, repo, true)
			cfg, err := config.Load(configPath)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, revision, err := cfg.Jobs[0].Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			ctx := context.Background()
			if _, err := firstStore.SyncJobAuthority(ctx, cfg.Jobs[0].ID, cfg.Jobs[0].Revision, revision, snapshot, true, now); err != nil {
				t.Fatal(err)
			}
			run, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
			if err != nil {
				t.Fatal(err)
			}
			client := &fakeHerdr{path: repo}
			eng := New(firstStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
			probeStarted := make(chan struct{})
			probeContinue := make(chan struct{})
			eng.DiskCapacity = func(string) (DiskCapacity, error) {
				close(probeStarted)
				<-probeContinue
				return DiskCapacity{FreeGiB: 100, Device: 1}, nil
			}
			dispatchErr := make(chan error, 1)
			go func() { dispatchErr <- eng.Dispatch(ctx) }()
			select {
			case <-probeStarted:
			case <-time.After(3 * time.Second):
				t.Fatal("dispatch did not reach the disk probe")
			}
			if err := testCase.mutate(ctx, secondStore, cfg.Jobs[0], now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			close(probeContinue)
			if err := <-dispatchErr; err != nil {
				t.Fatal(err)
			}
			after, err := secondStore.GetRun(ctx, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			defer client.mu.Unlock()
			if after.State != store.StateAccepted || after.JobRevision != revision || string(after.Definition) != string(snapshot) || client.provisions != 0 {
				t.Fatalf("run=%+v provisions=%d", after, client.provisions)
			}
		})
	}
}

func TestRunNowRefusesDisabledJobWithoutCreatingAcceptedRun(t *testing.T) {
	eng, state, client := newTestEngine(t, false, "provider model\nopenai-codex gpt-5.6-sol\n")
	_, err := eng.RunNow(context.Background(), "docs-drift", false, mustTime("2026-08-22T09:00:00+08:00"))
	if !errors.Is(err, store.ErrJobDisabled) {
		t.Fatalf("error=%v", err)
	}
	runs, listErr := state.ListRuns(context.Background(), "docs-drift", 10)
	if listErr != nil || len(runs) != 0 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 {
		t.Fatalf("disabled manual run provisioned %d workspaces", client.provisions)
	}
}

func TestDispatchFailsClosedWhenDiskProbeFails(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := cfg.Jobs[0].Snapshot()
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now); err != nil {
		t.Fatal(err)
	}
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{}, errors.New("statfs unavailable") }
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	runs, err := state.ListRuns(ctx, "docs-drift", 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 0 || runs[0].State != store.StateFailed || runs[0].ErrorCode != "capacity_probe_failed" {
		t.Fatalf("provisions=%d run=%+v", client.provisions, runs[0])
	}
}

func TestSecondDispatchAtMaxOneHoldsWhileRouteProbeBlocks(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	raw, err := os.ReadFile(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "version: 1", "version: 1\ncapacity:\n  max_concurrent_runs: 1\n  min_free_disk_gib: 3", 1))
	if err := os.WriteFile(eng.ConfigPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	first, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	// Distinct accepted_at timestamps keep the FIFO dispatch order deterministic.
	second, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	eng.Runner = fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n", probeGate: gate}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	// The first run is durably claimed as provisioning while its route probe
	// blocks, so the single concurrency slot is already spent.
	if got := waitForRunState(t, state, first.ID, store.StateProvisioning); got.ID != first.ID {
		t.Fatalf("first run=%+v", got)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	if provisions != 0 {
		t.Fatalf("provisioned %d runs while the route probe blocks", provisions)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := state.GetRun(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.State != store.StateAccepted {
		t.Fatalf("second dispatch overspent max_concurrent_runs=1: %+v", held)
	}
	close(gate)
	finished := waitForRunTerminal(t, state, first.ID)
	if finished.State != store.StateSucceeded {
		t.Fatalf("first run=%+v", finished)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	secondFinished := waitForRunTerminal(t, state, second.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if secondFinished.State != store.StateSucceeded || client.provisions != 2 {
		t.Fatalf("second run=%+v provisions=%d", secondFinished, client.provisions)
	}
}

func TestLegacySnapshotWithoutReserveChargesDefault(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// A snapshot persisted before disk_reserve_gib existed carries no field.
	legacy := []byte(`{"id":"docs-drift","execution":{"repository":` + strconv.Quote(cfg.Jobs[0].Execution.Repository) + `,"harness":"pi","model":"gpt-5.6-sol","permission_profile":"read-only-no-network"},"limits":{"max_runs_per_day":1}}`)
	var legacyJob config.Job
	if err := json.Unmarshal(legacy, &legacyJob); err != nil {
		t.Fatal(err)
	}
	if legacyJob.DiskReserve() != config.DefaultDiskReserveGiB {
		t.Fatalf("legacy reserve=%v want=%v", legacyJob.DiskReserve(), config.DefaultDiskReserveGiB)
	}
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	legacyRun, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, legacy, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, legacyRun.ID, store.StateAccepted, store.StateProvisioning, "legacy active run", now); err != nil {
		t.Fatal(err)
	}
	candidate, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	// 4.5 GiB free covers floor 3.0 plus the candidate's 1.25 (4.25) only if
	// the legacy run is charged nothing; charging the 1.25 default requires
	// 5.5 GiB and must hold the candidate.
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 4.5, Device: 1}, nil }
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := state.GetRun(ctx, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if held.State != store.StateAccepted || client.provisions != 0 {
		t.Fatalf("candidate=%+v provisions=%d", held, client.provisions)
	}
}

func TestAdmissionBoundaryIsExactAtRequiredHeadroom(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	active, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, active.ID, store.StateAccepted, store.StateProvisioning, "boundary anchor", now); err != nil {
		t.Fatal(err)
	}
	candidate, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	// Required headroom is exactly 5.50 GiB: 3.0 floor + 1.25 active reserve + 1.25 candidate reserve.
	free := 5.49
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: free, Device: 1}, nil }
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := state.GetRun(ctx, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	if held.State != store.StateAccepted || provisions != 0 {
		t.Fatalf("5.49 GiB must hold the candidate: run=%+v provisions=%d", held, provisions)
	}
	free = 5.50
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	finished := waitForRunTerminal(t, state, candidate.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if finished.State != store.StateSucceeded || client.provisions != 1 {
		t.Fatalf("5.50 GiB must admit the candidate: run=%+v provisions=%d", finished, client.provisions)
	}
}

func TestTerminalRunReleasesItsDiskReserve(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	client.waitCh = blocked
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 4.5, Device: 1}, nil }
	first, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	waitForRunState(t, state, first.ID, store.StateRunning)
	second, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := state.GetRun(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.State != store.StateAccepted {
		t.Fatalf("candidate must be held while the active reserve is charged: %+v", held)
	}
	// A terminal run no longer holds its reserve, freeing headroom for the next candidate.
	close(blocked)
	finished := waitForRunTerminal(t, state, first.ID)
	if finished.State != store.StateSucceeded {
		t.Fatalf("first run=%+v", finished)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	secondFinished := waitForRunTerminal(t, state, second.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if secondFinished.State != store.StateSucceeded || client.provisions != 2 {
		t.Fatalf("second run=%+v provisions=%d", secondFinished, client.provisions)
	}
}

func TestReservesAreGroupedByFilesystemDevice(t *testing.T) {
	repoA := t.TempDir()
	repoB := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repoA}
	configPath := filepath.Join(t.TempDir(), "automations.yaml")
	body := `version: 1
capacity:
  max_concurrent_runs: 8
  min_free_disk_gib: 0.5
jobs:
  - id: alpha-build
    schedule:
      kind: cron
      expression: "0 9 * * *"
      timezone: Asia/Hong_Kong
    execution:
      repository: ` + repoA + `
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      permission_profile: read-only-no-network
    prompt: Build alpha.
    limits:
      max_runs_per_day: 5
      disk_reserve_gib: 64
  - id: beta-check
    schedule:
      kind: cron
      expression: "0 9 * * *"
      timezone: Asia/Hong_Kong
    execution:
      repository: ` + repoB + `
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      permission_profile: read-only-no-network
    prompt: Check beta.
    limits:
      max_runs_per_day: 5
      disk_reserve_gib: 2
`
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	eng.DiskCapacity = func(path string) (DiskCapacity, error) {
		switch path {
		case repoA:
			return DiskCapacity{FreeGiB: 100, Device: 1}, nil
		case repoB:
			return DiskCapacity{FreeGiB: 3, Device: 2}, nil
		}
		return DiskCapacity{}, errors.New("unexpected repository " + path)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapA, revA, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapB, revB, err := cfg.Jobs[1].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime("2026-08-22T09:00:00+08:00")
	if _, err := state.SyncJob(ctx, "alpha-build", revA, snapA, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := state.SyncJob(ctx, "beta-check", revB, snapB, true, now); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	client.waitCh = blocked
	onA, err := state.CreateManualRun(ctx, "alpha-build", revA, snapA, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	waitForRunState(t, state, onA.ID, store.StateRunning)
	queuedOnA, err := state.CreateManualRun(ctx, "alpha-build", revA, snapA, now)
	if err != nil {
		t.Fatal(err)
	}
	onB, err := state.CreateManualRun(ctx, "beta-check", revB, snapB, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	// The crowded volume holds its own queue...
	held, err := state.GetRun(ctx, queuedOnA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.State != store.StateAccepted {
		t.Fatalf("run on crowded volume must be held: %+v", held)
	}
	// ...while the second filesystem admits from its own headroom. A global
	// charge of alpha's 64 GiB reserve would require 66.5 GiB on device two.
	waitForRunState(t, state, onB.ID, store.StateRunning)
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.provisions != 2 {
		t.Fatalf("provisions=%d", client.provisions)
	}
}

func TestCrossProcessReconcilePreservesReceiptToPromptWindow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ctx := context.Background()
	if _, err := firstStore.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	submitStarted := make(chan struct{}, 1)
	submitGate := make(chan struct{})
	client := &fakeHerdr{path: repo, submitStarted: submitStarted, submitGate: submitGate}
	firstEngine := New(firstStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	secondEngine := New(secondStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	for _, eng := range []*Engine{firstEngine, secondEngine} {
		eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	}
	if err := firstEngine.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-submitStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("prompt submission did not start")
	}
	starting, err := secondStore.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if starting.State != store.StateStarting || starting.ProvisioningOwner != firstEngine.owner || !starting.ProvisioningLeaseUntil.After(time.Now()) {
		t.Fatalf("receipt-bearing start did not retain its live owner: %+v", starting)
	}
	// Herdr reports an idle pane, but the second engine must not interpret that
	// as task completion while prompt acceptance is still pending.
	if err := secondEngine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	stillStarting, err := secondStore.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	statusCalls, provisions, starts, submits := client.statusCount, client.provisions, client.startAgentCount, client.submitCount
	client.mu.Unlock()
	if stillStarting.State != store.StateStarting || stillStarting.ProvisioningOwner != firstEngine.owner || statusCalls != 0 || provisions != 1 || starts != 1 || submits != 1 {
		t.Fatalf("run=%+v status=%d provisions=%d starts=%d submits=%d", stillStarting, statusCalls, provisions, starts, submits)
	}
	close(submitGate)
	finished := waitForRunTerminal(t, secondStore, run.ID)
	if finished.State != store.StateSucceeded || finished.ProvisioningOwner != "" || !finished.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("run=%+v", finished)
	}
}

func TestCrossProcessExpiredStartingClaimInterruptsWithoutIdleInference(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	ctx := context.Background()
	if _, err := firstStore.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeHerdr{path: repo, status: "idle"}
	firstEngine := New(firstStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	secondEngine := New(secondStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	admitted, err := firstStore.DecideAdmission(ctx, run.ID, firstEngine.owner, "1", 1.25, base, base.Add(time.Minute), func([]store.Run) (store.AdmissionDecision, error) {
		return store.AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	saved, err := firstStore.SaveProvisioningReceipt(ctx, run.ID, firstEngine.owner, "w-starting", "p-starting", "auto/test", repo, adapter.ModeAgent, "", base.Add(30*time.Second))
	if err != nil || !saved {
		t.Fatalf("saved=%t err=%v", saved, err)
	}
	secondEngine.Now = func() time.Time { return base.Add(2 * time.Minute) }
	secondEngine.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	if err := secondEngine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := secondStore.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_agent_start" || got.AgentResult != "not_started" || client.statusCount != 0 || client.startAgentCount != 0 || client.submitCount != 0 || client.closeCount != 1 {
		t.Fatalf("run=%+v status=%d starts=%d submits=%d closes=%d", got, client.statusCount, client.startAgentCount, client.submitCount, client.closeCount)
	}
}

func TestCrossProcessReconcilePreservesLiveProvisioningClaim(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()

	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "version: 1", "version: 1\ncapacity:\n  max_concurrent_runs: 1\n  min_free_disk_gib: 3", 1))
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := firstStore.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	first, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := firstStore.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	client := &fakeHerdr{path: repo}
	probeGate := make(chan struct{})
	probeStarted := make(chan struct{}, 1)
	firstEngine := New(firstStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n", probeGate: probeGate, probeStarted: probeStarted}, configPath)
	secondEngine := New(secondStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	for _, eng := range []*Engine{firstEngine, secondEngine} {
		eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	}
	if err := firstEngine.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("route probe did not start")
	}
	if err := secondEngine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := secondStore.GetRun(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	held, err := secondStore.GetRun(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	if live.State != store.StateProvisioning || live.ProvisioningOwner == "" || !live.ProvisioningLeaseUntil.After(time.Now()) {
		t.Fatalf("live claim was not preserved: %+v", live)
	}
	if held.State != store.StateAccepted || provisions != 0 {
		t.Fatalf("held=%+v provisions=%d", held, provisions)
	}

	close(probeGate)
	finished := waitForRunTerminal(t, secondStore, first.ID)
	if finished.State != store.StateSucceeded {
		t.Fatalf("first run=%+v", finished)
	}
	held, err = secondStore.GetRun(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if held.State != store.StateAccepted || client.provisions != 1 {
		t.Fatalf("held=%+v provisions=%d", held, client.provisions)
	}
}

func TestProvisionLookupFailurePersistsPartialReceiptForCleanup(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	base := time.Now()
	eng.Now = func() time.Time { return base }
	client.provisionReceipt = herdr.Receipt{WorkspaceID: "w-partial", PaneID: "p-partial"}
	client.provisionErr = errors.New("created worktree path lookup failed")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var run store.Run
	for time.Now().Before(deadline) {
		runs, err := state.ListRuns(ctx, "docs-drift", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == 1 && !eng.InFlight(runs[0].ID) {
			run = runs[0]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if run.State != store.StateProvisioning || run.WorkspaceID != "w-partial" || run.Branch == "" {
		t.Fatalf("partial receipt was not durable: %+v", run)
	}
	eng.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, state, run.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestExpiredProvisioningRecoversAndClosesUnsavedWorkspace(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := state.DecideAdmission(ctx, run.ID, eng.owner, "1", 1.25, base, base.Add(time.Minute), func([]store.Run) (store.AdmissionDecision, error) {
		return store.AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	branch := "auto/docs-drift/planned"
	planned, err := state.SaveProvisioningPlan(ctx, run.ID, eng.owner, branch, base.Add(10*time.Second))
	if err != nil || !planned {
		t.Fatalf("planned=%t err=%v", planned, err)
	}
	client.foundWorkspace = true
	client.foundReceipt = herdr.Receipt{WorkspaceID: "w-orphan", Branch: branch, Path: client.path}
	eng.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, state, run.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_provisioning" || got.WorkspaceID != "w-orphan" || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestExpiredProvisioningClaimIsInterruptedAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	if _, err := firstStore.SyncJob(context.Background(), cfg.Jobs[0].ID, revision, snapshot, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := firstStore.CreateManualRun(context.Background(), cfg.Jobs[0].ID, revision, snapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := firstStore.DecideAdmission(context.Background(), run.ID, "stopped-process", "1", 1.25, base, base.Add(time.Minute), func([]store.Run) (store.AdmissionDecision, error) {
		return store.AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	restartedStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedStore.Close()
	client := &fakeHerdr{path: repo}
	restarted := New(restartedStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	restarted.Now = func() time.Time { return base.Add(2 * time.Minute) }
	restarted.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := restartedStore.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_provisioning" || client.provisions != 0 {
		t.Fatalf("run=%+v provisions=%d", got, client.provisions)
	}
}

func TestProvisioningReceiptStoreFailureDoesNotCloseWithoutDurableClaim(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	created := make(chan struct{}, 1)
	gate := make(chan struct{})
	client.provisionCreated = created
	client.provisionGate = gate
	run, err := eng.RunNow(context.Background(), "docs-drift", false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace was not created")
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	close(gate)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && eng.InFlight(run.ID) {
		time.Sleep(10 * time.Millisecond)
	}
	surfaced := eng.Dispatch(context.Background())
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closeCount != 0 || surfaced == nil {
		t.Fatalf("unclaimed cleanup was executed or persistence failure was hidden: closes=%d surfaced=%v", client.closeCount, surfaced)
	}
}

func TestExpiredProvisioningClaimRecoversAndClosesKnownWorkspace(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	base := time.Now()
	var clockMu sync.Mutex
	clock := base
	eng.Now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	created := make(chan struct{}, 1)
	gate := make(chan struct{})
	client.provisionCreated = created
	client.provisionGate = gate
	run, err := eng.RunNow(context.Background(), "docs-drift", false, base)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace was not created")
	}
	clockMu.Lock()
	clock = base.Add(2 * time.Minute)
	clockMu.Unlock()
	close(gate)
	got := waitForRunTerminal(t, state, run.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.ErrorCode != "receipt_not_persisted" || got.WorkspaceID != "w1" || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestExpiredClaimRecoversPartialProvisioningReceipt(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	base := time.Now()
	var clockMu sync.Mutex
	clock := base
	eng.Now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	created := make(chan struct{}, 1)
	gate := make(chan struct{})
	client.provisionCreated = created
	client.provisionGate = gate
	client.provisionReceipt = herdr.Receipt{WorkspaceID: "w-partial", Path: client.path}
	client.provisionErr = errors.New("provision response incomplete")
	run, err := eng.RunNow(context.Background(), "docs-drift", false, base)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace was not created")
	}
	clockMu.Lock()
	clock = base.Add(2 * time.Minute)
	clockMu.Unlock()
	close(gate)
	got := waitForRunTerminal(t, state, run.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.WorkspaceID != "w-partial" || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestDelayedProvisioningResultCleansWorkspaceAfterRunWasTerminalized(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	base := time.Now()
	var clockMu sync.Mutex
	clock := base
	eng.Now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	created := make(chan struct{}, 1)
	gate := make(chan struct{})
	client.provisionCreated = created
	client.provisionGate = gate
	client.foundWorkspace = false
	run, err := eng.RunNow(context.Background(), "docs-drift", false, base)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace was not created")
	}
	clockMu.Lock()
	clock = base.Add(2 * time.Minute)
	clockMu.Unlock()
	if err := eng.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, err := state.GetRun(context.Background(), run.ID)
	if err != nil || terminal.State != store.StateInterrupted || terminal.WorkspaceID != "" {
		t.Fatalf("run was not terminalized before delayed receipt: run=%+v err=%v", terminal, err)
	}
	close(gate)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && eng.InFlight(run.ID) {
		time.Sleep(10 * time.Millisecond)
	}
	got, err := state.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_provisioning" || got.WorkspaceID != "w1" || got.EffectKind != "" || client.closeCount != 1 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestLostProvisioningClaimDoesNotDuplicateWorkspaceCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	var clockMu sync.Mutex
	clock := base
	now := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	if _, err := firstStore.SyncJob(context.Background(), cfg.Jobs[0].ID, revision, snapshot, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := firstStore.CreateManualRun(context.Background(), cfg.Jobs[0].ID, revision, snapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	created := make(chan struct{}, 1)
	gate := make(chan struct{})
	client := &fakeHerdr{path: repo, provisionCreated: created, provisionGate: gate, closeErr: errors.New("cleanup unavailable"), foundWorkspace: true, foundReceipt: herdr.Receipt{WorkspaceID: "w1", Path: repo}}
	firstEngine := New(firstStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	secondEngine := New(secondStore, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	firstEngine.Now, secondEngine.Now = now, now
	for _, eng := range []*Engine{firstEngine, secondEngine} {
		eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	}
	if err := firstEngine.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-created:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace was not created")
	}
	clockMu.Lock()
	clock = base.Add(2 * time.Minute)
	clockMu.Unlock()
	if err := secondEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(gate)
	deadline := time.Now().Add(3 * time.Second)
	var evidence store.Event
	for time.Now().Before(deadline) {
		events, eventErr := secondStore.Events(context.Background(), run.ID)
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		for _, event := range events {
			if event.Code == "workspace_close_failed" {
				evidence = event
				break
			}
		}
		if evidence.Code != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := secondStore.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	surfaced := firstEngine.Dispatch(context.Background())
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateProvisioning || got.WorkspaceID != "w1" || got.EffectKind != store.EffectWorkspaceClose || client.provisions != 1 || client.closeCount != 1 || evidence.Code != "workspace_close_failed" || !strings.Contains(evidence.Detail, `workspace_id="w1"`) || !strings.Contains(evidence.Detail, "cleanup unavailable") || surfaced != nil {
		t.Fatalf("run=%+v provisions=%d closes=%d evidence=%+v surfaced=%v", got, client.provisions, client.closeCount, evidence, surfaced)
	}
}

func waitForProvisions(t *testing.T, client *fakeHerdr, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		got := client.provisions
		client.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	t.Fatalf("provisions=%d want=%d", client.provisions, want)
}

func waitForRunState(t *testing.T, state *store.Store, runID, want string) store.Run {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := state.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.State == want {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach state %s", runID, want)
	return store.Run{}
}

func TestExpiredWorkspaceCloseChecksGoneStatusBeforeFinishing(t *testing.T) {
	first, second, firstEngine, _, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	base := time.Now()
	claim, err := first.ClaimEffect(context.Background(), run.ID, store.StateRunning, store.EffectWorkspaceClose, "stopped-closer", base, base.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("claim=%q err=%v", claim, err)
	}
	client.status = "gone"
	client.workspaceExistSet = true
	client.workspaceExists = false
	firstEngine.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := firstEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, second, run.ID)
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_workspace_close" {
		t.Fatalf("run=%+v", got)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closeCount != 0 || client.statusCount != 1 {
		t.Fatalf("recovery statuses=%d closes=%d", client.statusCount, client.closeCount)
	}
}

func TestExpiredWorkspaceCloseAmbiguityNeverClosesAgain(t *testing.T) {
	first, second, firstEngine, _, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	base := time.Now()
	claim, err := first.ClaimEffect(context.Background(), run.ID, store.StateRunning, store.EffectWorkspaceClose, "stopped-closer", base, base.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("claim=%q err=%v", claim, err)
	}
	if err := first.MarkRead(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	client.status = "idle"
	firstEngine.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := firstEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := second.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := second.Events(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		found = found || event.Code == "workspace_close_unverified"
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateRunning || !got.Unread || !found || client.statusCount != 1 || client.closeCount != 0 {
		t.Fatalf("run=%+v found=%t statuses=%d closes=%d", got, found, client.statusCount, client.closeCount)
	}
}

func TestFailedWorkspaceCloseReopensInboxWithDurableEvidence(t *testing.T) {
	first, _, firstEngine, _, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	if err := first.MarkRead(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	client.closeErr = errors.New("close unavailable")
	if err := firstEngine.Cancel(context.Background(), run.ID, time.Now()); err == nil {
		t.Fatal("failed workspace close reported success")
	}
	got, err := first.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := first.Events(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		found = found || event.Code == "workspace_close_failed"
	}
	if got.State != store.StateRunning || got.EffectKind != store.EffectWorkspaceClose || got.EffectOwner == "" || got.EffectClaim == "" || !got.Unread || !found {
		t.Fatalf("run=%+v events=%+v", got, events)
	}
}

func TestCrossProcessVerifierHasOneDurableOwner(t *testing.T) {
	root := t.TempDir()
	countPath := filepath.Join(root, "verifier-count")
	gatePath := filepath.Join(root, "verifier-release")
	scriptPath := filepath.Join(root, "verify.sh")
	script := "#!/bin/sh\nprintf 'x\\n' >> " + strconv.Quote(countPath) + "\nwhile [ ! -e " + strconv.Quote(gatePath) + " ]; do sleep 0.01; done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	first, second, firstEngine, secondEngine, _, run := newCrossProcessEffectRun(t, scriptPath)
	ctx := context.Background()
	if err := first.Transition(ctx, run.ID, store.StateRunning, store.StateSettled, "agent settled", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := firstEngine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	waitForFileLines(t, countPath, 1)
	if err := secondEngine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	count := fileLines(countPath)
	if err := os.WriteFile(gatePath, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("verifier executed %d times across two scheduler processes; want one durable owner", count)
	}
	got := waitForRunTerminal(t, second, run.ID)
	if got.State != store.StateSucceeded {
		t.Fatalf("run=%+v", got)
	}
}

func TestExpiredVerifierIsNotRerunWithoutCompletionEvidence(t *testing.T) {
	root := t.TempDir()
	countPath := filepath.Join(root, "verifier-count")
	gatePath := filepath.Join(root, "verifier-release")
	scriptPath := filepath.Join(root, "verify.sh")
	script := "#!/bin/sh\nprintf 'x\\n' >> " + strconv.Quote(countPath) + "\nwhile [ ! -e " + strconv.Quote(gatePath) + " ]; do sleep 0.01; done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	first, second, firstEngine, secondEngine, _, run := newCrossProcessEffectRun(t, scriptPath)
	base := time.Now()
	firstEngine.Now = func() time.Time { return base }
	secondEngine.Now = func() time.Time { return base.Add(verifierLease + time.Minute) }
	if err := first.Transition(context.Background(), run.ID, store.StateRunning, store.StateSettled, "agent settled", base); err != nil {
		t.Fatal(err)
	}
	if err := firstEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForFileLines(t, countPath, 1)
	if err := secondEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, second, run.ID)
	if err := os.WriteFile(gatePath, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got.State != store.StateInterrupted || got.ErrorCode != "restart_during_verifier" || got.TaskVerdict != "unverified" || fileLines(countPath) != 1 {
		t.Fatalf("run=%+v verifier executions=%d", got, fileLines(countPath))
	}
}

func TestStaleVerifierContenderCannotDeleteWinningReceipt(t *testing.T) {
	first, _, firstEngine, secondEngine, _, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	base := time.Now()
	if err := first.Transition(context.Background(), run.ID, store.StateRunning, store.StateSettled, "agent settled", base); err != nil {
		t.Fatal(err)
	}
	stale := run
	stale.State = store.StateSettled
	claim, receipt, err := first.ClaimVerifier(context.Background(), run.ID, "winner", base, base.Add(time.Minute))
	if err != nil || claim == "" || receipt == "" {
		t.Fatalf("claim=%q receipt=%q err=%v", claim, receipt, err)
	}
	if err := firstEngine.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	if code, _, err := firstEngine.runVerifierCommand(context.Background(), run.WorktreePath, receipt, claim, []string{"/usr/bin/true"}); err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	markerBefore, err := os.ReadFile(receipt)
	if err != nil {
		t.Fatal(err)
	}
	outputBefore, err := os.ReadFile(receipt + ".output")
	if err != nil {
		t.Fatal(err)
	}
	secondEngine.monitor(context.Background(), stale, "idle")
	markerAfter, err := os.ReadFile(receipt)
	if err != nil {
		t.Fatalf("stale contender removed winning marker: %v", err)
	}
	code, _, err := firstEngine.readVerifierReceiptFiles(receipt, claim)
	if err != nil || code != 0 || string(markerAfter) != string(markerBefore) {
		t.Fatalf("code=%d marker changed=%t err=%v", code, string(markerAfter) != string(markerBefore), err)
	}
	outputAfter, err := os.ReadFile(receipt + ".output")
	if err != nil || string(outputAfter) != string(outputBefore) {
		t.Fatalf("stale contender changed winning output: changed=%t err=%v", string(outputAfter) != string(outputBefore), err)
	}
}

func TestVerifierReceiptRejectsSymlinkAndPathEscape(t *testing.T) {
	eng, state, _ := newTestEngine(t, true, "")
	claim := "0123456789abcdef0123456789abcdef"
	receipt, err := state.VerifierReceiptPath(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	payload, err := json.Marshal(verifierReceiptRecord{Version: verifierReceiptVersion, Claim: claim, ExitCode: 0, Output: "outside"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, receipt); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.readVerifierReceiptFiles(receipt, claim); err == nil {
		t.Fatal("verifier accepted a symlink marker")
	}
	if _, _, err := eng.readVerifierReceiptFiles(filepath.Join(t.TempDir(), "escaped.result"), claim); err == nil {
		t.Fatal("verifier accepted a marker outside the canonical state directory")
	}
	raw, readErr := os.ReadFile(target)
	if readErr != nil || string(raw) != string(payload) {
		t.Fatalf("symlink target changed: %q err=%v", raw, readErr)
	}

	secondClaim := "11111111111111111111111111111111"
	secondReceipt, err := state.VerifierReceiptPath(secondClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.prepareVerifierReceipt(secondClaim, secondReceipt); err != nil {
		t.Fatal(err)
	}
	outputTarget := filepath.Join(t.TempDir(), "output-target")
	if err := os.WriteFile(outputTarget, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outputTarget, secondReceipt+".output"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.runVerifierCommand(context.Background(), t.TempDir(), secondReceipt, secondClaim, []string{"/usr/bin/true"}); err == nil {
		t.Fatal("verifier followed a pre-existing output symlink")
	}
	outputRaw, err := os.ReadFile(outputTarget)
	if err != nil || string(outputRaw) != "preserve" {
		t.Fatalf("output symlink target changed: %q err=%v", outputRaw, err)
	}
}

func TestVerifierSupervisorKillsBackgroundDescendants(t *testing.T) {
	eng, state, _ := newTestEngine(t, true, "")
	claim := "33333333333333333333333333333333"
	receipt, err := state.VerifierReceiptPath(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "escaped")
	command := []string{"/bin/sh", "-c", "(sleep 1; printf escaped > " + shellQuote(marker) + ") & exit 0"}
	code, _, err := eng.runVerifierCommand(context.Background(), t.TempDir(), receipt, claim, command)
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("background verifier descendant escaped supervision: %v", err)
	}
}

func TestVerifierOutputIsCappedBeforeDiskAndMemoryReadback(t *testing.T) {
	eng, state, _ := newTestEngine(t, true, "")
	claim := "22222222222222222222222222222222"
	receipt, err := state.VerifierReceiptPath(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	code, detail, err := eng.runVerifierCommand(context.Background(), t.TempDir(), receipt, claim, []string{"/bin/sh", "-c", "dd if=/dev/zero bs=1048576 count=2 2>/dev/null"})
	if err != nil || code == 0 {
		t.Fatalf("noisy verifier was not bounded: code=%d err=%v", code, err)
	}
	info, err := os.Stat(receipt + ".output")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > maxVerifierOutputBytes || !strings.Contains(detail, "output truncated") {
		t.Fatalf("size=%d detail suffix=%q", info.Size(), detail[max(0, len(detail)-80):])
	}
}

func TestVerifierReceiptRejectsEmbeddedClaimMismatch(t *testing.T) {
	eng, state, _ := newTestEngine(t, true, "")
	claim := "0123456789abcdef0123456789abcdef"
	receipt, err := state.VerifierReceiptPath(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	if err := eng.publishVerifierReceipt(receipt, verifierReceiptRecord{Version: verifierReceiptVersion, Claim: claim, ExitCode: 0, Output: "complete"}); err != nil {
		t.Fatal(err)
	}
	mismatch := verifierReceiptRecord{Version: verifierReceiptVersion, Claim: "fedcba9876543210fedcba9876543210", ExitCode: 0, Output: "forged"}
	payload, err := json.Marshal(mismatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receipt, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.readVerifierReceiptFiles(receipt, claim); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched claim err=%v", err)
	}
}

func TestExpiredVerifierUsesDurableCompletionEvidenceWithoutRerun(t *testing.T) {
	first, second, firstEngine, secondEngine, _, run := newCrossProcessEffectRun(t, "/usr/bin/false")
	base := time.Now()
	if err := first.Transition(context.Background(), run.ID, store.StateRunning, store.StateSettled, "agent settled", base); err != nil {
		t.Fatal(err)
	}
	claim, receipt, err := first.ClaimVerifier(context.Background(), run.ID, "stopped-verifier", base, base.Add(time.Minute))
	if err != nil || claim == "" || receipt == "" {
		t.Fatalf("claim=%q receipt=%q err=%v", claim, receipt, err)
	}
	if err := firstEngine.prepareVerifierReceipt(claim, receipt); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "verify.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'verified before persistence\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if code, detail, err := firstEngine.runVerifierCommand(context.Background(), run.WorktreePath, receipt, claim, []string{script}); err != nil || code != 0 || detail != "verified before persistence" {
		t.Fatalf("code=%d detail=%q err=%v", code, detail, err)
	}
	for _, path := range []string{receipt, receipt + ".output"} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("path=%s info=%v err=%v", path, info, err)
		}
	}
	if _, err := os.Lstat(receipt + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary marker remains: %v", err)
	}
	secondEngine.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := secondEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, second, run.ID)
	if got.State != store.StateSucceeded || got.TaskVerdict != "passed" || got.InfrastructureResult != "completed" || got.ErrorDetail != "verified before persistence" {
		t.Fatalf("run=%+v", got)
	}
}

func TestCancellationRejectsTerminalRunWithoutClosingWorkspace(t *testing.T) {
	first, _, firstEngine, _, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	now := time.Now()
	if err := first.Finish(context.Background(), run.ID, store.StateRunning, store.StateSucceeded, "completed", "completed", "passed", "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := firstEngine.Cancel(context.Background(), run.ID, now); err == nil {
		t.Fatal("terminal cancellation succeeded")
	}
	got, err := first.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateSucceeded || client.closeCount != 0 {
		t.Fatalf("run=%+v closes=%d", got, client.closeCount)
	}
}

func TestExpiredCloseRecoversItsPersistedCancellationOutcome(t *testing.T) {
	first, _, firstEngine, _, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	base := time.Now()
	intent, err := json.Marshal(workspaceCloseIntent{TerminalState: store.StateCancelled, Infrastructure: "completed", Agent: "cancelled", Verdict: "unverified", Code: "cancelled", Detail: "workspace closed by explicit cancellation"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := first.ClaimWorkspaceClose(context.Background(), run.ID, store.StateRunning, "stopped-closer", string(intent), base, base.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("claim=%q err=%v", claim, err)
	}
	client.status = "gone"
	client.workspaceExistSet = true
	client.workspaceExists = false
	firstEngine.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := firstEngine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, first, run.ID)
	if got.State != store.StateCancelled || got.ErrorCode != "cancelled" || got.AgentResult != "cancelled" {
		t.Fatalf("run=%+v", got)
	}
}

func TestCrossProcessCancellationClosesWorkspaceOnce(t *testing.T) {
	first, second, firstEngine, secondEngine, client, run := newCrossProcessEffectRun(t, "/usr/bin/true")
	started := make(chan struct{}, 2)
	gate := make(chan struct{})
	client.closeStarted = started
	client.closeGate = gate
	ctx := context.Background()
	errs := make(chan error, 2)
	go func() { errs <- firstEngine.Cancel(ctx, run.ID, time.Now()) }()
	go func() { errs <- secondEngine.Cancel(ctx, run.ID, time.Now()) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("workspace close did not start")
	}
	secondClose := false
	select {
	case <-started:
		secondClose = true
	case <-time.After(150 * time.Millisecond):
	}
	close(gate)
	firstErr, secondErr := <-errs, <-errs
	if secondClose {
		t.Fatal("two scheduler processes closed the same workspace concurrently")
	}
	if (firstErr == nil) == (secondErr == nil) {
		t.Fatalf("exactly one cancellation should own the effect; errors: %v / %v", firstErr, secondErr)
	}
	got := waitForRunTerminal(t, first, run.ID)
	if got.State != store.StateCancelled {
		t.Fatalf("run=%+v", got)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closeCount != 1 {
		t.Fatalf("workspace closed %d times", client.closeCount)
	}
	_ = second
}

func TestAmbiguousAgentStartWaitsForReconciliationWithoutDuplicateStart(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	base := time.Now()
	eng.Now = func() time.Time { return base }
	client.startAgentErr = errors.New("start response lost")
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	var run store.Run
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := state.ListRuns(ctx, "docs-drift", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == 1 && !eng.InFlight(runs[0].ID) {
			run = runs[0]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if run.State != store.StateStarting || run.ErrorCode == "agent_start_failed" {
		t.Fatalf("ambiguous start was terminalized: %+v", run)
	}
	client.startAgentErr = nil
	eng.Now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := eng.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	got := waitForRunTerminal(t, state, run.ID)
	client.mu.Lock()
	defer client.mu.Unlock()
	if got.State != store.StateInterrupted || client.startAgentCount != 1 || client.closeCount != 1 {
		t.Fatalf("run=%+v starts=%d closes=%d", got, client.startAgentCount, client.closeCount)
	}
}

func TestConfirmStartingClaimFaultAfterSubmitSurfacesDurableEvidence(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "provider model\nopenai-codex gpt-5.6-sol\n")
	ctx := context.Background()
	cfg, err := config.Load(eng.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := state.SyncJob(ctx, cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkRead(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	observer, err := store.Open(filepath.Join(state.StateDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	injected := errors.New("injected ConfirmStartingClaim failure")
	eng.confirmStartingClaim = func(context.Context, string, string, string, time.Time) (bool, error) {
		return false, injected
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var evidence store.Event
	for time.Now().Before(deadline) {
		events, eventErr := observer.Events(ctx, run.ID)
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		for _, event := range events {
			if event.Code == "background_store_error" {
				evidence = event
				break
			}
		}
		if evidence.Code != "" && !eng.InFlight(run.ID) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := observer.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	surfaced := eng.Dispatch(ctx)
	recovered := eng.Dispatch(ctx)
	client.mu.Lock()
	starts, submits := client.startAgentCount, client.submitCount
	client.mu.Unlock()
	if starts != 1 || submits != 1 || got.State != store.StateStarting || !got.Unread || evidence.Code != "background_store_error" || !strings.Contains(evidence.Detail, injected.Error()) || surfaced == nil || !strings.Contains(surfaced.Error(), injected.Error()) || recovered != nil {
		t.Fatalf("starts=%d submits=%d run=%+v evidence=%+v surfaced=%v recovered=%v", starts, submits, got, evidence, surfaced, recovered)
	}
}

func TestAgentNameAlwaysMeetsHerdrContract(t *testing.T) {
	cases := []string{
		"short_name",
		"123",
		"---___",
		"lifecycle-canary-20260823t1023-20260823T022324Z-69b4e8e287",
		"世界",
	}
	for _, runID := range cases {
		name := agentName(runID)
		if len(name) < 1 || len(name) > 32 {
			t.Errorf("agentName(%q) length=%d name=%q", runID, len(name), name)
			continue
		}
		if name[0] < 'a' || name[0] > 'z' {
			t.Errorf("agentName(%q) does not start with a lowercase letter: %q", runID, name)
		}
		for _, r := range name {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				t.Errorf("agentName(%q) contains invalid rune %q: %q", runID, r, name)
			}
		}
	}
}

func newCrossProcessEffectRun(t *testing.T, verifier string) (*store.Store, *store.Store, *Engine, *Engine, *fakeHerdr, store.Run) {
	t.Helper()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := writeJobs(t, repo, true)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "/usr/bin/true", verifier, 1))
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, "state.db")
	first, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Open(dbPath)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	client := &fakeHerdr{path: repo}
	firstEngine := New(first, client, fakeCommands{}, configPath)
	secondEngine := New(second, client, fakeCommands{}, configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, revision, err := cfg.Jobs[0].Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := first.SyncJob(context.Background(), cfg.Jobs[0].ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := first.CreateManualRun(context.Background(), cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetReceipt(context.Background(), run.ID, "w-effect", "p-effect", "auto/test", repo, adapter.ModeAgent, ""); err != nil {
		t.Fatal(err)
	}
	if err := first.Transition(context.Background(), run.ID, store.StateAccepted, store.StateRunning, "test running", now); err != nil {
		t.Fatal(err)
	}
	run.State, run.WorkspaceID, run.PaneID, run.WorktreePath = store.StateRunning, "w-effect", "p-effect", repo
	return first, second, firstEngine, secondEngine, client, run
}

func waitForFileLines(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fileLines(path) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not reach %d lines", path, want)
}

func fileLines(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return len(strings.Fields(string(raw)))
}

func waitForRunTerminal(t *testing.T, state *store.Store, runID string) store.Run {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := state.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if isTerminal(run.State) {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s did not become terminal", runID)
	return store.Run{}
}

func waitForTerminal(t *testing.T, state *store.Store, job string) store.Run {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := state.ListRuns(context.Background(), job, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) > 0 && isTerminal(runs[0].State) {
			return runs[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not become terminal")
	return store.Run{}
}
func isTerminal(state string) bool {
	return state == store.StateSucceeded || state == store.StateFailed || state == store.StateBlocked || state == store.StateTimedOut || state == store.StateCancelled || state == store.StateInterrupted
}
func mustTime(v string) time.Time {
	got, err := time.Parse(time.RFC3339, v)
	if err != nil {
		panic(err)
	}
	return got
}

func writeJobsWithAttention(t *testing.T, repo string, limit int, event bool) string {
	t.Helper()
	path := writeJobs(t, repo, true)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(raw), "    prompt: Check docs.", fmt.Sprintf("    attention:\n      max_unread_terminal_runs: %d\n    prompt: Check docs.", limit), 1)
	if event {
		body = strings.Replace(body, "kind: cron\n      expression: \"0 9 * * *\"", "kind: event", 1)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestUnreadGuardPausesScheduledAdmissionBeforeAnotherRun(t *testing.T) {
	repo := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, writeJobsWithAttention(t, repo, 1, false))
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateSucceeded || !run.Unread {
		t.Fatalf("first run=%+v", run)
	}
	// The next day's occurrence must pause the job before admitting anything.
	if err := eng.Evaluate(ctx, mustTime("2026-08-23T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	jobState, err := state.Job(ctx, "docs-drift")
	if err != nil {
		t.Fatal(err)
	}
	if !jobState.Paused || jobState.PauseReason != store.PauseReasonUnreadTerminalRuns || jobState.PauseAt.IsZero() {
		t.Fatalf("guard pause state=%+v", jobState)
	}
	runs, listErr := state.ListRuns(ctx, "docs-drift", 10)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
	client.mu.Lock()
	provisions := client.provisions
	client.mu.Unlock()
	if provisions != 1 {
		t.Fatalf("guard admitted a second workspace: provisions=%d", provisions)
	}
	// Marking the run read never auto-resumes the job.
	if err := state.MarkRead(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	jobState, err = state.Job(ctx, "docs-drift")
	if err != nil || !jobState.Paused {
		t.Fatalf("reading auto-resumed: %+v err=%v", jobState, err)
	}
}

func TestUnreadGuardPausesEventAdmission(t *testing.T) {
	repo := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, writeJobsWithAttention(t, repo, 1, true))
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	ctx := context.Background()
	if _, err := eng.Enqueue(ctx, "docs-drift", "first-event", mustTime("2026-08-22T01:00:00Z")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	first := waitForTerminal(t, state, "docs-drift")
	if !first.Unread {
		t.Fatalf("first run was not unread: %+v", first)
	}
	_, err = eng.Enqueue(ctx, "docs-drift", "second-event", mustTime("2026-08-22T02:00:00Z"))
	var notAccepted *EventNotAcceptedError
	if !errors.As(err, &notAccepted) || notAccepted.Outcome != "paused_unread_limit" {
		t.Fatalf("second enqueue err=%v", err)
	}
	jobState, err := state.Job(ctx, "docs-drift")
	if err != nil || !jobState.Paused || jobState.PauseReason != store.PauseReasonUnreadTerminalRuns {
		t.Fatalf("job=%+v err=%v", jobState, err)
	}
	runs, listErr := state.ListRuns(ctx, "docs-drift", 10)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
}

func TestUnreadGuardPausesManualAdmission(t *testing.T) {
	repo := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, writeJobsWithAttention(t, repo, 1, false))
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00+08:00")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30+08:00")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if !run.Unread {
		t.Fatalf("first run was not unread: %+v", run)
	}
	_, runErr := eng.RunNow(ctx, "docs-drift", false, mustTime("2026-08-22T10:00:00Z"))
	if !errors.Is(runErr, store.ErrJobUnreadPaused) {
		t.Fatalf("RunNow err=%v", runErr)
	}
	jobState, err := state.Job(ctx, "docs-drift")
	if err != nil || !jobState.Paused || jobState.PauseReason != store.PauseReasonUnreadTerminalRuns {
		t.Fatalf("job=%+v err=%v", jobState, err)
	}
	runs, listErr := state.ListRuns(ctx, "docs-drift", 10)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
}

func TestUnreadGuardStopsAtFirstDueOccurrenceWhenMultipleAreDue(t *testing.T) {
	repo := t.TempDir()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &fakeHerdr{path: repo}
	// Every minute, so two occurrences fall due inside one evaluation window.
	path := writeJobsWithAttention(t, repo, 1, false)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(raw), `"0 9 * * *"`, `"* * * * *"`, 1)
	// Widen the grace so both due occurrences are admissible rather than missed.
	body = strings.Replace(body, "catch_up_grace_minutes: 0", "catch_up_grace_minutes: 120", 1)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, path)
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	ctx := context.Background()
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T08:59:00Z")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:00:30Z")); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateSucceeded || !run.Unread {
		t.Fatalf("first run=%+v", run)
	}
	// Two further occurrences fall due in this single evaluation; the guard
	// must pause on the first and stop instead of returning ErrJobPaused.
	if err := eng.Evaluate(ctx, mustTime("2026-08-22T09:02:30Z")); err != nil {
		t.Fatalf("second evaluation returned %v; want nil (guard stops before the authority fence)", err)
	}
	jobState, err := state.Job(ctx, "docs-drift")
	if err != nil || !jobState.Paused || jobState.PauseReason != store.PauseReasonUnreadTerminalRuns {
		t.Fatalf("job=%+v err=%v", jobState, err)
	}
	runs, listErr := state.ListRuns(ctx, "docs-drift", 10)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
	last, err := state.LastJobResult(ctx, "docs-drift")
	if err != nil || last != "succeeded" {
		t.Fatalf("last result=%q err=%v; want succeeded from the first run only", last, err)
	}
}
