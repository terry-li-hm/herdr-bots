package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

func TestResolveVersion(t *testing.T) {
	build := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.1.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abcdef123456"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	for name, tc := range map[string]struct {
		linked string
		info   *debug.BuildInfo
		ok     bool
		want   string
	}{
		"linked wins":       {linked: "v9.9.9", info: build, ok: true, want: "v9.9.9"},
		"module fallback":   {linked: "dev", info: build, ok: true, want: "v0.1.0"},
		"revision fallback": {linked: "dev", info: &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: build.Settings}, ok: true, want: "abcdef123456-dirty"},
		"clean revision":    {linked: "dev", info: &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}}, ok: true, want: "abc"},
		"no build info":     {linked: "dev", ok: false, want: "dev"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := resolveVersion(tc.linked, tc.info, tc.ok); got != tc.want {
				t.Fatalf("resolveVersion() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPositionalFirstAllowsDocumentedJobThenFlags(t *testing.T) {
	got := positionalFirst([]string{"job", "--canary", "--state", "/tmp/state"})
	want := []string{"--canary", "--state", "/tmp/state", "job"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestAttendedRunReportsCapacityHoldWithoutWaitingForever(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "automations.yaml")
	statePath := filepath.Join(dir, "state.db")
	body := `version: 1
capacity:
  max_concurrent_runs: 2
  min_free_disk_gib: 1024
jobs:
  - id: held
    enabled: true
    schedule:
      kind: cron
      expression: "0 9 * * *"
      timezone: Asia/Hong_Kong
    execution:
      repository: ` + dir + `
      workspace: worktree
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    prompt: Inspect only.
    limits:
      max_runs_per_day: 1
      disk_reserve_gib: 64
`
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"run", "held", "--config", configPath, "--state", statePath})
	if err == nil || !strings.Contains(err.Error(), "accepted but held") {
		t.Fatalf("expected capacity hold, got %v", err)
	}
	state, openErr := store.Open(statePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer state.Close()
	runs, listErr := state.ListRuns(context.Background(), "held", 1)
	if listErr != nil || len(runs) != 1 || runs[0].State != store.StateAccepted {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
}

func writeCLIEventConfig(t *testing.T, dir string, enabled bool) string {
	t.Helper()
	path := filepath.Join(dir, "events.yaml")
	body := `version: 1
jobs:
  - id: repair
    enabled: ` + fmt.Sprintf("%t", enabled) + `
    schedule:
      kind: event
      timezone: Asia/Hong_Kong
    execution:
      repository: ` + dir + `
      workspace: worktree
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    prompt: Use only saved authority.
    overlap: forbid
    limits:
      max_runs_per_day: 10
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEnqueueCLIIsLocalTypedAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	configPath := writeCLIEventConfig(t, dir, true)
	statePath := filepath.Join(dir, "state.db")
	args := []string{"enqueue", "repair", "--event-id", "health-20260823", "--config", configPath, "--state", statePath}
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	runs, err := state.ListRuns(context.Background(), "repair", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if runs[0].Trigger != "event" || runs[0].State != store.StateAccepted || runs[0].InputContext != "" || !strings.Contains(string(runs[0].Definition), "Use only saved authority") {
		t.Fatalf("run=%+v definition=%s", runs[0], runs[0].Definition)
	}
}

func TestEnqueueCLIRejectsContextInputsAndInvalidIDs(t *testing.T) {
	dir := t.TempDir()
	configPath := writeCLIEventConfig(t, dir, true)
	statePath := filepath.Join(dir, "state.db")
	for _, args := range [][]string{
		{"enqueue", "repair", "--config", configPath, "--state", statePath},
		{"enqueue", "repair", "--event-id", "INVALID", "--config", configPath, "--state", statePath},
		{"enqueue", "repair", "--event-id", "valid", "--payload", "{}", "--config", configPath, "--state", statePath},
		{"enqueue", "repair", "--event-id", "valid", "--prompt", "override", "--config", configPath, "--state", statePath},
		{"enqueue", "repair", "--event-id", "valid", "--file", "/tmp/context", "--config", configPath, "--state", statePath},
	} {
		if err := run(args); err == nil {
			t.Fatalf("accepted forbidden args: %v", args)
		}
	}
}

func TestRunEventJobRequiresCanaryFlag(t *testing.T) {
	dir := t.TempDir()
	configPath := writeCLIEventConfig(t, dir, true)
	statePath := filepath.Join(dir, "state.db")
	err := run([]string{"run", "repair", "--config", configPath, "--state", statePath})
	if err == nil || !strings.Contains(err.Error(), "--canary") {
		t.Fatalf("error=%v", err)
	}
	state, openErr := store.Open(statePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer state.Close()
	runs, listErr := state.ListRuns(context.Background(), "repair", 10)
	if listErr != nil || len(runs) != 0 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
}

func TestHeldEventCanaryIsTerminalAndCannotBeRestarted(t *testing.T) {
	dir := t.TempDir()
	configPath := writeCLIEventConfig(t, dir, true)
	statePath := filepath.Join(dir, "state.db")
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetGlobalPaused(context.Background(), true); err != nil {
		state.Close()
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	err = run([]string{"run", "repair", "--canary", "--config", configPath, "--state", statePath})
	if err == nil || !strings.Contains(err.Error(), "globally paused") {
		t.Fatalf("error=%v", err)
	}
	state, err = store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	runs, err := state.ListRuns(context.Background(), "repair", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if runs[0].Trigger != "canary" || runs[0].State != store.StateBlocked || runs[0].ErrorCode != "global_paused" {
		t.Fatalf("run=%+v", runs[0])
	}
}

func TestAttendedWaitDoesNotCallForeignOwnedRunStalled(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := state.SyncJob(ctx, "held", "rev1", []byte(`{"id":"held"}`), true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, "held", "rev1", []byte(`{"id":"held"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	// Another process owns this run durably: it is deep in a nonterminal state
	// no executor in this process is responsible for.
	for _, step := range []struct{ from, to string }{
		{store.StateAccepted, store.StateProvisioning},
		{store.StateProvisioning, store.StateStarting},
		{store.StateStarting, store.StateRunning},
	} {
		if err := state.Transition(ctx, run.ID, step.from, step.to, "foreign process", now); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		time.Sleep(600 * time.Millisecond)
		if err := state.Transition(ctx, run.ID, store.StateRunning, store.StateSettled, "foreign process", time.Now()); err != nil {
			t.Error(err)
			return
		}
		if err := state.Finish(ctx, run.ID, store.StateSettled, store.StateSucceeded, "completed", "completed", "unverified", "", "foreign process", time.Now()); err != nil {
			t.Error(err)
		}
	}()
	if err := awaitRun(ctx, state, run.ID, time.Now().Add(30*time.Second)); err != nil {
		t.Fatalf("foreign-owned run was not waited on: %v", err)
	}
	final, err := state.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != store.StateSucceeded {
		t.Fatalf("run=%+v", final)
	}
}

func TestListReportsPauseReasonWithoutMarkingRead(t *testing.T) {
	dir := t.TempDir()
	configPath := writeCLIEventConfig(t, dir, true)
	// The event job reaches its unread-work guard limit after one unread run.
	body, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "    overlap: forbid", "    overlap: forbid\n    attention:\n      max_unread_terminal_runs: 1", 1))
	if err := os.WriteFile(configPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "state.db")
	if err := run([]string{"enqueue", "repair", "--event-id", "guard-1", "--config", configPath, "--state", statePath}); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := state.ListRuns(context.Background(), "repair", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if err := state.Finish(context.Background(), runs[0].ID, store.StateAccepted, store.StateSucceeded, "completed", "completed", "unverified", "", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	// The next event delivery trips the guard and durably pauses the job.
	if err := run([]string{"enqueue", "repair", "--event-id", "guard-2", "--config", configPath, "--state", statePath}); err == nil || !strings.Contains(err.Error(), "unread terminal") {
		t.Fatalf("expected guard pause error, got %v", err)
	}

	original := os.Stdout
	var captured []byte
	read, writeEnd, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := read.Read(buf)
			captured = append(captured, buf[:n]...)
			if readErr != nil {
				close(done)
				return
			}
		}
	}()
	os.Stdout = writeEnd
	listErr := run([]string{"list", "--config", configPath, "--state", statePath})
	os.Stdout = original
	_ = writeEnd.Close()
	<-done
	_ = read.Close()
	if listErr != nil {
		t.Fatal(listErr)
	}
	if !strings.Contains(string(captured), store.PauseReasonUnreadTerminalRuns) {
		t.Fatalf("list output did not report the pause reason: %q", captured)
	}

	state, err = store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	job, err := state.Job(context.Background(), "repair")
	if err != nil || !job.Paused || job.PauseReason != store.PauseReasonUnreadTerminalRuns {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	runs, err = state.ListRuns(context.Background(), "repair", 10)
	if err != nil || len(runs) != 1 || !runs[0].Unread {
		t.Fatalf("list must not mark runs read: runs=%+v err=%v", runs, err)
	}
}

func captureRunStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	original := os.Stdout
	read, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var captured []byte
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := read.Read(buf)
			captured = append(captured, buf[:n]...)
			if readErr != nil {
				close(done)
				return
			}
		}
	}()
	os.Stdout = writeEnd
	runErr := fn()
	os.Stdout = original
	_ = writeEnd.Close()
	<-done
	_ = read.Close()
	return string(captured), runErr
}

func captureRunStderr(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	original := os.Stderr
	read, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var captured []byte
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := read.Read(buf)
			captured = append(captured, buf[:n]...)
			if readErr != nil {
				close(done)
				return
			}
		}
	}()
	os.Stderr = writeEnd
	runErr := fn()
	os.Stderr = original
	_ = writeEnd.Close()
	<-done
	_ = read.Close()
	return string(captured), runErr
}

func TestRunsLimitHelpExplainsTerminalHistoryAndActiveRows(t *testing.T) {
	help, err := captureRunStderr(t, func() error {
		return run([]string{"runs", "--help"})
	})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("runs --help error=%v", err)
	}
	if !strings.Contains(help, "maximum terminal-history rows; active rows are always shown") {
		t.Fatalf("runs --limit help is unclear:\n%s", help)
	}
}

func TestRunsAndShowDisplayAcceptanceGroupingAndLegacyFields(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
	if _, err := state.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	createRun := func(job config.Job, at time.Time, finish bool) store.Run {
		raw, _, err := job.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		run, err := state.CreateManualRun(context.Background(), "job", "rev1", raw, at)
		if err != nil {
			t.Fatal(err)
		}
		if finish {
			if err := state.Finish(context.Background(), run.ID, store.StateAccepted, store.StateSucceeded, "completed", "completed", "passed", "", run.ID, at.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := state.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	mandatory := createRun(config.Job{ID: "job"}, now, true)
	sample := createRun(config.Job{ID: "job", Verifier: &config.Verifier{Command: []string{"git", "diff", "--check"}}, Acceptance: &config.Acceptance{Mode: config.AcceptanceSample, SamplePercent: 100}}, now.Add(time.Minute), true)
	auto := createRun(config.Job{ID: "job", Verifier: &config.Verifier{Command: []string{"git", "diff", "--check"}}, Acceptance: &config.Acceptance{Mode: config.AcceptanceAuto}}, now.Add(2*time.Minute), true)
	active := createRun(config.Job{ID: "job"}, now.Add(3*time.Minute), false)
	if err := state.Transition(context.Background(), active.ID, store.StateAccepted, store.StateProvisioning, "provisioning", now.Add(3*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	runsOutput, err := captureRunStdout(t, func() error {
		return run([]string{"runs", "--state", statePath})
	})
	if err != nil {
		t.Fatal(err)
	}
	indices := []int{strings.Index(runsOutput, mandatory.ID), strings.Index(runsOutput, sample.ID), strings.Index(runsOutput, auto.ID), strings.Index(runsOutput, active.ID)}
	for _, idx := range indices {
		if idx == -1 {
			t.Fatalf("missing run in output:\n%s", runsOutput)
		}
	}
	if !(indices[0] < indices[1] && indices[1] < indices[2] && indices[2] < indices[3]) {
		t.Fatalf("unexpected grouping order:\n%s", runsOutput)
	}
	for _, want := range []string{"VERDICT", "LANE", "REASON", "STATE", "unverified", store.StateProvisioning} {
		if !strings.Contains(runsOutput, want) {
			t.Fatalf("runs output missing %q:\n%s", want, runsOutput)
		}
	}
	zeroOutput, err := captureRunStdout(t, func() error {
		return run([]string{"runs", "--limit", "0", "--state", statePath})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(zeroOutput, active.ID) {
		t.Fatalf("limit zero hid active work:\n%s", zeroOutput)
	}
	for _, terminalID := range []string{mandatory.ID, sample.ID, auto.ID} {
		if strings.Contains(zeroOutput, terminalID) {
			t.Fatalf("limit zero included terminal history %s:\n%s", terminalID, zeroOutput)
		}
	}
	showOutput, err := captureRunStdout(t, func() error {
		return run([]string{"show", active.ID, "--state", statePath})
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"lane: -", "reason: -", "state: provisioning"} {
		if !strings.Contains(showOutput, want) {
			t.Fatalf("show output missing %q:\n%s", want, showOutput)
		}
	}
}

func TestAutoAcceptanceDoesNotTripUnreadGuard(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "events.yaml")
	statePath := filepath.Join(dir, "state.db")
	body := `version: 1
jobs:
  - id: repair
    enabled: true
    schedule:
      kind: event
      timezone: Asia/Hong_Kong
    execution:
      repository: ` + dir + `
      workspace: worktree
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    prompt: Use only saved authority.
    overlap: forbid
    attention:
      max_unread_terminal_runs: 1
    verifier:
      command: ["git", "diff", "--check"]
    acceptance:
      mode: auto
    limits:
      max_runs_per_day: 10
`
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"enqueue", "repair", "--event-id", "guard-1", "--config", configPath, "--state", statePath}); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := state.ListRuns(context.Background(), "repair", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if err := state.Finish(context.Background(), runs[0].ID, store.StateAccepted, store.StateSucceeded, "completed", "completed", "passed", "", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	doneRun, err := state.GetRun(context.Background(), runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if doneRun.Unread {
		t.Fatalf("auto lane should terminalize as read: %+v", doneRun)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"enqueue", "repair", "--event-id", "guard-2", "--config", configPath, "--state", statePath}); err != nil {
		t.Fatalf("auto-reviewed terminal run should not pause future admission: %v", err)
	}
}
