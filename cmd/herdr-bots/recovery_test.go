package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

// writeHerdrStub installs a stub herdr binary for the CLI path, which builds a
// real herdr client. The stub answers every query by printing its JSON with
// printf and single quotes: no temporary files, no argument forwarding, and
// no shell interpretation of the payload. The default stub reports no
// worktrees, the one observation explicit recovery needs to conclude
// verified absence.
func writeHerdrStub(t *testing.T, dir, output string) {
	t.Helper()
	path := filepath.Join(dir, "herdr-stub.sh")
	script := "#!/bin/sh\nprintf '%s\\n' " + "'" + strings.ReplaceAll(output, "'", `'\''`) + "'\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_BIN_PATH", path)
}

// cliRecoveryFixture creates one synthetic provisioning run whose claim
// expired in the past, backed by a synthetic saved snapshot with an absolute
// repository and worktree execution mode.
func cliRecoveryFixture(t *testing.T, dir string) (string, string) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().Add(-2 * time.Minute)
	statePath := filepath.Join(dir, "state.db")
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	job := config.Job{ID: "held", Execution: config.Execution{Repository: dir, Workspace: config.WorkspaceWorktree, Harness: config.HarnessPi, Model: "gpt-5.6-sol"}}
	snapshot, revision, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.SyncJob(ctx, "held", revision, snapshot, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, "held", revision, snapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := state.DecideAdmission(ctx, run.ID, "stopped-daemon", "1", 1.25, base, base.Add(time.Minute), func([]store.Run) (store.AdmissionDecision, error) {
		return store.AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		state.Close()
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	if _, err := state.SaveProvisioningPlan(ctx, run.ID, "stopped-daemon", "auto/held/planned", base); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	return run.ID, statePath
}

func TestRecoverCLIValidatesArityBeforeOpeningStore(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "nested", "state.db")
	for _, args := range [][]string{
		{"recover", "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath},
		{"recover", "first", "second", "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath},
	} {
		err := run(args)
		if err == nil || !strings.Contains(err.Error(), "one run id") {
			t.Fatalf("args=%v error=%v", args, err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, "nested")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("args=%v opened a writable store before arity validation", args)
		}
	}
}

func TestRecoverCLIRetiresRunAndAcceptsBothArgumentOrders(t *testing.T) {
	dir := t.TempDir()
	writeHerdrStub(t, dir, `{"result":{"worktrees":[]}}`)
	firstID, statePath := cliRecoveryFixture(t, dir)

	out, err := captureRunStdout(t, func() error {
		return run([]string{"recover", firstID, "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, firstID) || !strings.Contains(out, store.StateInterrupted) {
		t.Fatalf("output=%q want run id and interrupted state", out)
	}
	state, openErr := store.Open(statePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	first := mustGetCLIRun(t, state, firstID)
	if first.State != store.StateInterrupted || first.ErrorCode != store.RecoveryCodeProvisioning {
		t.Fatalf("run=%+v", first)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	secondID, statePath := cliRecoveryFixture(t, filepath.Join(dir, "second"))
	out, err = captureRunStdout(t, func() error {
		return run([]string{"recover", "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath, secondID})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, secondID) || !strings.Contains(out, store.StateInterrupted) {
		t.Fatalf("flags-first output=%q", out)
	}
}

func TestRecoverCLITerminalRetryChangesNothing(t *testing.T) {
	dir := t.TempDir()
	writeHerdrStub(t, dir, `{"result":{"worktrees":[]}}`)
	runID, statePath := cliRecoveryFixture(t, dir)
	if err := run([]string{"recover", runID, "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath}); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	retired := mustGetCLIRun(t, state, runID)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	retryErr := run([]string{"recover", runID, "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath})
	if retryErr == nil || !strings.Contains(retryErr.Error(), "provisioning") {
		t.Fatalf("retry error=%v", retryErr)
	}
	state, err = store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	after := mustGetCLIRun(t, state, runID)
	if after.State != retired.State || after.UpdatedAt != retired.UpdatedAt || after.ErrorCode != retired.ErrorCode {
		t.Fatalf("retry changed the retired run: before=%+v after=%+v", retired, after)
	}
	events, err := state.Events(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	typed := 0
	for _, event := range events {
		if event.Code == store.RecoveryCodeProvisioning {
			typed++
		}
	}
	if typed != 1 {
		t.Fatalf("typed recovery events=%d want 1", typed)
	}
}

func TestRecoverCLIRefusesWhenAWorktreeStillExists(t *testing.T) {
	dir := t.TempDir()
	writeHerdrStub(t, dir, `{"result":{"worktrees":[{"branch":"auto/held/planned","path":"/tmp/synthetic-wt"}]}}`)
	runID, statePath := cliRecoveryFixture(t, dir)
	err := run([]string{"recover", runID, "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath})
	if err == nil || !strings.Contains(err.Error(), "manual cleanup") {
		t.Fatalf("error=%v", err)
	}
	state, openErr := store.Open(statePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer state.Close()
	got := mustGetCLIRun(t, state, runID)
	if got.State != store.StateProvisioning || got.WorkspaceID != "" {
		t.Fatalf("refusal changed the run: %+v", got)
	}
}

func TestRecoverCLIRefusesMalformedInventoryWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	writeHerdrStub(t, dir, `{}`)
	runID, statePath := cliRecoveryFixture(t, dir)
	err := run([]string{"recover", runID, "--config", filepath.Join(dir, "missing.yaml"), "--state", statePath})
	if err == nil || !strings.Contains(err.Error(), "worktree inventory") {
		t.Fatalf("error=%v want a fail-closed inventory error", err)
	}
	state, openErr := store.Open(statePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer state.Close()
	got := mustGetCLIRun(t, state, runID)
	if got.State != store.StateProvisioning || got.WorkspaceID != "" || got.ProvisioningOwner != "stopped-daemon" || got.ErrorCode != "" {
		t.Fatalf("malformed inventory mutated the run: %+v", got)
	}
	events, eventErr := state.Events(context.Background(), runID)
	if eventErr != nil {
		t.Fatal(eventErr)
	}
	for _, event := range events {
		if event.Code == store.RecoveryCodeProvisioning {
			t.Fatalf("malformed inventory wrote a typed recovery event: %+v", event)
		}
	}
}

func TestUsageDocumentsRecoverCommand(t *testing.T) {
	out, err := captureRunStdout(t, func() error {
		return run([]string{"help"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "herdr-bots recover RUN [--config PATH] [--state PATH]") {
		t.Fatalf("usage does not document recover:\n%s", out)
	}
}

func mustGetCLIRun(t *testing.T, state *store.Store, id string) store.Run {
	t.Helper()
	run, err := state.GetRun(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}
