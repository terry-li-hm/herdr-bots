package herdr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWorktreeCreateArgsPinTheBaseRevision(t *testing.T) {
	got := worktreeCreateArgs("/repo", "abc123", "auto/review", "auto: review")
	want := []string{"worktree", "create", "--cwd", "/repo", "--branch", "auto/review", "--base", "abc123", "--label", "auto: review", "--no-focus"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestWorkspaceExistsUsesWorkspaceIdentity(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
if [ "$3" = "missing" ]; then
  printf '%s\n' '{"error":{"code":"workspace_not_found","message":"gone"}}'
  exit 1
fi
printf '%s\n' '{"result":{"workspace":{"workspace_id":"w7"}}}'
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	client := &CLI{Bin: bin}
	exists, err := client.WorkspaceExists(context.Background(), "w7")
	if err != nil || !exists {
		t.Fatalf("exists=%t err=%v", exists, err)
	}
	exists, err = client.WorkspaceExists(context.Background(), "missing")
	if err != nil || exists {
		t.Fatalf("missing exists=%t err=%v", exists, err)
	}
}

func TestRunParsesStructuredErrorFromStderrBeforePlainFallback(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
case "$1" in
  structured)
    printf '%s\n' '{"error":{"code":"timeout","message":"wait expired"}}' >&2
    ;;
  plain)
    printf '%s\n' 'plain stderr failure' >&2
    ;;
esac
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	client := &CLI{Bin: bin}
	if err := client.run(context.Background(), nil, "structured"); !hasCode(err, "timeout") || err.Error() != "timeout: wait expired" {
		t.Fatalf("structured stderr err=%v", err)
	}
	if err := client.run(context.Background(), nil, "plain"); err == nil || hasCode(err, "timeout") || err.Error() != "plain stderr failure" {
		t.Fatalf("plain stderr err=%v", err)
	}
}

func TestStatusMapsMissingAgentToGone(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
printf '%s\n' '{"error":{"code":"agent_not_running","message":"gone"}}'
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	status, err := (&CLI{Bin: bin}).Status(context.Background(), "p-gone")
	if err != nil || status != "gone" {
		t.Fatalf("status=%q err=%v", status, err)
	}
}

func TestProvisionReturnsPartialReceiptWhenPathLookupFails(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
if [ "$1 $2" = "worktree create" ]; then
  printf '%s\n' '{"result":{"workspace":{"workspace_id":"w7"},"root_pane":{"pane_id":"p7"}}}'
elif [ "$1 $2" = "worktree list" ]; then
  printf '%s\n' '{"result":{"worktrees":[]}}'
else
  exit 2
fi
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	receipt, err := (&CLI{Bin: bin}).Provision(context.Background(), "/repo", "worktree", "abc", "auto/job/planned", "auto: job")
	if err == nil || receipt.WorkspaceID != "w7" || receipt.PaneID != "p7" || receipt.Branch != "auto/job/planned" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestFindWorkspaceByBranchRecoversProvisionedWorktree(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
if [ "$1 $2" = "worktree list" ]; then
  printf '%s\n' '{"result":{"worktrees":[{"branch":"auto/job/planned","path":"/tmp/planned"}]}}'
elif [ "$1 $2" = "workspace list" ]; then
  printf '%s\n' '{"result":{"workspaces":[{"workspace_id":"w9","worktree":{"checkout_path":"/tmp/planned"}}]}}'
else
  exit 2
fi
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := (&CLI{Bin: bin}).FindWorkspaceByBranch(context.Background(), "/repo", "auto/job/planned")
	if err != nil || !found || receipt.WorkspaceID != "w9" || receipt.Path != "/tmp/planned" {
		t.Fatalf("receipt=%+v found=%t err=%v", receipt, found, err)
	}
}

func TestFindWorkspaceByBranchPreservesUnownedWorktreeEvidence(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fake-herdr")
	script := `#!/bin/sh
if [ "$1 $2" = "worktree list" ]; then
  printf '%s\n' '{"result":{"worktrees":[{"branch":"auto/job/planned","path":"/tmp/planned"}]}}'
elif [ "$1 $2" = "workspace list" ]; then
  printf '%s\n' '{"result":{"workspaces":[]}}'
else
  exit 2
fi
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := (&CLI{Bin: bin}).FindWorkspaceByBranch(context.Background(), "/repo", "auto/job/planned")
	if err != nil || !found || receipt.WorkspaceID != "" || receipt.Path != "/tmp/planned" {
		t.Fatalf("receipt=%+v found=%t err=%v", receipt, found, err)
	}
}

func TestShellQuoteKeepsACompoundCommandInOneArgument(t *testing.T) {
	got := shellQuote("claude -p 'prompt'; printf done")
	want := `'claude -p '\''prompt'\''; printf done'`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestSubmitRecoversAStagedPromptWithOneCanonicalEnter(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr")
	logPath := filepath.Join(dir, "calls.log")
	sentPath := filepath.Join(dir, "sent")
	script := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$HERDR_FAKE_LOG"
case "$1 $2" in
  "agent prompt")
    printf '%s\n' '{"error":{"code":"agent_prompt_stalled","message":"stalled"}}'
    exit 1
    ;;
  "agent get")
    seq=10
    [ ! -e "$HERDR_FAKE_SENT" ] || seq=11
    printf '{"result":{"agent":{"agent_status":"idle","state_change_seq":%s}}}\n' "$seq"
    ;;
  "agent send-keys")
    [ "$3" = "p1" ]
    [ "$4" = "enter" ]
    : > "$HERDR_FAKE_SENT"
    printf '%s\n' '{"result":{}}'
    ;;
  *)
    exit 22
    ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_FAKE_LOG", logPath)
	t.Setenv("HERDR_FAKE_SENT", sentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := (&CLI{Bin: bin}).Submit(ctx, "p1", "hello"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	calls := string(raw)
	if strings.Count(calls, "agent prompt ") != 1 || strings.Count(calls, "agent send-keys p1 enter") != 1 {
		t.Fatalf("unexpected calls:\n%s", calls)
	}
}

func TestWaitRetriesATransientStatusTimeout(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr")
	countPath := filepath.Join(dir, "status-count")
	waitCountPath := filepath.Join(dir, "wait-count")
	script := `#!/bin/sh
set -eu
case "$1 $2" in
  "agent wait")
    count=0
    [ ! -e "$HERDR_WAIT_COUNT" ] || count=$(cat "$HERDR_WAIT_COUNT")
    printf '%s' "$((count + 1))" > "$HERDR_WAIT_COUNT"
    printf '%s\n' '{"result":{}}'
    ;;
  "agent get")
    count=0
    [ ! -e "$HERDR_STATUS_COUNT" ] || count=$(cat "$HERDR_STATUS_COUNT")
    count=$((count + 1))
    printf '%s' "$count" > "$HERDR_STATUS_COUNT"
    if [ "$count" -eq 1 ]; then
      printf '%s\n' '{"error":{"code":"timeout","message":"timed out waiting for agent status"}}'
      exit 1
    fi
    printf '%s\n' '{"result":{"agent":{"agent_status":"done","state_change_seq":12}}}'
    ;;
  *)
    exit 22
    ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_STATUS_COUNT", countPath)
	t.Setenv("HERDR_WAIT_COUNT", waitCountPath)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := (&CLI{Bin: bin}).Wait(ctx, "p1", 5*time.Second)
	if err != nil || status != "done" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	raw, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "2" {
		t.Fatalf("status calls=%q", raw)
	}
	waitRaw, err := os.ReadFile(waitCountPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(waitRaw) != "1" {
		t.Fatalf("wait calls=%q", waitRaw)
	}
}

func TestWaitRetriesTimeoutEnvelopeFromStderr(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr")
	waitCountPath := filepath.Join(dir, "wait-count")
	script := `#!/bin/sh
set -eu
case "$1 $2" in
  "agent wait")
    count=0
    [ ! -e "$HERDR_WAIT_COUNT" ] || count=$(cat "$HERDR_WAIT_COUNT")
    count=$((count + 1))
    printf '%s' "$count" > "$HERDR_WAIT_COUNT"
    if [ "$count" -eq 1 ]; then
      printf '%s\n' '{"error":{"code":"timeout","message":"timed out waiting for agent"}}' >&2
      exit 1
    fi
    printf '%s\n' '{"result":{}}'
    ;;
  "agent get")
    printf '%s\n' '{"result":{"agent":{"agent_status":"done","state_change_seq":12}}}'
    ;;
  *)
    exit 22
    ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_WAIT_COUNT", waitCountPath)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := (&CLI{Bin: bin}).Wait(ctx, "p1", 5*time.Second)
	if err != nil || status != "done" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	waitRaw, err := os.ReadFile(waitCountPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(waitRaw) != "2" {
		t.Fatalf("wait calls=%q", waitRaw)
	}
}

func TestWaitReobservesGoneAfterCompletedWait(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr")
	countPath := filepath.Join(dir, "status-count")
	waitCountPath := filepath.Join(dir, "wait-count")
	script := `#!/bin/sh
set -eu
case "$1 $2" in
  "agent wait")
    printf '%s' '1' > "$HERDR_WAIT_COUNT"
    printf '%s\n' '{"result":{}}'
    ;;
  "agent get")
    count=0
    [ ! -e "$HERDR_STATUS_COUNT" ] || count=$(cat "$HERDR_STATUS_COUNT")
    count=$((count + 1))
    printf '%s' "$count" > "$HERDR_STATUS_COUNT"
    if [ "$count" -eq 1 ]; then
      printf '%s\n' '{"error":{"code":"agent_not_running","message":"gone"}}'
      exit 1
    fi
    printf '%s\n' '{"result":{"agent":{"agent_status":"done","state_change_seq":12}}}'
    ;;
  *)
    exit 22
    ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_STATUS_COUNT", countPath)
	t.Setenv("HERDR_WAIT_COUNT", waitCountPath)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := (&CLI{Bin: bin}).Wait(ctx, "p1", 5*time.Second)
	if err != nil || status != "done" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	waitRaw, err := os.ReadFile(waitCountPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(waitRaw) != "1" {
		t.Fatalf("wait calls=%q", waitRaw)
	}
}

func TestAwaitStateChangeRequiresASequenceAdvance(t *testing.T) {
	calls := 0
	err := awaitStateChange(context.Background(), 10, func(context.Context) (agentState, error) {
		calls++
		if calls == 1 {
			return agentState{Status: "idle", Sequence: 10}, nil
		}
		return agentState{Status: "working", Sequence: 11}, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestAwaitStateChangeRejectsAnUnchangedIdlePane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := awaitStateChange(ctx, 10, func(context.Context) (agentState, error) {
		return agentState{Status: "idle", Sequence: 10}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestAwaitStateChangeRejectsASequenceRegression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := awaitStateChange(ctx, 10, func(context.Context) (agentState, error) {
		return agentState{Status: "idle", Sequence: 9}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}
