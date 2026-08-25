package engine

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	// brIntegrationModel is the attested model used by the bounded review
	// integration tests.
	brIntegrationModel = "provider model\nopenai-codex gpt-5.6-sol\n"

	// brStagedRelPath is the destination the tests stage their input into,
	// relative to the worktree root.
	brStagedRelPath = ".herdr-bots/inputs/digest.md"

	// brPermissionMarker is the exact permission profile emitted by the test
	// config before it is opted into bounded review.
	brPermissionMarker = "permission_profile: read-only-no-network"

	// brVerifierMarker is the exact verifier command emitted by the test config.
	brVerifierMarker = `["/usr/bin/true"]`

	// The two distinct contents a single allowed path holds across one run: what
	// the agent left behind, and what a verifier rewrote over it afterwards.
	brAgentOutput    = "agent draft\n"
	brVerifierOutput = "verifier rewrite\n"
)

// brReadConfig returns the current on-disk config document.
func brReadConfig(t *testing.T, configPath string) string {
	t.Helper()
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config %s: %v", configPath, err)
	}
	return string(data)
}

// brWriteConfig rewrites the config document, preserving the 0600 mode the
// engine expects for its own config file.
func brWriteConfig(t *testing.T, configPath, contents string) {
	t.Helper()
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config %s: %v", configPath, err)
	}
}

// brJobID extracts the first job identifier declared by the config so the
// tests do not have to hard-code a value that lives in the fixture.
func brJobID(t *testing.T, config string) string {
	t.Helper()
	for _, line := range strings.Split(config, "\n") {
		trimmed := strings.TrimPrefix(strings.TrimSpace(line), "- ")
		if !strings.HasPrefix(trimmed, "id:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(trimmed, "id:"))
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		if value != "" {
			return value
		}
	}
	t.Fatalf("no job id found in config:\n%s", config)
	return ""
}

// brEnableBoundedReview swaps the read-only profile for repo-write and adds
// the bounded review input mapping plus the allowed write scope. The
// replacement reuses the indentation of the permission line so the injected
// keys land as siblings of permission_profile.
func brEnableBoundedReview(t *testing.T, configPath, source string, allowedWritePaths []string) {
	t.Helper()

	config := brReadConfig(t, configPath)
	idx := strings.Index(config, brPermissionMarker)
	if idx < 0 {
		t.Fatalf("config does not contain %q:\n%s", brPermissionMarker, config)
	}
	indent := config[strings.LastIndex(config[:idx], "\n")+1 : idx]

	var b strings.Builder
	b.WriteString("permission_profile: repo-write-no-network\n")
	b.WriteString(indent + "inputs:\n")
	b.WriteString(indent + "  - source: " + strconv.Quote(source) + "\n")
	b.WriteString(indent + "    destination: " + strconv.Quote(brStagedRelPath) + "\n")
	b.WriteString(indent + "allowed_write_paths:")
	for _, path := range allowedWritePaths {
		b.WriteString("\n" + indent + "  - " + strconv.Quote(path))
	}

	brWriteConfig(t, configPath, strings.Replace(config, brPermissionMarker, b.String(), 1))
}

// brReplaceVerifier points the job's verifier at the supplied command.
func brReplaceVerifier(t *testing.T, configPath, command string) {
	t.Helper()

	config := brReadConfig(t, configPath)
	if !strings.Contains(config, brVerifierMarker) {
		t.Fatalf("config does not contain verifier %q:\n%s", brVerifierMarker, config)
	}
	replacement := "[" + strconv.Quote(command) + "]"
	brWriteConfig(t, configPath, strings.Replace(config, brVerifierMarker, replacement, 1))
}

// brSentinelVerifier writes an executable that touches sentinel when run, so a
// test can assert whether the verifier was ever reached.
func brSentinelVerifier(t *testing.T, sentinel string) string {
	t.Helper()

	script := filepath.Join(t.TempDir(), "verifier.sh")
	body := fmt.Sprintf("#!/bin/sh\nprintf ran > %q\nexit 0\n", sentinel)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write verifier script: %v", err)
	}
	return script
}

// brScriptVerifier writes an executable that runs the supplied shell lines and
// then exits 0, so a failing run can only be failing because of what the
// verifier did to the worktree rather than what it returned. Every line names
// its target by absolute path, because the verifier runs with the worktree as
// its working directory and a relative path would hide which tree was touched.
func brScriptVerifier(t *testing.T, lines ...string) string {
	t.Helper()

	script := filepath.Join(t.TempDir(), "verifier.sh")
	body := "#!/bin/sh\n" + strings.Join(lines, "\n") + "\nexit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write verifier script: %v", err)
	}
	return script
}

// brWriteLine is one shell line writing exact bytes to an exact absolute path.
func brWriteLine(path, content string) string {
	return fmt.Sprintf("printf %q > %q", content, path)
}

// brWriteSource writes the review input that lives outside the worktree.
func brWriteSource(t *testing.T, name string) string {
	t.Helper()

	source := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(source, []byte("# Digest\n\nupstream findings\n"), 0o600); err != nil {
		t.Fatalf("write source %s: %v", source, err)
	}
	return source
}

// brEvaluateWindow drives the engine across the tick before the schedule and
// the tick that fires it.
func brEvaluateWindow(t *testing.T, eng *Engine) {
	t.Helper()

	ctx := context.Background()
	for _, at := range []string{"2026-08-25T08:59:00+08:00", "2026-08-25T09:00:30+08:00"} {
		if err := eng.Evaluate(ctx, mustTime(at)); err != nil {
			t.Fatalf("evaluate at %s: %v", at, err)
		}
	}
}

func brStagedPath(worktree string) string {
	return filepath.Join(worktree, filepath.FromSlash(brStagedRelPath))
}

func TestBoundedReviewIntegrationSucceedsWithinScope(t *testing.T) {
	eng, state, client := newTestEngine(t, true, brIntegrationModel)
	initGitRepo(t, client.path)

	source := brWriteSource(t, "digest.md")
	brEnableBoundedReview(t, eng.ConfigPath, source, []string{"notes/"})
	jobID := brJobID(t, brReadConfig(t, eng.ConfigPath))

	brEvaluateWindow(t, eng)
	run := waitForTerminal(t, state, jobID)

	if run.State != "succeeded" {
		t.Fatalf("state = %v, want succeeded (error_code=%v)", run.State, run.ErrorCode)
	}
	if run.TaskVerdict != "passed" {
		t.Fatalf("task verdict = %v, want passed", run.TaskVerdict)
	}
	if run.InputReceipt == "" {
		t.Error("input receipt is empty")
	}
	if run.ChangeReceipt == "" {
		t.Error("change receipt is empty")
	}

	// A successful run deliberately retains the staged input as evidence, so it
	// must still be there and still be byte-for-byte the source.
	staged, err := os.ReadFile(brStagedPath(client.path))
	if err != nil {
		t.Fatalf("staged input %s not retained after success: %v", brStagedRelPath, err)
	}
	original, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read source %s: %v", source, err)
	}
	if !bytes.Equal(staged, original) {
		t.Errorf("staged input = %q, want the source bytes %q", staged, original)
	}

	if !strings.Contains(client.prompt, brStagedRelPath) {
		t.Errorf("prompt does not mention destination %q:\n%s", brStagedRelPath, client.prompt)
	}
	if !strings.Contains(client.prompt, "notes/") {
		t.Errorf("prompt does not mention write scope %q:\n%s", "notes/", client.prompt)
	}
	if strings.Contains(client.prompt, source) {
		t.Errorf("prompt leaks source path %q:\n%s", source, client.prompt)
	}
}

func TestBoundedReviewIntegrationFailsOnOutOfScopeWrite(t *testing.T) {
	eng, state, client := newTestEngine(t, true, brIntegrationModel)
	initGitRepo(t, client.path)

	source := brWriteSource(t, "digest.md")
	brEnableBoundedReview(t, eng.ConfigPath, source, []string{"notes/"})

	sentinel := filepath.Join(t.TempDir(), "verifier-ran")
	brReplaceVerifier(t, eng.ConfigPath, brSentinelVerifier(t, sentinel))
	jobID := brJobID(t, brReadConfig(t, eng.ConfigPath))

	client.submitMutation = func() {
		if err := os.WriteFile(filepath.Join(client.path, "outside.md"), []byte("out of scope\n"), 0o600); err != nil {
			t.Errorf("write out-of-scope file: %v", err)
		}
	}

	brEvaluateWindow(t, eng)
	run := waitForTerminal(t, state, jobID)

	if run.State != "failed" {
		t.Fatalf("state = %v, want failed", run.State)
	}
	if run.ErrorCode != "bounded_review_failed" {
		t.Errorf("error code = %v, want bounded_review_failed", run.ErrorCode)
	}
	if run.TaskVerdict != "unverified" {
		t.Errorf("task verdict = %v, want unverified", run.TaskVerdict)
	}
	if run.ChangeReceipt != "" {
		t.Errorf("change receipt = %q, want empty for a rejected change", run.ChangeReceipt)
	}

	if _, err := os.Stat(brStagedPath(client.path)); err != nil {
		t.Errorf("staged input %s not retained for inspection: %v", brStagedRelPath, err)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Errorf("verifier ran despite out-of-scope write (stat err = %v)", err)
	}
}

// A verifier that rewrites a file the agent already wrote changes none of the
// paths git names, only the bytes behind one of them. Nothing but the content
// fingerprint can see that, so this is the case that proves the run is observed
// again after the verifier and not only before it.
func TestBoundedReviewIntegrationFailsWhenVerifierRewritesAllowedFile(t *testing.T) {
	eng, state, client := newTestEngine(t, true, brIntegrationModel)
	initGitRepo(t, client.path)

	source := brWriteSource(t, "digest.md")
	brEnableBoundedReview(t, eng.ConfigPath, source, []string{"notes/"})

	output := filepath.Join(client.path, "notes", "output.md")
	sentinel := filepath.Join(t.TempDir(), "verifier-ran")
	brReplaceVerifier(t, eng.ConfigPath, brScriptVerifier(t,
		brWriteLine(output, brVerifierOutput),
		brWriteLine(sentinel, "ran"),
	))
	jobID := brJobID(t, brReadConfig(t, eng.ConfigPath))

	// The agent stays inside the declared scope, so the first observation
	// succeeds and the run is entitled to reach its verifier.
	client.submitMutation = func() {
		if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
			t.Errorf("create allowed directory: %v", err)
			return
		}
		if err := os.WriteFile(output, []byte(brAgentOutput), 0o600); err != nil {
			t.Errorf("write allowed file: %v", err)
		}
	}

	brEvaluateWindow(t, eng)
	run := waitForTerminal(t, state, jobID)

	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("verifier did not run despite an in-scope change: %v", err)
	}
	if run.State != "failed" {
		t.Fatalf("state = %v, want failed", run.State)
	}
	if run.ErrorCode != "bounded_review_failed" {
		t.Errorf("error code = %v, want bounded_review_failed", run.ErrorCode)
	}
	if run.TaskVerdict != "unverified" {
		t.Errorf("task verdict = %v, want unverified", run.TaskVerdict)
	}
	// The first observation is durable evidence of what the agent left behind,
	// and the drift the second one found never replaces it.
	if run.ChangeReceipt == "" {
		t.Error("change receipt is empty, want the pre-verifier observation retained")
	}

	written, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read %s: %v", output, err)
	}
	if string(written) != brVerifierOutput {
		t.Errorf("%s = %q, want the verifier bytes %q", output, written, brVerifierOutput)
	}
}

// A verifier that adds a path outside the declared scope is a new change rather
// than a rewrite, so the re-observation refuses it at the scope check before any
// receipt is recomputed. The run fails the same way either route is taken.
func TestBoundedReviewIntegrationFailsWhenVerifierWritesOutOfScope(t *testing.T) {
	eng, state, client := newTestEngine(t, true, brIntegrationModel)
	initGitRepo(t, client.path)

	source := brWriteSource(t, "digest.md")
	brEnableBoundedReview(t, eng.ConfigPath, source, []string{"notes/"})

	output := filepath.Join(client.path, "notes", "output.md")
	outside := filepath.Join(client.path, "outside-after-verifier.md")
	sentinel := filepath.Join(t.TempDir(), "verifier-ran")
	brReplaceVerifier(t, eng.ConfigPath, brScriptVerifier(t,
		brWriteLine(outside, "out of scope\n"),
		brWriteLine(sentinel, "ran"),
	))
	jobID := brJobID(t, brReadConfig(t, eng.ConfigPath))

	client.submitMutation = func() {
		if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
			t.Errorf("create allowed directory: %v", err)
			return
		}
		if err := os.WriteFile(output, []byte(brAgentOutput), 0o600); err != nil {
			t.Errorf("write allowed file: %v", err)
		}
	}

	brEvaluateWindow(t, eng)
	run := waitForTerminal(t, state, jobID)

	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("verifier did not run despite an in-scope change: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("verifier's out-of-scope file is not on disk to be found: %v", err)
	}
	if run.State != "failed" {
		t.Fatalf("state = %v, want failed", run.State)
	}
	if run.ErrorCode != "bounded_review_failed" {
		t.Errorf("error code = %v, want bounded_review_failed", run.ErrorCode)
	}
	if run.TaskVerdict != "unverified" {
		t.Errorf("task verdict = %v, want unverified", run.TaskVerdict)
	}
}

func TestBoundedReviewIntegrationFailsOnMissingSource(t *testing.T) {
	eng, state, client := newTestEngine(t, true, brIntegrationModel)
	initGitRepo(t, client.path)

	missing := filepath.Join(t.TempDir(), "absent", "digest.md")
	brEnableBoundedReview(t, eng.ConfigPath, missing, []string{"notes/"})
	jobID := brJobID(t, brReadConfig(t, eng.ConfigPath))

	brEvaluateWindow(t, eng)
	run := waitForTerminal(t, state, jobID)

	if run.State != "failed" {
		t.Fatalf("state = %v, want failed", run.State)
	}
	if run.ErrorCode != "input_staging_failed" {
		t.Errorf("error code = %v, want input_staging_failed", run.ErrorCode)
	}
	if client.startAgentCount != 0 {
		t.Errorf("start agent count = %d, want 0", client.startAgentCount)
	}
	if client.submitCount != 0 {
		t.Errorf("submit count = %d, want 0", client.submitCount)
	}
	if run.InputReceipt != "" {
		t.Errorf("input receipt = %q, want empty", run.InputReceipt)
	}
	if run.ChangeReceipt != "" {
		t.Errorf("change receipt = %q, want empty", run.ChangeReceipt)
	}
}
