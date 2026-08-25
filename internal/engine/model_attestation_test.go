package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/terry-li-hm/herdr-bots/internal/adapter"
	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

// attestedModel is the full model name an attested job must configure; aliases
// cannot identify one model and are rejected by config validation.
const attestedModel = "claude-opus-5"

// attestedClaudeEvaluations are the two clock readings the nearby Claude tests
// use: one before the 09:00 occurrence and one after it.
var attestedClaudeEvaluations = []string{"2026-08-22T08:59:00+08:00", "2026-08-22T09:00:30+08:00"}

// writeAttestedClaudeJob rewrites the generated Pi definition into a
// claude-code job that requires a model attestation receipt. The file keeps the
// 0600 mode config.Load demands of executable authority.
func writeAttestedClaudeJob(t *testing.T, configPath string) {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw),
		"harness: pi\n      provider: openai-codex\n      model: gpt-5.6-sol",
		"harness: claude-code\n      model: "+attestedModel+"\n      require_model_attestation: true", 1)
	if err := os.WriteFile(configPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// attestedTranscript is a realistic finished Claude pane: a one-line
// machine-readable result followed by the completion status. The {{MARKER}}
// placeholder is rewritten by fakeHerdr with the marker the engine launched,
// because the run id is not known until the run exists.
func attestedTranscript(usageModel string) string {
	result := `{"type":"result","subtype":"success","is_error":false,"result":"done","modelUsage":{"` +
		usageModel + `":{"canonicalModel":"` + usageModel +
		`","provider":"firstParty","inputTokens":1200,"outputTokens":340}}}`
	return "Reviewing the repository for documentation drift.\n" + result + "\n{{MARKER}}:0\n"
}

func evaluateAttestedClaudeJob(t *testing.T, eng *Engine) {
	t.Helper()
	ctx := context.Background()
	for _, stamp := range attestedClaudeEvaluations {
		if err := eng.Evaluate(ctx, mustTime(stamp)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAttestedClaudeRunPersistsItsModelReceipt(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	writeAttestedClaudeJob(t, eng.ConfigPath)
	client.commandTranscript = attestedTranscript(attestedModel)

	evaluateAttestedClaudeJob(t, eng)

	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateSucceeded || run.TaskVerdict != "passed" {
		t.Fatalf("run=%+v", run)
	}
	if run.ModelAttestation == "" || !strings.Contains(run.ModelAttestation, attestedModel) {
		t.Fatalf("attestation=%q want a durable receipt naming %s", run.ModelAttestation, attestedModel)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.commandTranscriptCalls != 1 {
		t.Fatalf("transcript reads=%d want exactly one", client.commandTranscriptCalls)
	}
}

func TestUnattestableClaudeTranscriptFailsClosed(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		transcript string
	}{
		{name: "model mismatch", transcript: attestedTranscript("claude-sonnet-5")},
		{name: "malformed result", transcript: "Reviewing.\n" + `{"type":"result","modelUsage":{` + "\n{{MARKER}}:0\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			eng, state, client := newTestEngine(t, true, "")
			writeAttestedClaudeJob(t, eng.ConfigPath)
			client.commandTranscript = testCase.transcript

			evaluateAttestedClaudeJob(t, eng)

			run := waitForTerminal(t, state, "docs-drift")
			if run.State != store.StateFailed || run.ErrorCode != "model_attestation_failed" || run.TaskVerdict != "unverified" {
				t.Fatalf("run=%+v", run)
			}
			if run.ModelAttestation != "" {
				t.Fatalf("unattestable run persisted a receipt: %q", run.ModelAttestation)
			}
			client.mu.Lock()
			defer client.mu.Unlock()
			if client.commandTranscriptCalls != 1 {
				t.Fatalf("transcript reads=%d want exactly one", client.commandTranscriptCalls)
			}
		})
	}
}

func TestUnreadableClaudeTranscriptFailsClosed(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	writeAttestedClaudeJob(t, eng.ConfigPath)
	client.commandTranscriptErr = errors.New("pane read failed")

	evaluateAttestedClaudeJob(t, eng)

	run := waitForTerminal(t, state, "docs-drift")
	if run.State != store.StateFailed || run.ErrorCode != "model_attestation_failed" || run.TaskVerdict != "unverified" {
		t.Fatalf("run=%+v", run)
	}
	if run.ModelAttestation != "" {
		t.Fatalf("unreadable transcript persisted a receipt: %q", run.ModelAttestation)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.commandTranscriptCalls != 1 {
		t.Fatalf("transcript reads=%d want exactly one", client.commandTranscriptCalls)
	}
}

// TestPersistedModelAttestationIsNotReread proves the durable receipt, not the
// pane, is the source of truth: a completion that reruns after the receipt was
// already saved must not read the transcript a second time.
func TestPersistedModelAttestationIsNotReread(t *testing.T) {
	eng, state, client := newTestEngine(t, true, "")
	writeAttestedClaudeJob(t, eng.ConfigPath)
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
	accepted, err := state.CreateManualRun(ctx, cfg.Jobs[0].ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	const marker = "HERDR_BOTS_RUN_REREAD"
	if err := state.Transition(ctx, accepted.ID, store.StateAccepted, store.StateProvisioning, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := state.SetReceipt(ctx, accepted.ID, "w-attested", "p-attested", "auto/test", client.path, adapter.ModeCommand, marker); err != nil {
		t.Fatal(err)
	}
	for _, transition := range []struct{ from, to string }{
		{store.StateProvisioning, store.StateStarting},
		{store.StateStarting, store.StateRunning},
	} {
		if err := state.Transition(ctx, accepted.ID, transition.from, transition.to, "test", now); err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := adapter.ParseClaudeModelAttestation(
		strings.ReplaceAll(attestedTranscript(attestedModel), "{{MARKER}}", marker), marker, attestedModel)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetModelAttestation(ctx, accepted.ID, receipt); err != nil {
		t.Fatal(err)
	}
	running, err := state.GetRun(ctx, accepted.ID)
	if err != nil {
		t.Fatal(err)
	}

	eng.completeCommand(ctx, running, 0)

	finished := waitForRunTerminal(t, state, accepted.ID)
	if finished.State != store.StateSucceeded || finished.TaskVerdict != "passed" {
		t.Fatalf("run=%+v", finished)
	}
	if finished.ModelAttestation != receipt {
		t.Fatalf("attestation=%q want the already-persisted receipt %q", finished.ModelAttestation, receipt)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.commandTranscriptCalls != 0 {
		t.Fatalf("persisted receipt was re-derived from %d transcript reads", client.commandTranscriptCalls)
	}
}
