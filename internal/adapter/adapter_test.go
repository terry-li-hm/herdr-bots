package adapter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/terry-li-hm/herdr-bots/internal/config"
)

type fakeRunner struct {
	outputs map[string][]byte
	missing map[string]bool
	fail    map[string]bool
	calls   *[][]string
}

func (f fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	key := name
	if len(args) > 0 {
		key += " " + args[0]
	}
	if f.fail[key] {
		return nil, errors.New("command failed: " + key)
	}
	if f.calls != nil {
		*f.calls = append(*f.calls, append([]string{name}, args...))
	}
	out, ok := f.outputs[key]
	if !ok {
		return nil, errors.New("unexpected command: " + key)
	}
	return out, nil
}
func (f fakeRunner) LookPath(name string) (string, error) {
	if f.missing[name] {
		return "", errors.New("missing")
	}
	return "/bin/" + name, nil
}

func piJob() config.Job {
	return config.Job{Execution: config.Execution{Harness: config.HarnessPi, Provider: "openai-codex", Model: "gpt-5.6-sol", Thinking: "high", PermissionProfile: config.PermissionReadOnly}}
}

func TestProbePiRequiresObservedExactRoute(t *testing.T) {
	f := fakeRunner{outputs: map[string][]byte{
		"pi auth":            []byte(`{"status":"ready","provider":"openai-codex"}`),
		"pi --no-extensions": []byte("provider model context\nopenai-codex gpt-5.6-sol 272K\n"),
	}}
	if err := Probe(context.Background(), f, piJob()); err != nil {
		t.Fatal(err)
	}
	job := piJob()
	job.Execution.Model = "missing"
	if err := Probe(context.Background(), f, job); err == nil {
		t.Fatal("missing model should fail closed")
	}
}

func TestProbePiModelListUsesRestrictedArgvInOrder(t *testing.T) {
	var calls [][]string
	f := fakeRunner{outputs: map[string][]byte{
		"pi auth":            []byte(`{"status":"ready","provider":"openai-codex"}`),
		"pi --no-extensions": []byte("provider model context\nopenai-codex gpt-5.6-sol 272K\n"),
	}, calls: &calls}
	if err := Probe(context.Background(), f, piJob()); err != nil {
		t.Fatal(err)
	}
	want := []string{"pi", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--list-models", "openai-codex"}
	if len(calls) != 2 || !reflect.DeepEqual(calls[1], want) {
		t.Fatalf("model-list argv = %v, want %v", calls, want)
	}
	// The auth check must keep --no-refresh and precede the model query.
	authWant := []string{"pi", "auth", "check", "--provider", "openai-codex", "--json", "--no-refresh"}
	if !reflect.DeepEqual(calls[0], authWant) {
		t.Fatalf("auth argv = %v, want %v", calls[0], authWant)
	}
}

func TestProbePiSkipsModelListForHarnessDefault(t *testing.T) {
	var calls [][]string
	f := fakeRunner{outputs: map[string][]byte{
		"pi auth": []byte(`{"status":"ready","provider":"openai-codex"}`),
	}, calls: &calls}
	job := piJob()
	job.Execution.Model = "harness-default"
	if err := Probe(context.Background(), f, job); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("model list must be skipped for harness-default: %v", calls)
	}
}

func TestProbePiRefusesBadAuthWithoutListing(t *testing.T) {
	for _, out := range []string{
		`{"status":"expired","provider":"openai-codex"}`,
		`{"status":"ready","provider":"other"}`,
		`not json`,
	} {
		var calls [][]string
		f := fakeRunner{outputs: map[string][]byte{
			"pi auth": []byte(out),
		}, calls: &calls}
		if err := Probe(context.Background(), f, piJob()); err == nil {
			t.Fatalf("auth output %q should fail closed", out)
		}
		if len(calls) != 1 {
			t.Fatalf("no model list after bad auth %q: %v", out, calls)
		}
	}
}

func TestProbePiModelListFailureFailsClosed(t *testing.T) {
	f := fakeRunner{outputs: map[string][]byte{
		"pi auth": []byte(`{"status":"ready","provider":"openai-codex"}`),
	}, fail: map[string]bool{"pi --no-extensions": true}}
	err := Probe(context.Background(), f, piJob())
	if err == nil || !contains(err.Error(), "cannot inspect pi models") {
		t.Fatalf("model-list failure should fail closed, got %v", err)
	}
}

func TestPiReadOnlyLaunchIsHeadlessCommandWithoutShellOrWriteTools(t *testing.T) {
	got, err := LaunchFor(piJob())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--provider", "openai-codex", "--no-approve", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--model", "gpt-5.6-sol", "--thinking", "high", "--tools", "read,grep,find,ls"}
	// A scheduled Pi job runs as a headless command in its Herdr workspace, so
	// the command exits and leaves no live Pi process counted as an attended
	// session. The
	// engine's command line supplies `-p`; the adapter must not duplicate it.
	if got.Mode != ModeCommand || got.Kind != "pi" || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("got %+v want mode=%q kind=pi args=%v", got, ModeCommand, want)
	}
	for _, arg := range got.Args {
		if arg == "-p" || arg == "--no-session" {
			t.Fatalf("adapter must not add %q: %v", arg, got.Args)
		}
	}
}

func TestClaudeRepoWriteStillDeniesShellAndWeb(t *testing.T) {
	job := config.Job{Execution: config.Execution{Harness: config.HarnessClaudeCode, Model: "opus", Thinking: "high", PermissionProfile: config.PermissionRepoWrite}}
	got, err := LaunchFor(job)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != ModeCommand {
		t.Fatalf("claude launch mode = %q, want command", got.Mode)
	}
	joined := ""
	for _, arg := range got.Args {
		joined += " " + arg
	}
	for _, required := range []string{"--safe-mode", "--restricted", "--strict-mcp-config", "acceptEdits", "Read,Glob,Grep,Edit,Write"} {
		if !contains(joined, required) {
			t.Fatalf("%q missing from %q", required, joined)
		}
	}
	if contains(joined, "Bash") || contains(joined, "--allowedTools") {
		t.Fatalf("Claude boundary is not an availability allowlist: %q", joined)
	}
}
func TestClaudeLaunchArgvIsUnchangedWithoutAttestation(t *testing.T) {
	job := config.Job{Execution: config.Execution{Harness: config.HarnessClaudeCode, Model: "claude-opus-5", Thinking: "high", PermissionProfile: config.PermissionReadOnly}}
	got, err := LaunchFor(job)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--model", "claude-opus-5", "--effort", "high", "--safe-mode", "--restricted", "--no-chrome", "--no-session-persistence", "--strict-mcp-config", "--permission-mode", "plan", "--tools", "Read,Glob,Grep"}
	if got.Mode != ModeCommand || got.Kind != "claude" || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("got %+v want %v", got, want)
	}
}

func TestAttestedClaudeLaunchAddsJSONOutputFormat(t *testing.T) {
	flag := true
	job := config.Job{Execution: config.Execution{Harness: config.HarnessClaudeCode, Model: "claude-opus-5", Thinking: "high", PermissionProfile: config.PermissionReadOnly, RequireModelAttestation: &flag}}
	got, err := LaunchFor(job)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--model", "claude-opus-5", "--effort", "high", "--safe-mode", "--restricted", "--no-chrome", "--no-session-persistence", "--strict-mcp-config", "--output-format", "json", "--permission-mode", "plan", "--tools", "Read,Glob,Grep"}
	if got.Mode != ModeCommand || got.Kind != "claude" || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("got %+v want %v", got, want)
	}
}

func TestPiLaunchIsUnchangedEvenWithAttestationFlag(t *testing.T) {
	flag := true
	job := piJob()
	job.Execution.RequireModelAttestation = &flag
	got, err := LaunchFor(job)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--provider", "openai-codex", "--no-approve", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--model", "gpt-5.6-sol", "--thinking", "high", "--tools", "read,grep,find,ls"}
	if got.Mode != ModeCommand || got.Kind != "pi" || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("pi argv changed by attestation flag: got %+v want %v", got, want)
	}
}
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
