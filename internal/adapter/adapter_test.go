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
}

func (f fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	key := name
	if len(args) > 0 {
		key += " " + args[0]
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
		"pi auth":          []byte(`{"status":"ready","provider":"openai-codex"}`),
		"pi --list-models": []byte("provider model context\nopenai-codex gpt-5.6-sol 272K\n"),
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

func TestPiReadOnlyLaunchHasNoShellOrWriteTools(t *testing.T) {
	got, err := LaunchFor(piJob())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--provider", "openai-codex", "--no-approve", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--model", "gpt-5.6-sol", "--thinking", "high", "--tools", "read,grep,find,ls"}
	if got.Mode != ModeAgent || got.Kind != "pi" || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("got %+v want %v", got, want)
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
	for _, required := range []string{"--safe-mode", "--strict-mcp-config", "acceptEdits", "Read,Glob,Grep,Edit,Write"} {
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
	want := []string{"--model", "claude-opus-5", "--effort", "high", "--safe-mode", "--no-chrome", "--no-session-persistence", "--strict-mcp-config", "--permission-mode", "plan", "--tools", "Read,Glob,Grep"}
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
	want := []string{"--model", "claude-opus-5", "--effort", "high", "--safe-mode", "--no-chrome", "--no-session-persistence", "--strict-mcp-config", "--output-format", "json", "--permission-mode", "plan", "--tools", "Read,Glob,Grep"}
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
	if got.Mode != ModeAgent || got.Kind != "pi" || !reflect.DeepEqual(got.Args, want) {
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
