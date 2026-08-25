package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/terry-li-hm/herdr-bots/internal/config"
)

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
	LookPath(name string) (string, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
func (ExecRunner) LookPath(name string) (string, error) { return exec.LookPath(name) }

const (
	ModeAgent   = "agent"
	ModeCommand = "command"
)

type Launch struct {
	Mode string
	Kind string
	Args []string
}

func Probe(ctx context.Context, runner CommandRunner, job config.Job) error {
	switch job.Execution.Harness {
	case config.HarnessPi:
		if _, err := runner.LookPath("pi"); err != nil {
			return fmt.Errorf("route_unavailable: pi is not on PATH")
		}
		raw, err := runner.Run(ctx, "pi", "auth", "check", "--provider", job.Execution.Provider, "--json", "--no-refresh")
		if err != nil {
			return fmt.Errorf("route_unavailable: %w", err)
		}
		var auth struct {
			Status   string `json:"status"`
			Provider string `json:"provider"`
		}
		if json.Unmarshal(raw, &auth) != nil || auth.Status != "ready" || auth.Provider != job.Execution.Provider {
			return fmt.Errorf("route_unavailable: pi provider %q is not ready", job.Execution.Provider)
		}
		if job.Execution.Model != "harness-default" {
			models, err := runner.Run(ctx, "pi", "--list-models", job.Execution.Provider)
			if err != nil {
				return fmt.Errorf("route_unavailable: cannot inspect pi models: %w", err)
			}
			if !piModelPresent(string(models), job.Execution.Provider, job.Execution.Model) {
				return fmt.Errorf("route_unavailable: pi model %s/%s is not observed", job.Execution.Provider, job.Execution.Model)
			}
		}
	case config.HarnessClaudeCode:
		if _, err := runner.LookPath("claude"); err != nil {
			return fmt.Errorf("route_unavailable: claude is not on PATH")
		}
		raw, err := runner.Run(ctx, "claude", "auth", "status")
		if err != nil {
			return fmt.Errorf("route_unavailable: %w", err)
		}
		var auth struct {
			LoggedIn   bool   `json:"loggedIn"`
			AuthMethod string `json:"authMethod"`
		}
		if json.Unmarshal(raw, &auth) != nil || !auth.LoggedIn || auth.AuthMethod != "claude.ai" {
			return fmt.Errorf("route_unavailable: claude.ai subscription auth is not ready")
		}
	default:
		return fmt.Errorf("route_unavailable: unsupported harness %q", job.Execution.Harness)
	}
	return nil
}

func LaunchFor(job config.Job) (Launch, error) {
	profile := job.Execution.PermissionProfile
	switch job.Execution.Harness {
	case config.HarnessPi:
		args := []string{
			"--provider", job.Execution.Provider,
			"--no-approve", "--no-extensions", "--no-skills",
			"--no-prompt-templates", "--no-context-files",
		}
		if job.Execution.Model != "harness-default" {
			args = append(args, "--model", job.Execution.Model)
		}
		args = append(args, "--thinking", job.Execution.Thinking)
		switch profile {
		case config.PermissionReadOnly:
			args = append(args, "--tools", "read,grep,find,ls")
		case config.PermissionRepoWrite:
			args = append(args, "--tools", "read,grep,find,ls,edit,write")
		default:
			return Launch{}, fmt.Errorf("unsupported permission profile %q", profile)
		}
		// Scheduled Pi jobs run as headless commands in the existing Herdr
		// workspace: the engine's command line already supplies `-p` for every
		// command-mode launch, so the headless Pi command exits and leaves no
		// live Pi process contributing to the process-based attended-session
		// count. Args are unchanged, so the provider, model,
		// thinking, tool, and session boundaries stay exactly as before.
		return Launch{Mode: ModeCommand, Kind: "pi", Args: args}, nil
	case config.HarnessClaudeCode:
		args := []string{}
		if job.Execution.Model != "harness-default" {
			args = append(args, "--model", job.Execution.Model)
		}
		args = append(args, "--effort", job.Execution.Thinking, "--safe-mode", "--no-chrome", "--no-session-persistence", "--strict-mcp-config")
		// The attested launch contract requires a machine-readable result so a
		// later slice can bind the outcome to the configured model. Ordinary
		// Claude launches stay byte-for-byte unchanged.
		if job.Execution.RequiresModelAttestation() {
			args = append(args, "--output-format", "json")
		}
		switch profile {
		case config.PermissionReadOnly:
			args = append(args, "--permission-mode", "plan", "--tools", "Read,Glob,Grep")
		case config.PermissionRepoWrite:
			args = append(args, "--permission-mode", "acceptEdits", "--tools", "Read,Glob,Grep,Edit,Write")
		default:
			return Launch{}, fmt.Errorf("unsupported permission profile %q", profile)
		}
		return Launch{Mode: ModeCommand, Kind: "claude", Args: args}, nil
	default:
		return Launch{}, fmt.Errorf("unsupported harness %q", job.Execution.Harness)
	}
}

// ClaudeAttestationVersion is the schema version of the model attestation
// receipt. It is part of the durable receipt, so it changes only when the
// meaning of the receipt changes.
const ClaudeAttestationVersion = 1

// claudeFirstPartyProvider is the only provider that can satisfy the attested
// launch contract: a first-party Anthropic route is what the configured model
// name identifies.
const claudeFirstPartyProvider = "firstParty"

const attestationFailure = "model_attestation_unverified"

type claudeAttestation struct {
	Version       int    `json:"version"`
	ExpectedModel string `json:"expected_model"`
	ObservedModel string `json:"observed_model"`
	Provider      string `json:"provider"`
	ResultSHA256  string `json:"result_sha256"`
	Verdict       string `json:"verdict"`
}

type claudeModelUsage struct {
	CanonicalModel string `json:"canonicalModel"`
	Provider       string `json:"provider"`
	InputTokens    int64  `json:"inputTokens"`
	OutputTokens   int64  `json:"outputTokens"`
}

// ParseClaudeModelAttestation binds a finished Claude invocation to the model
// the job configured, using only evidence the harness itself printed. It is
// pure: the observed transcript, the run's completion marker, and the expected
// model are the whole input.
//
// Only lines before the first completion status for this marker are evidence,
// so a later pane write can never supply the result. The last line that
// independently parses as a Claude single-result object is the one the marker
// reported on; absent, malformed, mismatched, or unused-model evidence is an
// error rather than a weaker verdict. The proof is the exact expected usage
// key plus a canonical model that matches it; additional usage entries stay
// allowed because real Claude results report internal auxiliary model usage.
// The returned receipt is deterministic
// compact JSON, so the same transcript always yields the same durable bytes.
func ParseClaudeModelAttestation(transcript, completionMarker, expectedModel string) (string, error) {
	if completionMarker == "" {
		return "", fmt.Errorf("%s: a completion marker is required", attestationFailure)
	}
	if expectedModel == "" {
		return "", fmt.Errorf("%s: an expected model is required", attestationFailure)
	}
	lines := strings.Split(transcript, "\n")
	boundary := -1
	for i, line := range lines {
		if hasCompletionStatus(line, completionMarker) {
			boundary = i
			break
		}
	}
	if boundary < 0 {
		return "", fmt.Errorf("%s: no %s completion status is present in the transcript", attestationFailure, completionMarker)
	}
	line, usage, found := lastClaudeResultLine(lines[:boundary])
	if !found {
		return "", fmt.Errorf("%s: no Claude result JSON precedes the %s completion status", attestationFailure, completionMarker)
	}
	rawUsage, ok := usage[expectedModel]
	if !ok {
		return "", fmt.Errorf("%s: the result reports usage for %s, not the configured model %q", attestationFailure, observedModels(usage), expectedModel)
	}
	var entry claudeModelUsage
	if err := json.Unmarshal(rawUsage, &entry); err != nil {
		return "", fmt.Errorf("%s: usage for %q is malformed: %w", attestationFailure, expectedModel, err)
	}
	if entry.Provider != claudeFirstPartyProvider {
		return "", fmt.Errorf("%s: usage for %q reports provider %q, want %q", attestationFailure, expectedModel, entry.Provider, claudeFirstPartyProvider)
	}
	if entry.CanonicalModel != expectedModel {
		return "", fmt.Errorf("%s: the result reports canonical model %q, want %q", attestationFailure, entry.CanonicalModel, expectedModel)
	}
	if entry.InputTokens <= 0 && entry.OutputTokens <= 0 {
		return "", fmt.Errorf("%s: usage for %q reports no input or output tokens", attestationFailure, expectedModel)
	}
	sum := sha256.Sum256([]byte(line))
	receipt, err := json.Marshal(claudeAttestation{
		Version:       ClaudeAttestationVersion,
		ExpectedModel: expectedModel,
		ObservedModel: entry.CanonicalModel,
		Provider:      entry.Provider,
		ResultSHA256:  hex.EncodeToString(sum[:]),
		Verdict:       "attested",
	})
	if err != nil {
		return "", fmt.Errorf("%s: encode receipt: %w", attestationFailure, err)
	}
	return string(receipt), nil
}

// hasCompletionStatus reports whether a line carries this marker's exit status,
// which is the same `marker:<code>` evidence the command waiter observes.
func hasCompletionStatus(line, marker string) bool {
	rest := line
	for {
		at := strings.Index(rest, marker+":")
		if at < 0 {
			return false
		}
		rest = rest[at+len(marker)+1:]
		if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
			return true
		}
	}
}

// lastClaudeResultLine returns the exact untrimmed line, and its model usage,
// for the last line that on its own is a Claude single-result JSON object.
// Partial or wrapped output never qualifies, because each candidate line must
// parse completely by itself.
func lastClaudeResultLine(lines []string) (string, map[string]json.RawMessage, bool) {
	for i := len(lines) - 1; i >= 0; i-- {
		var object map[string]json.RawMessage
		if json.Unmarshal([]byte(lines[i]), &object) != nil {
			continue
		}
		var kind string
		if json.Unmarshal(object["type"], &kind) != nil || kind != "result" {
			continue
		}
		var usage map[string]json.RawMessage
		if json.Unmarshal(object["modelUsage"], &usage) != nil || usage == nil {
			continue
		}
		return lines[i], usage, true
	}
	return "", nil, false
}

func observedModels(usage map[string]json.RawMessage) string {
	if len(usage) == 0 {
		return "no model"
	}
	names := make([]string, 0, len(usage))
	for name := range usage {
		names = append(names, strconv.Quote(name))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func piModelPresent(table, provider, model string) bool {
	for _, line := range strings.Split(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == provider && fields[1] == model {
			return true
		}
	}
	return false
}
