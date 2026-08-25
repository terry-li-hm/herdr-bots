package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
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
		return Launch{Mode: ModeAgent, Kind: "pi", Args: args}, nil
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

func piModelPresent(table, provider, model string) bool {
	for _, line := range strings.Split(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == provider && fields[1] == model {
			return true
		}
	}
	return false
}
