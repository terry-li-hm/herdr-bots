package herdr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Receipt struct {
	WorkspaceID string
	PaneID      string
	Branch      string
	Path        string
}

type Client interface {
	Provision(ctx context.Context, repo, workspace, baseRef, branch, label string) (Receipt, error)
	FindWorkspaceByBranch(ctx context.Context, repo, branch string) (Receipt, bool, error)
	WorkspaceExists(ctx context.Context, workspaceID string) (bool, error)
	StartAgent(ctx context.Context, name, kind, paneID string, args []string) error
	Submit(ctx context.Context, paneID, prompt string) error
	Wait(ctx context.Context, paneID string, timeout time.Duration) (string, error)
	Status(ctx context.Context, paneID string) (string, error)
	StartCommand(ctx context.Context, paneID, command string) error
	WaitCommand(ctx context.Context, paneID, marker string, timeout time.Duration) (int, error)
	CommandResult(ctx context.Context, paneID, marker string) (int, bool, error)
	CloseWorkspace(ctx context.Context, workspaceID string) error
}

type CLI struct{ Bin string }

func New() *CLI {
	bin := os.Getenv("HERDR_BIN_PATH")
	if bin == "" {
		bin = "herdr"
	}
	return &CLI{Bin: bin}
}

type apiError struct{ Code, Message string }

func (e *apiError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}
func hasCode(err error, code string) bool {
	var target *apiError
	return errors.As(err, &target) && target.Code == code
}

func (c *CLI) run(ctx context.Context, out any, args ...string) error {
	cmd := exec.CommandContext(ctx, c.Bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var envelope struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		if json.Unmarshal(stdout.Bytes(), &envelope) == nil && (envelope.Error.Code != "" || envelope.Error.Message != "") {
			return &apiError{Code: envelope.Error.Code, Message: envelope.Error.Message}
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(stdout.String())
		}
		if msg == "" {
			msg = err.Error()
		}
		return &apiError{Message: msg}
	}
	if out == nil {
		return nil
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		return fmt.Errorf("decode herdr response: %w", err)
	}
	raw := envelope.Result
	if len(raw) == 0 {
		raw = stdout.Bytes()
	}
	return json.Unmarshal(raw, out)
}

func (c *CLI) WorkspaceExists(ctx context.Context, workspaceID string) (bool, error) {
	var result struct {
		Workspace struct {
			ID string `json:"workspace_id"`
		} `json:"workspace"`
	}
	if err := c.run(ctx, &result, "workspace", "get", workspaceID); err != nil {
		if hasCode(err, "workspace_not_found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *CLI) FindWorkspaceByBranch(ctx context.Context, repo, branch string) (Receipt, bool, error) {
	var worktreeResult struct {
		Worktrees []struct {
			Branch string `json:"branch"`
			Path   string `json:"path"`
		} `json:"worktrees"`
	}
	if err := c.run(ctx, &worktreeResult, "worktree", "list", "--cwd", repo); err != nil {
		return Receipt{}, false, err
	}
	path := ""
	for _, worktree := range worktreeResult.Worktrees {
		if worktree.Branch == branch {
			path = worktree.Path
			break
		}
	}
	if path == "" {
		return Receipt{}, false, nil
	}
	var workspaceResult struct {
		Workspaces []struct {
			ID       string `json:"workspace_id"`
			Worktree struct {
				Path string `json:"checkout_path"`
			} `json:"worktree"`
		} `json:"workspaces"`
	}
	if err := c.run(ctx, &workspaceResult, "workspace", "list"); err != nil {
		return Receipt{}, false, err
	}
	for _, workspace := range workspaceResult.Workspaces {
		if workspace.ID != "" && workspace.Worktree.Path == path {
			return Receipt{WorkspaceID: workspace.ID, Branch: branch, Path: path}, true, nil
		}
	}
	return Receipt{Branch: branch, Path: path}, true, nil
}

func (c *CLI) Provision(ctx context.Context, repo, workspace, baseRef, branch, label string) (Receipt, error) {
	var result struct {
		Workspace struct {
			ID string `json:"workspace_id"`
		} `json:"workspace"`
		RootPane struct {
			ID string `json:"pane_id"`
		} `json:"root_pane"`
	}
	var err error
	if workspace == "worktree" {
		args := worktreeCreateArgs(repo, baseRef, branch, label)
		err = c.run(ctx, &result, args...)
	} else {
		err = c.run(ctx, &result, "workspace", "create", "--cwd", repo, "--label", label, "--no-focus")
	}
	if err != nil {
		return Receipt{}, err
	}
	if result.Workspace.ID == "" || result.RootPane.ID == "" {
		return Receipt{}, fmt.Errorf("herdr returned no workspace or pane id")
	}
	receipt := Receipt{WorkspaceID: result.Workspace.ID, PaneID: result.RootPane.ID, Branch: branch, Path: repo}
	if workspace == "worktree" {
		path, err := c.worktreePath(ctx, repo, branch)
		if err != nil {
			return receipt, err
		}
		receipt.Path = path
	}
	return receipt, nil
}

func worktreeCreateArgs(repo, baseRef, branch, label string) []string {
	args := []string{"worktree", "create", "--cwd", repo, "--branch", branch}
	if baseRef != "" {
		args = append(args, "--base", baseRef)
	}
	return append(args, "--label", label, "--no-focus")
}

func (c *CLI) worktreePath(ctx context.Context, repo, branch string) (string, error) {
	var result struct {
		Worktrees []struct{ Branch, Path string } `json:"worktrees"`
	}
	if err := c.run(ctx, &result, "worktree", "list", "--cwd", repo); err != nil {
		return "", err
	}
	for _, wt := range result.Worktrees {
		if wt.Branch == branch && wt.Path != "" {
			return wt.Path, nil
		}
	}
	return "", fmt.Errorf("created branch %q was not found in herdr worktree list", branch)
}

func (c *CLI) StartAgent(ctx context.Context, name, kind, paneID string, args []string) error {
	cmd := []string{"agent", "start", name, "--kind", kind, "--pane", paneID, "--timeout", "120000"}
	if len(args) > 0 {
		cmd = append(cmd, "--")
		cmd = append(cmd, args...)
	}
	return c.run(ctx, nil, cmd...)
}

func (c *CLI) Submit(ctx context.Context, paneID, prompt string) error {
	err := c.run(ctx, nil, "agent", "prompt", paneID, prompt, "--wait", "--until", "working", "--timeout", "30000")
	if err == nil {
		return nil
	}
	if !hasCode(err, "agent_prompt_stalled") {
		return err
	}
	before, statusErr := c.agentState(ctx, paneID)
	if statusErr != nil {
		return errors.Join(err, fmt.Errorf("inspect stalled prompt: %w", statusErr))
	}
	if before.Status == "working" || before.Status == "done" || before.Status == "blocked" {
		return nil
	}
	// Herdr can stage the prompt in Pi's composer before its first Enter is
	// accepted. Deliver one Enter, then require an observed pane state change;
	// an unchanged idle pane is never treated as a confirmed task start.
	if sendErr := c.run(ctx, nil, "agent", "send-keys", paneID, "enter"); sendErr != nil {
		return errors.Join(err, fmt.Errorf("submit staged prompt: %w", sendErr))
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if waitErr := awaitStateChange(waitCtx, before.Sequence, func(observeCtx context.Context) (agentState, error) {
		return c.agentState(observeCtx, paneID)
	}); waitErr != nil {
		return errors.Join(err, fmt.Errorf("confirm staged prompt: %w", waitErr))
	}
	return nil
}

func (c *CLI) Wait(ctx context.Context, paneID string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", context.DeadlineExceeded
		}
		slice := 30 * time.Second
		if remaining < slice {
			slice = remaining
		}
		callCtx, cancel := context.WithTimeout(ctx, slice+5*time.Second)
		err := c.run(callCtx, nil, "agent", "wait", paneID, "--timeout", fmt.Sprintf("%d", slice.Milliseconds()))
		callErr := callCtx.Err()
		cancel()
		if err == nil {
			return c.statusAfterCompletedWait(ctx, paneID, deadline)
		}
		if hasCode(err, "agent_not_running") {
			return "gone", nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if callErr == nil && !hasCode(err, "timeout") && !hasCode(err, "agent_wait_timeout") {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *CLI) statusAfterCompletedWait(ctx context.Context, paneID string, jobDeadline time.Time) (string, error) {
	statusDeadline := jobDeadline
	if bound := time.Now().Add(10 * time.Second); bound.Before(statusDeadline) {
		statusDeadline = bound
	}
	for {
		if !time.Now().Before(statusDeadline) {
			return "", context.DeadlineExceeded
		}
		statusCtx, cancel := context.WithDeadline(ctx, statusDeadline)
		status, err := c.Status(statusCtx, paneID)
		statusCallErr := statusCtx.Err()
		cancel()
		if err == nil && status != "gone" && status != "unknown" {
			return status, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if err != nil && statusCallErr == nil && !hasCode(err, "timeout") && !hasCode(err, "agent_wait_timeout") {
			return "", err
		}
		// Once agent wait reports completion, never invoke it again. A missing
		// or timed-out immediate status is ambiguous and gets a bounded,
		// rate-limited re-observation instead of becoming a false gone result.
		delay := 100 * time.Millisecond
		if remaining := time.Until(statusDeadline); remaining < delay {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

type agentState struct {
	Status   string
	Sequence uint64
}

func (c *CLI) agentState(ctx context.Context, paneID string) (agentState, error) {
	var result struct {
		Agent struct {
			Status   string `json:"agent_status"`
			Sequence uint64 `json:"state_change_seq"`
		} `json:"agent"`
	}
	if err := c.run(ctx, &result, "agent", "get", paneID); err != nil {
		if hasCode(err, "agent_not_running") {
			return agentState{Status: "gone"}, nil
		}
		return agentState{}, err
	}
	return agentState{Status: result.Agent.Status, Sequence: result.Agent.Sequence}, nil
}

func awaitStateChange(ctx context.Context, initial uint64, observe func(context.Context) (agentState, error)) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := observe(ctx)
		if err != nil {
			return err
		}
		if current.Sequence > initial && current.Status != "unknown" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *CLI) Status(ctx context.Context, paneID string) (string, error) {
	state, err := c.agentState(ctx, paneID)
	return state.Status, err
}

func (c *CLI) StartCommand(ctx context.Context, paneID, command string) error {
	// `herdr pane run` types its command arguments into an interactive shell.
	// Quote the complete script so `sh -c` receives one argument rather than
	// treating the harness flags as sh's own positional parameters.
	return c.run(ctx, nil, "pane", "run", paneID, "sh", "-c", shellQuote(command))
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func (c *CLI) WaitCommand(ctx context.Context, paneID, marker string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	consecutiveFailures := 0
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		code, done, err := c.CommandResult(pollCtx, paneID, marker)
		cancel()
		if err != nil {
			consecutiveFailures++
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			if consecutiveFailures >= 3 {
				return 0, err
			}
		} else {
			consecutiveFailures = 0
			if done {
				return code, nil
			}
		}
		if !time.Now().Before(deadline) {
			return 0, context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (c *CLI) CommandResult(ctx context.Context, paneID, marker string) (int, bool, error) {
	screen, err := c.paneRead(ctx, paneID, 200)
	if err != nil {
		return 0, false, err
	}
	matches := regexp.MustCompile(regexp.QuoteMeta(marker)+`:(\d+)`).FindAllStringSubmatch(screen, -1)
	if len(matches) == 0 {
		return 0, false, nil
	}
	code, err := strconv.Atoi(matches[len(matches)-1][1])
	return code, err == nil, err
}

func (c *CLI) paneRead(ctx context.Context, paneID string, lines int) (string, error) {
	cmd := exec.CommandContext(ctx, c.Bin, "pane", "read", paneID, "--source", "recent-unwrapped", "--lines", fmt.Sprintf("%d", lines), "--format", "text")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("pane read: %s", message)
	}
	return stdout.String(), nil
}

func (c *CLI) CloseWorkspace(ctx context.Context, workspaceID string) error {
	return c.run(ctx, nil, "workspace", "close", workspaceID)
}

func (c *CLI) Focus(ctx context.Context, workspaceID, paneID string) error {
	if workspaceID != "" {
		if err := c.run(ctx, nil, "workspace", "focus", workspaceID); err != nil {
			return err
		}
	}
	if paneID != "" {
		if err := c.run(ctx, nil, "agent", "focus", paneID); err != nil {
			// The workspace remains the useful destination if its old agent pane ended.
			return nil
		}
	}
	return nil
}
