package config

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

const CurrentVersion = 1

// ConfigRequiredMode is the only accepted file mode for a job definition. The
// config is executable authority: verifier commands are arbitrary saved local
// commands, and job definitions name repositories, harnesses, and models that
// will act on this machine. A file writable or readable by anyone else would
// let another local account learn the saved authority or replace it between
// reads, so Load refuses anything except a regular, non-symlink, 0600 file
// owned by the effective user. macOS ACLs are not inspected; operators must
// ensure they do not grant any other user access.
const ConfigRequiredMode = 0o600

const (
	DefaultMaxConcurrentRuns = 2
	DefaultMinFreeDiskGiB    = 3.0
	DefaultDiskReserveGiB    = 1.25
)

var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

const (
	HarnessClaudeCode = "claude-code"
	HarnessPi         = "pi"

	ScheduleCron  = "cron"
	ScheduleOnce  = "once"
	ScheduleEvent = "event"

	WorkspaceWorktree = "worktree"
	WorkspaceRoot     = "root"

	PermissionReadOnly  = "read-only-no-network"
	PermissionRepoWrite = "repo-write-no-network"

	AcceptanceMandatory = "mandatory"
	AcceptanceAuto      = "auto"
	AcceptanceSample    = "sample"
)

type Config struct {
	Version  int      `yaml:"version" json:"version"`
	Capacity Capacity `yaml:"capacity,omitempty" json:"capacity"`
	Jobs     []Job    `yaml:"jobs" json:"jobs"`
}

type Capacity struct {
	MaxConcurrentRuns *int     `yaml:"max_concurrent_runs,omitempty" json:"max_concurrent_runs"`
	MinFreeDiskGiB    *float64 `yaml:"min_free_disk_gib,omitempty" json:"min_free_disk_gib"`
}

type Job struct {
	ID             string      `yaml:"id" json:"id"`
	Revision       int         `yaml:"revision" json:"revision"`
	Enabled        *bool       `yaml:"enabled,omitempty" json:"enabled"`
	Schedule       Schedule    `yaml:"schedule" json:"schedule"`
	Execution      Execution   `yaml:"execution" json:"execution"`
	RunIfChanged   bool        `yaml:"run_if_changed,omitempty" json:"run_if_changed"`
	Prompt         string      `yaml:"prompt" json:"prompt"`
	TimeoutMinutes int         `yaml:"timeout_minutes,omitempty" json:"timeout_minutes"`
	Overlap        string      `yaml:"overlap,omitempty" json:"overlap"`
	Verifier       *Verifier   `yaml:"verifier,omitempty" json:"verifier,omitempty"`
	Acceptance     *Acceptance `yaml:"acceptance,omitempty" json:"acceptance,omitempty"`
	Limits         Limits      `yaml:"limits,omitempty" json:"limits"`
	Attention      *Attention  `yaml:"attention,omitempty" json:"attention,omitempty"`
}

// Attention holds opt-in operator-attention gates. An absent attention block
// preserves the historical admission behavior exactly.
type Attention struct {
	// MaxUnreadTerminalRuns pauses the job before another run is admitted when
	// this many terminal runs are still unread. Runs begin unread, so only
	// terminal states are counted. An explicit value must be 1..1000.
	MaxUnreadTerminalRuns *int `yaml:"max_unread_terminal_runs,omitempty" json:"max_unread_terminal_runs,omitempty"`
}

type Schedule struct {
	Kind                string `yaml:"kind" json:"kind"`
	Expression          string `yaml:"expression,omitempty" json:"expression,omitempty"`
	At                  string `yaml:"at,omitempty" json:"at,omitempty"`
	Timezone            string `yaml:"timezone" json:"timezone"`
	CatchUpGraceMinutes *int   `yaml:"catch_up_grace_minutes,omitempty" json:"catch_up_grace_minutes"`
}

type Execution struct {
	Repository        string `yaml:"repository" json:"repository"`
	Workspace         string `yaml:"workspace,omitempty" json:"workspace"`
	BaseRef           string `yaml:"base_ref,omitempty" json:"base_ref,omitempty"`
	Harness           string `yaml:"harness" json:"harness"`
	Provider          string `yaml:"provider,omitempty" json:"provider,omitempty"`
	Model             string `yaml:"model" json:"model"`
	Thinking          string `yaml:"thinking,omitempty" json:"thinking"`
	PermissionProfile string `yaml:"permission_profile" json:"permission_profile"`
}

type Verifier struct {
	Command []string `yaml:"command" json:"command"`
}

type Acceptance struct {
	Mode          string `yaml:"mode" json:"mode"`
	SamplePercent int    `yaml:"sample_percent,omitempty" json:"sample_percent,omitempty"`
}

type Limits struct {
	MaxRunsPerDay  int      `yaml:"max_runs_per_day,omitempty" json:"max_runs_per_day"`
	DiskReserveGiB *float64 `yaml:"disk_reserve_gib,omitempty" json:"disk_reserve_gib"`
}

func Load(path string) (*Config, error) {
	// Admit the descriptor, not a preliminary pathname observation. O_NOFOLLOW
	// makes the open itself reject a final symlink, CLOEXEC prevents authority
	// from leaking to child processes, and NONBLOCK lets us fstat and reject a
	// FIFO without ever waiting for a writer.
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if err != nil {
		if err == syscall.ELOOP {
			return nil, fmt.Errorf("config %s is a symlink; the job definition must be a regular file owned by the current user", path)
		}
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("config %s: cannot adopt opened descriptor", path)
	}
	defer f.Close()

	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() {
		return nil, fmt.Errorf("config %s is not a regular file", path)
	}
	specialMode := opened.Mode() & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	if opened.Mode().Perm() != ConfigRequiredMode || specialMode != 0 {
		return nil, fmt.Errorf("config %s has permissions %04o with special mode bits %v; exactly 0600 is required because the job definition is executable authority (verifier commands are arbitrary saved local commands)", path, opened.Mode().Perm(), specialMode)
	}
	stat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("config %s: cannot determine owner of opened file", path)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("config %s is owned by uid %d; effective uid %d is required", path, stat.Uid, os.Geteuid())
	}

	// Confirm immediately before parsing that the current path is still a
	// non-symlink name for the exact inode admitted through the descriptor.
	current, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("recheck config %s: %w", path, err)
	}
	if current.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("config %s became a symlink while opening; refusing to parse", path)
	}
	if !os.SameFile(current, opened) {
		return nil, fmt.Errorf("config %s was replaced while opening; refusing to parse a different file than the one admitted", path)
	}

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Version == 0 {
		cfg.Version = CurrentVersion
	}
	if cfg.Version != CurrentVersion {
		return nil, fmt.Errorf("unsupported config version %d, want %d", cfg.Version, CurrentVersion)
	}
	cfg.Capacity.applyDefaults()
	if err := cfg.Capacity.validate(); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(cfg.Jobs))
	for i := range cfg.Jobs {
		job := &cfg.Jobs[i]
		job.applyDefaults()
		if err := job.validate(); err != nil {
			return nil, err
		}
		if _, ok := seen[job.ID]; ok {
			return nil, fmt.Errorf("duplicate job id %q", job.ID)
		}
		seen[job.ID] = struct{}{}
	}
	return &cfg, nil
}

func (j *Job) applyDefaults() {
	if j.Enabled == nil {
		enabled := true
		j.Enabled = &enabled
	}
	if j.Revision == 0 {
		j.Revision = 1
	}
	if j.Execution.Workspace == "" {
		j.Execution.Workspace = WorkspaceWorktree
	}
	if j.Execution.Thinking == "" {
		j.Execution.Thinking = "high"
	}
	if j.TimeoutMinutes == 0 {
		j.TimeoutMinutes = 60
	}
	if j.Overlap == "" {
		j.Overlap = "forbid"
	}
	if j.Limits.MaxRunsPerDay == 0 {
		j.Limits.MaxRunsPerDay = 10
	}
	if j.Limits.DiskReserveGiB == nil {
		value := DefaultDiskReserveGiB
		j.Limits.DiskReserveGiB = &value
	}
	if j.Schedule.CatchUpGraceMinutes == nil {
		minutes := 120
		j.Schedule.CatchUpGraceMinutes = &minutes
	}
	j.Execution.Repository = expandHome(j.Execution.Repository)
}

func (j Job) IsEnabled() bool { return j.Enabled != nil && *j.Enabled }

func (j Job) CatchUpGrace() time.Duration {
	if j.Schedule.CatchUpGraceMinutes == nil {
		return 0
	}
	return time.Duration(*j.Schedule.CatchUpGraceMinutes) * time.Minute
}

func (j Job) Snapshot() ([]byte, string, error) {
	raw, err := json.Marshal(j)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func (j Job) validate() error {
	if j.ID == "" {
		return fmt.Errorf("job without an id")
	}
	if !validID(j.ID) {
		return fmt.Errorf("%s: id must use lowercase letters, digits, and single hyphens", j.ID)
	}
	if j.Revision < 1 {
		return fmt.Errorf("%s: revision must be at least 1", j.ID)
	}
	if strings.TrimSpace(j.Prompt) == "" {
		return fmt.Errorf("%s: prompt is required", j.ID)
	}
	if j.TimeoutMinutes < 1 || j.TimeoutMinutes > 24*60 {
		return fmt.Errorf("%s: timeout_minutes must be between 1 and 1440", j.ID)
	}
	if j.Overlap != "forbid" && j.Overlap != "queue_one" && j.Overlap != "allow" {
		return fmt.Errorf("%s: overlap must be forbid, queue_one, or allow", j.ID)
	}
	if j.Limits.MaxRunsPerDay < 1 {
		return fmt.Errorf("%s: max_runs_per_day must be positive", j.ID)
	}
	reserve := j.DiskReserve()
	if math.IsNaN(reserve) || math.IsInf(reserve, 0) || reserve < 0.25 || reserve > 64 {
		return fmt.Errorf("%s: disk_reserve_gib must be between 0.25 and 64", j.ID)
	}
	if j.Attention != nil && j.Attention.MaxUnreadTerminalRuns != nil {
		limit := *j.Attention.MaxUnreadTerminalRuns
		if limit < minUnreadTerminalRunsLimit || limit > maxUnreadTerminalRunsLimit {
			return fmt.Errorf("%s: attention.max_unread_terminal_runs must be between %d and %d", j.ID, minUnreadTerminalRunsLimit, maxUnreadTerminalRunsLimit)
		}
	}
	if err := j.Schedule.validate(j.ID); err != nil {
		return err
	}
	if err := j.Execution.validate(j.ID); err != nil {
		return err
	}
	if j.RunIfChanged && j.Schedule.Kind == ScheduleEvent {
		return fmt.Errorf("%s: event schedules forbid run_if_changed", j.ID)
	}
	if j.RunIfChanged && j.Execution.BaseRef == "" {
		return fmt.Errorf("%s: run_if_changed requires execution.base_ref", j.ID)
	}
	if j.Verifier != nil {
		if len(j.Verifier.Command) == 0 {
			return fmt.Errorf("%s: verifier.command must not be empty", j.ID)
		}
		for _, arg := range j.Verifier.Command {
			if arg == "" {
				return fmt.Errorf("%s: verifier.command contains an empty argument", j.ID)
			}
		}
	}
	if j.Acceptance != nil {
		switch j.Acceptance.Mode {
		case AcceptanceMandatory:
			if j.Acceptance.SamplePercent != 0 {
				return fmt.Errorf("%s: acceptance.sample_percent is allowed only for sample mode", j.ID)
			}
		case AcceptanceAuto:
			if j.Acceptance.SamplePercent != 0 {
				return fmt.Errorf("%s: acceptance.sample_percent is allowed only for sample mode", j.ID)
			}
			if j.Verifier == nil {
				return fmt.Errorf("%s: acceptance auto mode requires a verifier", j.ID)
			}
		case AcceptanceSample:
			if j.Verifier == nil {
				return fmt.Errorf("%s: acceptance sample mode requires a verifier", j.ID)
			}
			if j.Acceptance.SamplePercent < 1 || j.Acceptance.SamplePercent > 100 {
				return fmt.Errorf("%s: acceptance.sample_percent must be between 1 and 100", j.ID)
			}
		default:
			return fmt.Errorf("%s: acceptance.mode must be mandatory, auto, or sample", j.ID)
		}
	}
	return nil
}

func (j Job) DiskReserve() float64 {
	if j.Limits.DiskReserveGiB == nil {
		return DefaultDiskReserveGiB
	}
	return *j.Limits.DiskReserveGiB
}

const minUnreadTerminalRunsLimit = 1
const maxUnreadTerminalRunsLimit = 1000

// MaxUnreadTerminalRuns returns the configured unread-work guard limit, or 0
// when no policy is configured. Zero always means "no guard".
func (j Job) MaxUnreadTerminalRuns() int {
	if j.Attention == nil || j.Attention.MaxUnreadTerminalRuns == nil {
		return 0
	}
	return *j.Attention.MaxUnreadTerminalRuns
}

func (j Job) AcceptanceMode() string {
	if j.Acceptance == nil || j.Acceptance.Mode == "" {
		return AcceptanceMandatory
	}
	return j.Acceptance.Mode
}

func (j Job) ClassifyTerminalRun(runID, state, verdict string) (string, string, bool) {
	normalizedVerdict := strings.TrimSpace(verdict)
	if normalizedVerdict == "" {
		normalizedVerdict = "unverified"
	}
	if normalizedVerdict == "failed" {
		return AcceptanceMandatory, "verifier_failed", true
	}
	if state != "succeeded" {
		if strings.TrimSpace(state) == "" {
			return AcceptanceMandatory, "state_unknown", true
		}
		return AcceptanceMandatory, "state_" + state, true
	}
	if normalizedVerdict != "passed" {
		return AcceptanceMandatory, "unverified", true
	}
	switch j.AcceptanceMode() {
	case AcceptanceAuto:
		if j.Acceptance != nil && j.Acceptance.SamplePercent != 0 {
			return AcceptanceMandatory, "acceptance_invalid", true
		}
		if !j.hasDeterministicVerifier() {
			return AcceptanceMandatory, "verifier_missing", true
		}
		return AcceptanceAuto, "verifier_passed", false
	case AcceptanceSample:
		if j.Acceptance == nil || j.Acceptance.SamplePercent < 1 || j.Acceptance.SamplePercent > 100 {
			return AcceptanceMandatory, "acceptance_invalid", true
		}
		if !j.hasDeterministicVerifier() {
			return AcceptanceMandatory, "verifier_missing", true
		}
		if acceptanceSampled(runID, j.Acceptance.SamplePercent) {
			return AcceptanceSample, "sampled", true
		}
		return AcceptanceAuto, "unsampled", false
	case AcceptanceMandatory:
		if j.Acceptance == nil {
			return AcceptanceMandatory, "acceptance_missing", true
		}
		return AcceptanceMandatory, "mode_mandatory", true
	default:
		return AcceptanceMandatory, "acceptance_unknown", true
	}
}

func (j Job) hasDeterministicVerifier() bool {
	if j.Verifier == nil || len(j.Verifier.Command) == 0 {
		return false
	}
	for _, argument := range j.Verifier.Command {
		if argument == "" {
			return false
		}
	}
	return true
}

func acceptanceSampled(runID string, samplePercent int) bool {
	if samplePercent <= 0 {
		return false
	}
	if samplePercent >= 100 {
		return true
	}
	sum := sha256.Sum256([]byte(runID))
	bucket := binary.BigEndian.Uint64(sum[:8]) % 100
	return int(bucket) < samplePercent
}

func (c *Capacity) applyDefaults() {
	if c.MaxConcurrentRuns == nil {
		value := DefaultMaxConcurrentRuns
		c.MaxConcurrentRuns = &value
	}
	if c.MinFreeDiskGiB == nil {
		value := DefaultMinFreeDiskGiB
		c.MinFreeDiskGiB = &value
	}
}

func (c Capacity) MaxConcurrent() int {
	if c.MaxConcurrentRuns == nil {
		return DefaultMaxConcurrentRuns
	}
	return *c.MaxConcurrentRuns
}

func (c Capacity) MinFreeDisk() float64 {
	if c.MinFreeDiskGiB == nil {
		return DefaultMinFreeDiskGiB
	}
	return *c.MinFreeDiskGiB
}

func (c Capacity) validate() error {
	maxRuns := c.MaxConcurrent()
	if maxRuns < 1 || maxRuns > 32 {
		return fmt.Errorf("capacity.max_concurrent_runs must be between 1 and 32")
	}
	minFree := c.MinFreeDisk()
	if math.IsNaN(minFree) || math.IsInf(minFree, 0) || minFree < 0.5 || minFree > 1024 {
		return fmt.Errorf("capacity.min_free_disk_gib must be between 0.5 and 1024")
	}
	return nil
}

func (s Schedule) validate(jobID string) error {
	if s.Kind != ScheduleCron && s.Kind != ScheduleOnce && s.Kind != ScheduleEvent {
		return fmt.Errorf("%s: schedule.kind must be cron, once, or event", jobID)
	}
	if s.Timezone == "" {
		return fmt.Errorf("%s: schedule.timezone is required", jobID)
	}
	if _, err := time.LoadLocation(s.Timezone); err != nil {
		return fmt.Errorf("%s: invalid timezone %q: %w", jobID, s.Timezone, err)
	}
	if s.CatchUpGraceMinutes == nil || *s.CatchUpGraceMinutes < 0 {
		return fmt.Errorf("%s: catch_up_grace_minutes must be zero or positive", jobID)
	}
	switch s.Kind {
	case ScheduleCron:
		if strings.TrimSpace(s.Expression) == "" || s.At != "" {
			return fmt.Errorf("%s: cron schedule requires expression and forbids at", jobID)
		}
		if _, err := cronParser.Parse(s.Expression); err != nil {
			return fmt.Errorf("%s: invalid cron expression %q: %w", jobID, s.Expression, err)
		}
	case ScheduleOnce:
		if s.At == "" || s.Expression != "" {
			return fmt.Errorf("%s: once schedule requires at and forbids expression", jobID)
		}
		if _, err := time.Parse(time.RFC3339, s.At); err != nil {
			return fmt.Errorf("%s: schedule.at must be RFC3339: %w", jobID, err)
		}
	case ScheduleEvent:
		if s.Expression != "" || s.At != "" {
			return fmt.Errorf("%s: event schedule forbids expression and at", jobID)
		}
	}
	return nil
}

func (e Execution) validate(jobID string) error {
	if !filepath.IsAbs(e.Repository) {
		return fmt.Errorf("%s: execution.repository must resolve to an absolute path", jobID)
	}
	if e.Workspace != WorkspaceWorktree {
		return fmt.Errorf("%s: execution.workspace must be worktree in version one; root is not isolated from user or sibling activity", jobID)
	}
	if strings.TrimSpace(e.BaseRef) != e.BaseRef || strings.HasPrefix(e.BaseRef, "-") || strings.ContainsAny(e.BaseRef, "\x00\r\n") {
		return fmt.Errorf("%s: execution.base_ref is invalid", jobID)
	}
	if e.Harness != HarnessClaudeCode && e.Harness != HarnessPi {
		return fmt.Errorf("%s: execution.harness must be claude-code or pi", jobID)
	}
	if e.Model == "" {
		return fmt.Errorf("%s: execution.model is required; use harness-default explicitly if intended", jobID)
	}
	if e.Harness == HarnessPi && e.Provider == "" {
		return fmt.Errorf("%s: pi execution requires an explicit provider", jobID)
	}
	if e.Harness == HarnessClaudeCode && e.Provider != "" {
		return fmt.Errorf("%s: claude-code execution does not accept provider", jobID)
	}
	allowedThinking := map[string]bool{
		"off": true, "minimal": true, "low": true, "medium": true,
		"high": true, "xhigh": true, "max": true,
	}
	if !allowedThinking[e.Thinking] {
		return fmt.Errorf("%s: unsupported thinking level %q", jobID, e.Thinking)
	}
	if e.Harness == HarnessClaudeCode && (e.Thinking == "off" || e.Thinking == "minimal") {
		return fmt.Errorf("%s: claude-code thinking must be low, medium, high, xhigh, or max", jobID)
	}
	if e.PermissionProfile != PermissionReadOnly && e.PermissionProfile != PermissionRepoWrite {
		return fmt.Errorf("%s: permission_profile must be %s or %s", jobID, PermissionReadOnly, PermissionRepoWrite)
	}
	return nil
}

func ValidateEventID(id string) error {
	if len(id) < 1 || len(id) > 128 || !validID(id) {
		return fmt.Errorf("event id must be 1-128 lowercase ASCII letters or digits separated by single hyphens")
	}
	return nil
}

func validID(id string) bool {
	if strings.HasPrefix(id, "-") || strings.HasSuffix(id, "-") || strings.Contains(id, "--") {
		return false
	}
	for _, r := range id {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return id != ""
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}
