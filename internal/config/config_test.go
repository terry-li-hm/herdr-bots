package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bots.yaml")
	if err := os.WriteFile(path, []byte(body), ConfigRequiredMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func validYAML() string {
	return `version: 1
jobs:
  - id: docs-drift
    revision: 1
    schedule:
      kind: cron
      expression: "0 9 * * 1"
      timezone: Asia/Hong_Kong
      catch_up_grace_minutes: 120
    execution:
      repository: /tmp/repo
      workspace: worktree
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    prompt: Check documentation drift.
    timeout_minutes: 30
    overlap: forbid
    limits:
      max_runs_per_day: 1
`
}

func TestLoadValidConfigAndSnapshot(t *testing.T) {
	cfg, err := Load(writeConfig(t, validYAML()))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	if !job.IsEnabled() || job.Execution.Workspace != WorkspaceWorktree {
		t.Fatalf("defaults not applied: %+v", job)
	}
	if cfg.Capacity.MaxConcurrent() != 2 || cfg.Capacity.MinFreeDisk() != 3.0 {
		t.Fatalf("capacity defaults not applied: %+v", cfg.Capacity)
	}
	if job.DiskReserve() != 1.25 {
		t.Fatalf("disk reserve default = %v, want 1.25", job.DiskReserve())
	}
	raw, revision, err := job.Snapshot()
	if err != nil || len(raw) == 0 || len(revision) != 64 {
		t.Fatalf("bad snapshot: bytes=%d revision=%q err=%v", len(raw), revision, err)
	}
	if strings.Contains(string(raw), "acceptance") {
		t.Fatalf("absent acceptance changed snapshot: %s", raw)
	}
	const wantSnapshotHash = "e31805f4de0c73accacf3e25d7e26f0d403ac9e80ee6fb6bc2cae991feb8005e"
	if revision != wantSnapshotHash {
		t.Fatalf("absent acceptance changed snapshot hash: got %s want %s", revision, wantSnapshotHash)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	body := strings.Replace(validYAML(), "version: 1", "version: 1\nsurprise: true", 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatalf("expected unknown-field error, got %v", err)
	}
}

func TestZeroCatchUpGraceIsDistinctFromDefault(t *testing.T) {
	body := strings.Replace(validYAML(), "catch_up_grace_minutes: 120", "catch_up_grace_minutes: 0", 1)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Jobs[0].CatchUpGrace(); got != 0 {
		t.Fatalf("grace = %s, want zero", got)
	}
}

func TestRootWorkspaceIsRejectedUntilItCanBeIsolated(t *testing.T) {
	body := strings.Replace(validYAML(), "workspace: worktree", "workspace: root", 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "not isolated") {
		t.Fatalf("expected root isolation error, got %v", err)
	}
}

func TestPiRequiresExplicitProviderAndModel(t *testing.T) {
	for _, old := range []string{"      provider: openai-codex\n", "      model: gpt-5.6-sol\n"} {
		body := strings.Replace(validYAML(), old, "", 1)
		if _, err := Load(writeConfig(t, body)); err == nil {
			t.Fatalf("expected validation failure after removing %q", strings.TrimSpace(old))
		}
	}
}

func TestChangedGateRequiresExplicitBaseRef(t *testing.T) {
	body := strings.Replace(validYAML(), "    prompt: Check documentation drift.", "    run_if_changed: true\n    prompt: Check documentation drift.", 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "requires execution.base_ref") {
		t.Fatalf("expected base-ref validation error, got %v", err)
	}
	body = strings.Replace(body, "      workspace: worktree", "      workspace: worktree\n      base_ref: main", 1)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Jobs[0].RunIfChanged || cfg.Jobs[0].Execution.BaseRef != "main" {
		t.Fatalf("change gate not loaded: %+v", cfg.Jobs[0])
	}
}

func TestInvalidCronIsRejectedAtLoad(t *testing.T) {
	body := strings.Replace(validYAML(), `expression: "0 9 * * 1"`, `expression: "not cron"`, 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "invalid cron") {
		t.Fatalf("expected cron error, got %v", err)
	}
}

func TestOnceScheduleRequiresRFC3339(t *testing.T) {
	body := strings.Replace(validYAML(), "kind: cron\n      expression: \"0 9 * * 1\"", "kind: once\n      at: tomorrow", 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "RFC3339") {
		t.Fatalf("expected timestamp error, got %v", err)
	}
}

func TestCapacityLimitsAreValidated(t *testing.T) {
	for name, prefix := range map[string]string{
		"concurrency":      "capacity:\n  max_concurrent_runs: 33\n  min_free_disk_gib: 3\n",
		"zero concurrency": "capacity:\n  max_concurrent_runs: 0\n  min_free_disk_gib: 3\n",
		"disk floor":       "capacity:\n  max_concurrent_runs: 2\n  min_free_disk_gib: 0.1\n",
		"zero disk floor":  "capacity:\n  max_concurrent_runs: 2\n  min_free_disk_gib: 0\n",
		"nan floor":        "capacity:\n  max_concurrent_runs: 2\n  min_free_disk_gib: .nan\n",
		"inf floor":        "capacity:\n  max_concurrent_runs: 2\n  min_free_disk_gib: .inf\n",
		"negative floor":   "capacity:\n  max_concurrent_runs: 2\n  min_free_disk_gib: -1\n",
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(validYAML(), "version: 1\n", "version: 1\n"+prefix, 1)
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Fatal("expected capacity validation failure")
			}
		})
	}
}

func TestPerRunDiskReserveIsValidated(t *testing.T) {
	for _, value := range []string{"0", "0.01", "-.inf", "-1", ".nan", ".inf"} {
		body := strings.Replace(validYAML(), "max_runs_per_day: 1", "max_runs_per_day: 1\n      disk_reserve_gib: "+value, 1)
		if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "disk_reserve_gib") {
			t.Fatalf("expected disk reserve validation failure for %s, got %v", value, err)
		}
	}
}

func TestCapacityBoundEndpointsAndExplicitValuesAreAccepted(t *testing.T) {
	cases := []struct {
		maxConcurrent int
		minFreeDisk   float64
	}{
		{1, 0.5},
		{32, 1024},
		{5, 8},
	}
	for _, tc := range cases {
		prefix := fmt.Sprintf("capacity:\n  max_concurrent_runs: %d\n  min_free_disk_gib: %g\n", tc.maxConcurrent, tc.minFreeDisk)
		t.Run(fmt.Sprintf("max=%d", tc.maxConcurrent), func(t *testing.T) {
			body := strings.Replace(validYAML(), "version: 1\n", "version: 1\n"+prefix, 1)
			cfg, err := Load(writeConfig(t, body))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Capacity.MaxConcurrent() != tc.maxConcurrent || cfg.Capacity.MinFreeDisk() != tc.minFreeDisk {
				t.Fatalf("capacity=%+v want max=%d min=%g", cfg.Capacity, tc.maxConcurrent, tc.minFreeDisk)
			}
		})
	}
}

func TestPerRunDiskReserveBoundEndpointsAreAccepted(t *testing.T) {
	for value, want := range map[string]float64{"0.25": 0.25, "64": 64} {
		body := strings.Replace(validYAML(), "max_runs_per_day: 1", "max_runs_per_day: 1\n      disk_reserve_gib: "+value, 1)
		cfg, err := Load(writeConfig(t, body))
		if err != nil {
			t.Fatalf("reserve %s rejected: %v", value, err)
		}
		if cfg.Jobs[0].DiskReserve() != want {
			t.Fatalf("reserve=%v want=%v", cfg.Jobs[0].DiskReserve(), want)
		}
	}
}

func eventYAML() string {
	return strings.Replace(validYAML(), "kind: cron\n      expression: \"0 9 * * 1\"", "kind: event", 1)
}

func TestEventScheduleRequiresTimezoneAndForbidsClockFieldsAndChangeGate(t *testing.T) {
	cfg, err := Load(writeConfig(t, eventYAML()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jobs[0].Schedule.Kind != ScheduleEvent {
		t.Fatalf("schedule=%+v", cfg.Jobs[0].Schedule)
	}
	cases := map[string]string{
		"expression":     strings.Replace(eventYAML(), "kind: event", "kind: event\n      expression: \"0 9 * * *\"", 1),
		"at":             strings.Replace(eventYAML(), "kind: event", "kind: event\n      at: \"2026-08-25T09:00:00+08:00\"", 1),
		"timezone":       strings.Replace(eventYAML(), "      timezone: Asia/Hong_Kong\n", "", 1),
		"run_if_changed": strings.Replace(eventYAML(), "    prompt: Check documentation drift.", "    run_if_changed: true\n    prompt: Check documentation drift.", 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Fatal("expected event schedule validation failure")
			}
		})
	}
}

// The config is executable authority: verifier commands are arbitrary saved
// local commands, so admission refuses any file another account could read,
// write, or substitute. These tests pin the admission rules that hold before
// parsing; the Lstat-to-open replacement race itself is narrowed by the
// os.SameFile check in Load and cannot be replayed deterministically here.
func TestLoadAcceptsOnlyMode0600(t *testing.T) {
	for name, mode := range map[string]os.FileMode{
		"group readable":  0o640,
		"world readable":  0o604,
		"group writable":  0o620,
		"world writable":  0o602,
		"owner read-only": 0o400,
		"setuid":          0o600 | os.ModeSetuid,
		"group and world": 0o644,
		"all bits":        0o777,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bots.yaml")
			if err := os.WriteFile(path, []byte(validYAML()), mode); err != nil {
				t.Fatal(err)
			}
			// Chmod pins the exact mode: file creation is umask-filtered, which
			// would silently turn write-bit cases into 0600.
			if err := os.Chmod(path, mode); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("mode %04o accepted; exactly 0600 is required", mode)
			}
			if !strings.Contains(err.Error(), "0600") {
				t.Fatalf("error should name the required mode: %v", err)
			}
		})
	}
	path := writeConfig(t, validYAML())
	if _, err := Load(path); err != nil {
		t.Fatalf("0600 rejected: %v", err)
	}
}

func TestLoadRejectsWrongOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("ownership rejection test unsupported without root; not changing ownership outside the test temp directory")
	}
	path := writeConfig(t, validYAML())
	const otherUID = 65534
	if err := os.Chown(path, otherUID, -1); err != nil {
		t.Skipf("ownership rejection test unsupported: chown test fixture: %v", err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("expected wrong-owner rejection, got %v", err)
	}
}

func TestLoadRejectsSymlinkedConfig(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.yaml")
	if err := os.WriteFile(real, []byte(validYAML()), ConfigRequiredMode); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.yaml")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestLoadRejectsNonRegularConfig(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "bots.yaml")
	if err := syscall.Mkfifo(fifo, ConfigRequiredMode); err != nil {
		t.Skipf("cannot create fifo: %v", err)
	}
	if _, err := Load(fifo); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("expected non-regular rejection, got %v", err)
	}
	if _, err := Load(t.TempDir()); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("expected directory rejection, got %v", err)
	}
}

func TestValidateEventID(t *testing.T) {
	valid := []string{"a", "event-1", strings.Repeat("a", 128)}
	for _, id := range valid {
		if err := ValidateEventID(id); err != nil {
			t.Fatalf("valid id %q rejected: %v", id, err)
		}
	}
	invalid := []string{"", "Event", "event_id", "-event", "event-", "event--id", "event.id", "événement", strings.Repeat("a", 129)}
	for _, id := range invalid {
		if err := ValidateEventID(id); err == nil {
			t.Fatalf("invalid id %q accepted", id)
		}
	}
}

func TestAttentionUnreadGuardIsOptionalAndValidated(t *testing.T) {
	cfg, err := Load(writeConfig(t, validYAML()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jobs[0].Attention != nil || cfg.Jobs[0].MaxUnreadTerminalRuns() != 0 {
		t.Fatalf("omitted attention must preserve current behavior: %+v", cfg.Jobs[0].Attention)
	}
	withLimit := strings.Replace(validYAML(), "    prompt: Check documentation drift.", "    attention:\n      max_unread_terminal_runs: 1\n    prompt: Check documentation drift.", 1)
	cfg, err = Load(writeConfig(t, withLimit))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Jobs[0].MaxUnreadTerminalRuns(); got != 1 {
		t.Fatalf("limit=%d want 1", got)
	}
	for _, limit := range []string{"0", "1001", "-3"} {
		body := strings.Replace(withLimit, "max_unread_terminal_runs: 1", "max_unread_terminal_runs: "+limit, 1)
		if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "attention.max_unread_terminal_runs") {
			t.Fatalf("limit %s: expected bound error, got %v", limit, err)
		}
	}
	for _, limit := range []string{"1", "1000"} {
		body := strings.Replace(withLimit, "max_unread_terminal_runs: 1", "max_unread_terminal_runs: "+limit, 1)
		if _, err := Load(writeConfig(t, body)); err != nil {
			t.Fatalf("limit %s: %v", limit, err)
		}
	}
	body := strings.Replace(withLimit, "max_unread_terminal_runs: 1", "max_unread_terminal_runs: 1\n      surprise: true", 1)
	if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatalf("expected unknown attention field error, got %v", err)
	}
}

func TestAcceptanceValidationAndSampling(t *testing.T) {
	withVerifier := strings.Replace(validYAML(), "    overlap: forbid\n    limits:\n", "    overlap: forbid\n    verifier:\n      command: [\"git\", \"diff\", \"--check\"]\n    limits:\n", 1)
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "auto requires verifier",
			body: strings.Replace(validYAML(), "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: auto\n", 1),
			want: "auto mode requires a verifier",
		},
		{
			name: "sample requires verifier",
			body: strings.Replace(validYAML(), "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: sample\n      sample_percent: 10\n", 1),
			want: "sample mode requires a verifier",
		},
		{
			name: "sample lower bound",
			body: strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: sample\n      sample_percent: 0\n", 1),
			want: "acceptance.sample_percent must be between 1 and 100",
		},
		{
			name: "sample upper bound",
			body: strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: sample\n      sample_percent: 101\n", 1),
			want: "acceptance.sample_percent must be between 1 and 100",
		},
		{
			name: "sample percent only on auto",
			body: strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: auto\n      sample_percent: 10\n", 1),
			want: "acceptance.sample_percent is allowed only for sample mode",
		},
		{
			name: "sample percent only on mandatory",
			body: strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: mandatory\n      sample_percent: 10\n", 1),
			want: "acceptance.sample_percent is allowed only for sample mode",
		},
		{
			name: "unknown mode",
			body: strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: later\n", 1),
			want: "acceptance.mode must be mandatory, auto, or sample",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, tc.body)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
	cfg, err := Load(writeConfig(t, strings.Replace(withVerifier, "    overlap: forbid\n", "    overlap: forbid\n    acceptance:\n      mode: sample\n      sample_percent: 100\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jobs[0].AcceptanceMode() != AcceptanceSample {
		t.Fatalf("acceptance=%+v", cfg.Jobs[0].Acceptance)
	}
}

func TestClassifyTerminalRunDeterministicAndFailClosed(t *testing.T) {
	verifying := Job{ID: "job", Verifier: &Verifier{Command: []string{"git", "diff", "--check"}}}
	sampleJob := Job{ID: "job", Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceSample, SamplePercent: 50}}
	foundSample := false
	foundUnsampled := false
	for i := 0; i < 2000 && (!foundSample || !foundUnsampled); i++ {
		runID := fmt.Sprintf("run-%d", i)
		lane, reason, unread := sampleJob.ClassifyTerminalRun(runID, "succeeded", "passed")
		againLane, againReason, againUnread := sampleJob.ClassifyTerminalRun(runID, "succeeded", "passed")
		if lane != againLane || reason != againReason || unread != againUnread {
			t.Fatalf("classification changed for %s", runID)
		}
		if lane == AcceptanceSample {
			foundSample = true
		}
		if lane == AcceptanceAuto {
			foundUnsampled = true
		}
	}
	if !foundSample || !foundUnsampled {
		t.Fatalf("sampling never produced both lanes: sample=%t unsampled=%t", foundSample, foundUnsampled)
	}
	for _, tc := range []struct {
		name       string
		job        Job
		state      string
		verdict    string
		wantLane   string
		wantReason string
		wantUnread bool
	}{
		{name: "missing acceptance defaults mandatory", job: verifying, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "acceptance_missing", wantUnread: true},
		{name: "explicit mandatory", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceMandatory}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "mode_mandatory", wantUnread: true},
		{name: "auto passes through", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceAuto, wantReason: "verifier_passed", wantUnread: false},
		{name: "malformed auto without verifier", job: Job{Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "verifier_missing", wantUnread: true},
		{name: "malformed sample without verifier", job: Job{Acceptance: &Acceptance{Mode: AcceptanceSample, SamplePercent: 100}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "verifier_missing", wantUnread: true},
		{name: "malformed auto sample percent", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceAuto, SamplePercent: 10}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "acceptance_invalid", wantUnread: true},
		{name: "malformed sample percent", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceSample, SamplePercent: 101}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "acceptance_invalid", wantUnread: true},
		{name: "malformed verifier command", job: Job{Verifier: &Verifier{Command: []string{""}}, Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "succeeded", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "verifier_missing", wantUnread: true},
		{name: "failed verifier overrides", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "succeeded", verdict: "failed", wantLane: AcceptanceMandatory, wantReason: "verifier_failed", wantUnread: true},
		{name: "blank verdict is unverified", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "succeeded", verdict: "", wantLane: AcceptanceMandatory, wantReason: "unverified", wantUnread: true},
		{name: "unknown state is mandatory", job: Job{Verifier: verifying.Verifier, Acceptance: &Acceptance{Mode: AcceptanceAuto}}, state: "", verdict: "passed", wantLane: AcceptanceMandatory, wantReason: "state_unknown", wantUnread: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lane, reason, unread := tc.job.ClassifyTerminalRun("run-id", tc.state, tc.verdict)
			if lane != tc.wantLane || reason != tc.wantReason || unread != tc.wantUnread {
				t.Fatalf("lane=%s reason=%s unread=%t", lane, reason, unread)
			}
		})
	}
}
