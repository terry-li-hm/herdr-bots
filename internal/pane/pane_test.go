package pane

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

func TestTruncatePreservesUnicodeBoundaries(t *testing.T) {
	if got := truncate("排程結果", 3); got != "排程…" {
		t.Fatalf("got %q", got)
	}
}

func TestEmptyViewShowsBotInboxStrings(t *testing.T) {
	m := model{}
	view := m.View()
	if !strings.Contains(view, "Bot inbox") {
		t.Fatalf("empty view missing title %q", "Bot inbox")
	}
	if !strings.Contains(view, "No bot runs yet.") {
		t.Fatalf("empty view missing empty state %q", "No bot runs yet.")
	}
}

func TestViewKeepsLongStateVisible(t *testing.T) {
	m := model{runs: []store.Run{{JobID: "job", TaskVerdict: "passed", AcceptanceLane: "auto", AcceptanceReason: "verifier_passed", State: store.StateProvisioning, UpdatedAt: time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)}}}
	view := m.View()
	if !strings.Contains(view, store.StateProvisioning) {
		t.Fatalf("state truncated in view:\n%s", view)
	}
}

func TestLoadGroupsAcceptanceLanesAndViewShowsVerdictAndReason(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
	if _, err := state.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	create := func(job config.Job, at time.Time) store.Run {
		raw, _, err := job.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		run, err := state.CreateManualRun(context.Background(), "job", "rev1", raw, at)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Finish(context.Background(), run.ID, store.StateAccepted, store.StateSucceeded, "completed", "completed", "passed", "", "done", at.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := state.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	mandatory := create(config.Job{ID: "job"}, now)
	sample := create(config.Job{ID: "job", Verifier: &config.Verifier{Command: []string{"git", "diff", "--check"}}, Acceptance: &config.Acceptance{Mode: config.AcceptanceSample, SamplePercent: 100}}, now.Add(time.Minute))
	auto := create(config.Job{ID: "job", Verifier: &config.Verifier{Command: []string{"git", "diff", "--check"}}, Acceptance: &config.Acceptance{Mode: config.AcceptanceAuto}}, now.Add(2*time.Minute))
	active, err := state.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	loaded := load(state, nil)
	want := []string{mandatory.ID, sample.ID, auto.ID, active.ID}
	if len(loaded.runs) != len(want) {
		t.Fatalf("runs=%+v", loaded.runs)
	}
	for i, runID := range want {
		if loaded.runs[i].ID != runID {
			t.Fatalf("order=%+v want=%v", loaded.runs, want)
		}
	}
	view := loaded.View()
	for _, wantText := range []string{mandatory.TaskVerdict, mandatory.AcceptanceLane, mandatory.AcceptanceReason, sample.AcceptanceLane, auto.AcceptanceLane} {
		if !strings.Contains(view, wantText) {
			t.Fatalf("view missing %q:\n%s", wantText, view)
		}
	}
}
