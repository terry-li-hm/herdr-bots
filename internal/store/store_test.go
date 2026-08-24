package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ts(value string) time.Time { return must(time.Parse(time.RFC3339, value)) }
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func syncJob(t *testing.T, s *Store, now time.Time) JobState {
	t.Helper()
	job, err := s.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func request(at time.Time) AcceptRequest {
	return AcceptRequest{JobID: "job", JobRevision: "rev1", OccurrenceKey: "cron:" + at.Format("2006-01-02T15:04"), Definition: []byte(`{"id":"job"}`), Trigger: "cron", ScheduledFor: at, Overlap: "forbid", DayStart: at.Add(-time.Hour), MaxRunsPerDay: 10, Now: at.Add(30 * time.Second)}
}

func TestOccurrenceIsAcceptedOnce(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	first, err := s.AcceptOccurrence(context.Background(), request(at))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcceptOccurrence(context.Background(), request(at))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Inserted || first.Run == nil || second.Inserted || second.Outcome != "accepted" {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestSuccessfulRunPersistsSourceContextForNextChangeGate(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	req := request(at)
	req.SourceBaseRevision = strings.Repeat("a", 40)
	req.SourceRevision = strings.Repeat("b", 40)
	req.InputContext = "changed paths"
	accepted, err := s.AcceptOccurrence(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), accepted.Run.ID, StateAccepted, StateSucceeded, "completed", "completed", "passed", "", "", at); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(context.Background(), accepted.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	last, err := s.LastSuccessfulSource(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceBaseRevision != req.SourceBaseRevision || got.SourceRevision != req.SourceRevision || got.InputContext != req.InputContext || last.SourceRevision != req.SourceRevision || last.JobRevision != req.JobRevision {
		t.Fatalf("run=%+v last=%+v", got, last)
	}
}

func TestRepeatedDSTWallClockKeyIsAcceptedOnce(t *testing.T) {
	s := testStore(t)
	firstAt := ts("2026-11-01T05:30:00Z")
	syncJob(t, s, firstAt.Add(-time.Hour))
	first := request(firstAt)
	first.OccurrenceKey = "cron:2026-11-01T01:30"
	accepted, err := s.AcceptOccurrence(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), accepted.Run.ID, StateAccepted, StateSucceeded, "completed", "completed", "unverified", "", "", first.Now); err != nil {
		t.Fatal(err)
	}
	second := request(firstAt.Add(time.Hour))
	second.OccurrenceKey = first.OccurrenceKey
	got, err := s.AcceptOccurrence(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Inserted || got.Outcome != "accepted" {
		t.Fatalf("repeated wall clock was not deduplicated: %+v", got)
	}
}

func TestLastJobResultReportsUnchangedSkip(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	if _, err := s.RecordOccurrence(context.Background(), "job", "cron:2026-08-22T01:00", at, at, "skipped_unchanged", "same revision"); err != nil {
		t.Fatal(err)
	}
	got, err := s.LastJobResult(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if got != "skipped_unchanged" {
		t.Fatalf("result=%q", got)
	}
}

func TestNumericInstantsOrderRunsOccurrencesAndSourceBaselines(t *testing.T) {
	s := testStore(t)
	exact := must(time.Parse(time.RFC3339Nano, "2026-08-22T02:00:00Z"))
	fractional := must(time.Parse(time.RFC3339Nano, "2026-08-22T02:00:00.5Z"))
	syncJob(t, s, exact.Add(-time.Hour))

	older, err := s.CreateManualRun(context.Background(), "job", "older", []byte(`{"id":"job"}`), exact)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := s.CreateManualRun(context.Background(), "job", "newer", []byte(`{"id":"job"}`), fractional)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListRuns(context.Background(), "job", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].ID != newer.ID || listed[1].ID != older.ID {
		t.Fatalf("latest run order=%+v", listed)
	}
	nonterminal, err := s.NonTerminalRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nonterminal) != 2 || nonterminal[0].ID != older.ID || nonterminal[1].ID != newer.ID {
		t.Fatalf("FIFO run order=%+v", nonterminal)
	}
	if err := s.SetSourceContext(context.Background(), older.ID, "base-a", "source-a", "older"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSourceContext(context.Background(), newer.ID, "base-b", "source-b", "newer"); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), older.ID, StateAccepted, StateSucceeded, "completed", "completed", "passed", "", "", exact); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), newer.ID, StateAccepted, StateSucceeded, "completed", "completed", "passed", "", "", fractional); err != nil {
		t.Fatal(err)
	}
	baseline, err := s.LastSuccessfulSource(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if baseline.JobRevision != "newer" || baseline.SourceRevision != "source-b" {
		t.Fatalf("baseline=%+v", baseline)
	}

	first := request(exact)
	first.OccurrenceKey = "cron:exact"
	first.Overlap = "allow"
	first.Now = exact
	if _, err := s.AcceptOccurrence(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordOccurrence(context.Background(), "job", "cron:fractional", fractional, fractional, "skipped_overlap", "newer occurrence"); err != nil {
		t.Fatal(err)
	}
	result, err := s.LastJobResult(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if result != "skipped_overlap" {
		t.Fatalf("latest occurrence result=%q", result)
	}
}

func TestRunOrderUsesStableIDTieBreakers(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T02:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	first, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	ascending := []string{first.ID, second.ID}
	sort.Strings(ascending)
	nonterminal, err := s.NonTerminalRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nonterminal) != 2 || nonterminal[0].ID != ascending[0] || nonterminal[1].ID != ascending[1] {
		t.Fatalf("ascending tie-break=%+v want=%v", nonterminal, ascending)
	}
	listed, err := s.ListRuns(context.Background(), "job", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].ID != ascending[1] || listed[1].ID != ascending[0] {
		t.Fatalf("descending tie-break=%+v want reverse %v", listed, ascending)
	}
}

func TestForbidOverlapSkipsSecondRun(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	if _, err := s.AcceptOccurrence(context.Background(), request(at)); err != nil {
		t.Fatal(err)
	}
	req := request(at.Add(time.Hour))
	req.Now = req.ScheduledFor
	got, err := s.AcceptOccurrence(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != "skipped_overlap" || got.Run != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestQueueOneAllowsOnePending(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	first := request(at)
	first.Overlap = "allow"
	accepted, err := s.AcceptOccurrence(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), accepted.Run.ID, StateAccepted, StateRunning, "test", at); err != nil {
		t.Fatal(err)
	}
	queued := request(at.Add(time.Hour))
	queued.Overlap = "queue_one"
	queued.Now = queued.ScheduledFor
	one, err := s.AcceptOccurrence(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	again := request(at.Add(2 * time.Hour))
	again.Overlap = "queue_one"
	again.Now = again.ScheduledFor
	two, err := s.AcceptOccurrence(context.Background(), again)
	if err != nil {
		t.Fatal(err)
	}
	if one.Run == nil || two.Outcome != "skipped_overlap" {
		t.Fatalf("one=%+v two=%+v", one, two)
	}
}

func TestTerminalStateCannotTransition(t *testing.T) {
	s := testStore(t)
	at := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, at.Add(-time.Hour))
	accepted, err := s.AcceptOccurrence(context.Background(), request(at))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), accepted.Run.ID, StateAccepted, StateFailed, "failed", "not_started", "unverified", "test", "boom", at); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), accepted.Run.ID, StateFailed, StateRunning, "bad", at); err == nil {
		t.Fatal("terminal transition should fail")
	}
}

func TestNewRevisionResetsOneOffCompletion(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now)
	if err := s.SetCompleted(context.Background(), "job", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SyncJob(context.Background(), "job", "rev2", []byte(`{"id":"job","revision":2}`), true, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if job.Completed {
		t.Fatal("new definition revision should reset one-off completion")
	}
}

func TestNewJobCursorStartsAtSyncTime(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	job := syncJob(t, s, now)
	if !job.Cursor.Equal(now) {
		t.Fatalf("cursor=%s want=%s", job.Cursor, now)
	}
}

func TestDecideAdmissionClaimIsSerializedAcrossConcurrentCalls(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	runs := make([]Run, 2)
	for i := range runs {
		run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = run
	}
	check := func(active []Run) (AdmissionDecision, error) {
		if len(active) >= 1 {
			return AdmissionDecision{}, nil // capacity one is already spent
		}
		return AdmissionDecision{Admit: true}, nil
	}
	var wg sync.WaitGroup
	results := make([]bool, len(runs))
	for i := range runs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			admitted, err := s.DecideAdmission(context.Background(), runs[i].ID, "owner", "dev1", 1.5, now, now.Add(time.Minute), check)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = admitted
		}(i)
	}
	wg.Wait()
	admitted := 0
	for _, got := range results {
		if got {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted=%d results=%v; concurrent claims overspent capacity", admitted, results)
	}
	states := map[string]int{}
	for _, run := range runs {
		got, err := s.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		states[got.State]++
		if got.State == StateProvisioning && (got.ProvisioningOwner != "owner" || !got.ProvisioningLeaseUntil.Equal(now.Add(time.Minute))) {
			t.Fatalf("claim was not persisted atomically: %+v", got)
		}
	}
	if states[StateProvisioning] != 1 || states[StateAccepted] != 1 {
		t.Fatalf("states=%v; want exactly one durable provisioning claim", states)
	}
}

func TestDecideAdmissionRechecksAuthorityAcrossIndependentHandles(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*Store, time.Time) error
	}{
		{name: "global pause", mutate: func(state *Store, _ time.Time) error {
			return state.SetGlobalPaused(context.Background(), true)
		}},
		{name: "job pause", mutate: func(state *Store, _ time.Time) error {
			return state.SetPaused(context.Background(), "job", true)
		}},
		{name: "job disable", mutate: func(state *Store, now time.Time) error {
			_, err := state.SyncJobAuthority(context.Background(), "job", 2, "rev2", []byte(`{"id":"job","revision":2}`), false, now)
			return err
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "authority.sqlite3")
			first, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			now := ts("2026-08-22T01:00:00Z")
			if _, err := first.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			run, err := first.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
			if err != nil {
				t.Fatal(err)
			}
			if err := testCase.mutate(second, now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			admitted, err := first.DecideAdmission(context.Background(), run.ID, "owner", "dev1", 1.5, now.Add(2*time.Second), now.Add(3*time.Second), func([]Run) (AdmissionDecision, error) {
				return AdmissionDecision{Admit: true}, nil
			})
			if err != nil || admitted {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
			got, err := second.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != StateAccepted || got.JobRevision != "rev1" || string(got.Definition) != `{"id":"job"}` || got.ProvisioningOwner != "" {
				t.Fatalf("immutable accepted run changed or was claimed: %+v", got)
			}
		})
	}
}

func TestCanaryAdmissionHoldsAreTerminalAndTyped(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		definition []byte
		prepare    func(*testing.T, *Store, time.Time)
		decision   AdmissionDecision
		wantCode   string
	}{
		{
			name:       "overlap",
			definition: []byte(`{"id":"job","overlap":"forbid"}`),
			prepare: func(t *testing.T, state *Store, now time.Time) {
				if _, err := state.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now.Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
			},
			decision: AdmissionDecision{Admit: true},
			wantCode: "overlap_hold",
		},
		{
			name:       "capacity",
			definition: []byte(`{"id":"job","overlap":"allow"}`),
			decision:   AdmissionDecision{HoldCode: "capacity_hold", HoldDetail: "maximum concurrent runs are already active"},
			wantCode:   "capacity_hold",
		},
		{
			name:       "disk",
			definition: []byte(`{"id":"job","overlap":"allow"}`),
			decision:   AdmissionDecision{HoldCode: "disk_hold", HoldDetail: "free disk is below required headroom"},
			wantCode:   "disk_hold",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			state := testStore(t)
			now := ts("2026-08-22T01:00:00Z")
			syncJob(t, state, now.Add(-time.Hour))
			if testCase.prepare != nil {
				testCase.prepare(t, state, now)
			}
			canary, err := state.CreateCanaryRun(context.Background(), "job", "rev1", testCase.definition, now)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := state.DecideAdmission(context.Background(), canary.ID, "owner", "dev1", 1.5, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
				return testCase.decision, nil
			})
			if err != nil || admitted {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
			got, err := state.GetRun(context.Background(), canary.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Trigger != "canary" || got.State != StateBlocked || got.ErrorCode != testCase.wantCode || got.InfrastructureResult != "not_started" || got.AgentResult != "not_started" {
				t.Fatalf("canary=%+v", got)
			}
			events, err := state.Events(context.Background(), canary.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 2 || events[1].Code != testCase.wantCode || events[1].ToState != StateBlocked {
				t.Fatalf("events=%+v", events)
			}
		})
	}
}

func TestDecideAdmissionPersistsProbeFailureAtomically(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	failing := func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{FailureCode: "capacity_probe_failed", FailureDetail: "statfs unavailable"}, nil
	}
	admitted, err := s.DecideAdmission(context.Background(), run.ID, "owner", "", 0, now, now.Add(time.Minute), failing)
	if err != nil || admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	got, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateFailed || got.ErrorCode != "capacity_probe_failed" || got.ErrorDetail != "statfs unavailable" ||
		got.InfrastructureResult != "failed" || got.AgentResult != "not_started" || got.TaskVerdict != "unverified" {
		t.Fatalf("run=%+v", got)
	}
	// The terminal run can no longer be claimed or refailed by a later decision.
	admitted, err = s.DecideAdmission(context.Background(), run.ID, "owner", "dev1", 1.5, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err != nil || admitted {
		t.Fatalf("second decision admitted=%t err=%v", admitted, err)
	}
	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != StateFailed || after.ErrorCode != "capacity_probe_failed" {
		t.Fatalf("terminal state changed: %+v", after)
	}
}

func TestLiveProvisioningClaimRequiresSameOwner(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := s.DecideAdmission(context.Background(), run.ID, "owner-a", "dev1", 1.5, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	if interrupted, err := s.InterruptExpiredProvisioning(context.Background(), run.ID, now.Add(30*time.Second)); err != nil || interrupted {
		t.Fatalf("live claim interrupted=%t err=%v", interrupted, err)
	}
	if saved, err := s.SaveProvisioningReceipt(context.Background(), run.ID, "owner-b", "w-wrong", "p", "branch", "/tmp/worktree", "agent", "", now.Add(30*time.Second)); err != nil || saved {
		t.Fatalf("wrong owner saved=%t err=%v", saved, err)
	}
	if renewed, err := s.RenewProvisioningClaim(context.Background(), run.ID, "owner-a", now.Add(30*time.Second), now.Add(90*time.Second)); err != nil || !renewed {
		t.Fatalf("renewed=%t err=%v", renewed, err)
	}
	if saved, err := s.SaveProvisioningReceipt(context.Background(), run.ID, "owner-a", "w1", "p1", "branch", "/tmp/worktree", "agent", "", now.Add(45*time.Second)); err != nil || !saved {
		t.Fatalf("owner receipt saved=%t err=%v", saved, err)
	}
	got, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateStarting || got.WorkspaceID != "w1" || got.ProvisioningOwner != "owner-a" || !got.ProvisioningLeaseUntil.Equal(now.Add(90*time.Second)) {
		t.Fatalf("receipt did not retain the live owner claim: %+v", got)
	}
	if confirmed, err := s.ConfirmStartingClaim(context.Background(), run.ID, "owner-b", "wrong owner", now.Add(50*time.Second)); err != nil || confirmed {
		t.Fatalf("wrong owner confirmed=%t err=%v", confirmed, err)
	}
	if confirmed, err := s.ConfirmStartingClaim(context.Background(), run.ID, "owner-a", "prompt accepted", now.Add(50*time.Second)); err != nil || !confirmed {
		t.Fatalf("owner confirmed=%t err=%v", confirmed, err)
	}
	got, err = s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateRunning || got.ProvisioningOwner != "" || !got.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("start confirmation did not release ownership: %+v", got)
	}
}

func TestProvisioningLeaseIsBounded(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DecideAdmission(context.Background(), run.ID, "owner", "dev1", 1.5, now, now.Add(MaxProvisioningLease+time.Nanosecond), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err == nil {
		t.Fatal("unbounded provisioning lease was accepted")
	}
}

func TestOpenMigratesDiskReserveColumnsWithDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-disk.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE jobs (
  id TEXT PRIMARY KEY, revision TEXT NOT NULL, definition BLOB NOT NULL,
  enabled INTEGER NOT NULL, paused INTEGER NOT NULL DEFAULT 0,
  completed INTEGER NOT NULL DEFAULT 0, cursor TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE occurrences (
  job_id TEXT NOT NULL, scheduled_for TEXT NOT NULL, outcome TEXT NOT NULL,
  run_id TEXT, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY(job_id, scheduled_for)
);
CREATE TABLE runs (
  id TEXT PRIMARY KEY, job_id TEXT NOT NULL, job_revision TEXT NOT NULL,
  definition BLOB NOT NULL, trigger TEXT NOT NULL, scheduled_for TEXT,
  state TEXT NOT NULL, infrastructure_result TEXT NOT NULL DEFAULT '',
  agent_result TEXT NOT NULL DEFAULT '', task_verdict TEXT NOT NULL DEFAULT 'unverified',
  accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '', pane_id TEXT NOT NULL DEFAULT '',
  branch TEXT NOT NULL DEFAULT '', worktree_path TEXT NOT NULL DEFAULT '',
  execution_mode TEXT NOT NULL DEFAULT 'agent', completion_marker TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '', error_detail TEXT NOT NULL DEFAULT '',
  provisioning_owner TEXT NOT NULL DEFAULT '',
  provisioning_lease_until INTEGER NOT NULL DEFAULT 0,
  unread INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(job_id) REFERENCES jobs(id)
);
INSERT INTO jobs(id,revision,definition,enabled,cursor,updated_at)
VALUES('job','rev7','{"id":"job","revision":7}',1,'2026-08-22T00:00:00Z','2026-08-22T00:00:00Z');
INSERT INTO occurrences(job_id,scheduled_for,outcome,created_at) VALUES
('job','2026-08-22T00:00:00Z','missed','2026-08-22T00:00:00.5Z'),
('job','2026-08-22T01:00:00Z','missed','2026-08-22T00:00:00Z');
INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at)
VALUES('legacy-run','job','rev7','{"id":"job","revision":7}','manual','accepted','2026-08-22T00:00:00.5Z','2026-08-22T00:00:00.5Z');
`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	legacy, err := s.GetRun(context.Background(), "legacy-run")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.DiskDevice != "" || legacy.DiskReserveGiB != 0 {
		t.Fatalf("legacy run backfilled device=%q reserve=%v; want empty and zero", legacy.DiskDevice, legacy.DiskReserveGiB)
	}
	if legacy.AcceptedUnixNano != ts("2026-08-22T00:00:00.5Z").UnixNano() {
		t.Fatalf("legacy accepted instant=%d", legacy.AcceptedUnixNano)
	}
	for key, wantInstant := range map[string]int64{
		"2026-08-22T00:00:00Z": ts("2026-08-22T00:00:00.5Z").UnixNano(),
		"2026-08-22T01:00:00Z": ts("2026-08-22T00:00:00Z").UnixNano(),
	} {
		var occurrenceInstant int64
		if err := s.db.QueryRow(`SELECT created_unix_nano FROM occurrences WHERE job_id='job' AND occurrence_key=?`, key).Scan(&occurrenceInstant); err != nil {
			t.Fatal(err)
		}
		if occurrenceInstant != wantInstant {
			t.Fatalf("legacy occurrence key=%q instant=%d want=%d", key, occurrenceInstant, wantInstant)
		}
	}
	job, err := s.Job(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if job.ConfigRevision != 7 || job.Revision != "rev7" {
		t.Fatalf("legacy job authority=%+v", job)
	}
	// A raw insert that omits the new columns must still satisfy the NOT NULL
	// constraints through the column-level defaults.
	if _, err := s.db.Exec(`INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at)
VALUES('defaulted-run','job','rev1','{"id":"job"}','manual','accepted','2026-08-22T02:00:00Z','2026-08-22T02:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	defaulted, err := s.GetRun(context.Background(), "defaulted-run")
	if err != nil {
		t.Fatal(err)
	}
	if defaulted.DiskDevice != "" || defaulted.DiskReserveGiB != 0 {
		t.Fatalf("defaulted run device=%q reserve=%v; want empty and zero", defaulted.DiskDevice, defaulted.DiskReserveGiB)
	}
}

func TestDecideAdmissionPersistsCandidateDeviceAndReserve(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	candidate, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := s.DecideAdmission(context.Background(), candidate.ID, "owner", "disk-42", 2.5, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	got, err := s.GetRun(context.Background(), candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateProvisioning || got.DiskDevice != "disk-42" || got.DiskReserveGiB != 2.5 ||
		got.ProvisioningOwner != "owner" || !got.ProvisioningLeaseUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("claim did not persist device and reserve atomically: %+v", got)
	}
	// The next decision must see the claimed reserve in its active rows without
	// probing any filesystem: the callback projects the persisted fields.
	second, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var seen *Run
	_, err = s.DecideAdmission(context.Background(), second.ID, "owner", "disk-42", 1.25, now.Add(2*time.Second), now.Add(2*time.Second).Add(time.Minute), func(active []Run) (AdmissionDecision, error) {
		for i := range active {
			if active[i].ID == candidate.ID {
				claimed := active[i]
				seen = &claimed
			}
		}
		return AdmissionDecision{}, nil // hold the candidate; only the projection matters
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen == nil {
		t.Fatal("claimed run was not projected in active rows")
	}
	if seen.DiskDevice != "disk-42" || seen.DiskReserveGiB != 2.5 {
		t.Fatalf("active row device=%q reserve=%v; want persisted claim values", seen.DiskDevice, seen.DiskReserveGiB)
	}
	if err := s.Finish(context.Background(), candidate.ID, StateProvisioning, StateSucceeded, "completed", "completed", "unverified", "", "", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	done, err := s.GetRun(context.Background(), candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.State != StateSucceeded {
		t.Fatalf("terminal behavior changed: %+v", done)
	}
}

func TestDecideAdmissionFailsClosedWithoutAdmissionLockRow(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM metadata WHERE key='admission_lock'`); err != nil {
		t.Fatal(err)
	}
	_, err = s.DecideAdmission(context.Background(), run.ID, "owner", "disk-42", 2.5, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err == nil {
		t.Fatal("missing admission lock row should fail closed with an error")
	}
	got, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Fail-closed means neither claimed nor terminally failed: the run stays accepted.
	if got.State != StateAccepted {
		t.Fatalf("run state=%s; want still accepted after fail-closed lock error", got.State)
	}
	if got.DiskDevice != "" || got.DiskReserveGiB != 0 {
		t.Fatalf("failed-closed run persisted claim fields: %+v", got)
	}
}

func TestConcurrentOpenSerializesLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE jobs (id TEXT PRIMARY KEY, revision TEXT NOT NULL, definition BLOB NOT NULL, enabled INTEGER NOT NULL, paused INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0, cursor TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE occurrences (job_id TEXT NOT NULL, scheduled_for TEXT NOT NULL, outcome TEXT NOT NULL, run_id TEXT, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, PRIMARY KEY(job_id, scheduled_for));
CREATE TABLE runs (id TEXT PRIMARY KEY, job_id TEXT NOT NULL, job_revision TEXT NOT NULL, definition BLOB NOT NULL, trigger TEXT NOT NULL, scheduled_for TEXT, state TEXT NOT NULL, infrastructure_result TEXT NOT NULL DEFAULT '', agent_result TEXT NOT NULL DEFAULT '', task_verdict TEXT NOT NULL DEFAULT 'unverified', accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL, workspace_id TEXT NOT NULL DEFAULT '', pane_id TEXT NOT NULL DEFAULT '', branch TEXT NOT NULL DEFAULT '', worktree_path TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '', error_detail TEXT NOT NULL DEFAULT '', unread INTEGER NOT NULL DEFAULT 1);
CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, from_state TEXT NOT NULL, to_state TEXT NOT NULL, at TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const rounds = 2
	for round := 0; round < rounds; round++ {
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				s, err := Open(path)
				if err == nil {
					err = s.Close()
				}
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent migration round %d failed: %v", round, err)
			}
		}
	}
}

func TestDecideAdmissionSerializesClaimsAcrossIndependentHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.sqlite3")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	now := ts("2026-08-22T01:00:00Z")
	if _, err := first.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	runs := make([]Run, 2)
	for i := range runs {
		run, err := second.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = run
	}
	check := func(active []Run) (AdmissionDecision, error) {
		if len(active) >= 1 {
			return AdmissionDecision{}, nil // one filesystem reserve is already committed
		}
		return AdmissionDecision{Admit: true}, nil
	}
	stores := []*Store{first, second}
	results := make([]bool, len(runs))
	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			admitted, err := stores[i].DecideAdmission(context.Background(), runs[i].ID, "owner", "disk-42", 2.5, now, now.Add(time.Minute), check)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = admitted
		}(i)
	}
	wg.Wait()
	admitted := 0
	for _, got := range results {
		if got {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted=%d results=%v; independent handles overspent the reserve", admitted, results)
	}
	states := map[string]int{}
	for _, run := range runs {
		got, err := first.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		states[got.State]++
		switch got.State {
		case StateProvisioning:
			if got.ProvisioningOwner != "owner" || !got.ProvisioningLeaseUntil.Equal(now.Add(time.Minute)) || got.DiskDevice != "disk-42" || got.DiskReserveGiB != 2.5 {
				t.Fatalf("serialized claim was not fully persisted: %+v", got)
			}
		case StateAccepted:
			// The losing candidate stays held, not terminally failed.
		default:
			t.Fatalf("spurious terminal state %s for run %s", got.State, got.ID)
		}
	}
	if states[StateProvisioning] != 1 || states[StateAccepted] != 1 {
		t.Fatalf("states=%v; want exactly one durable claim across handles", states)
	}
}

func TestOpenMigratesExistingDatabaseProvisioningClaimsFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE jobs (
  id TEXT PRIMARY KEY, revision TEXT NOT NULL, definition BLOB NOT NULL,
  enabled INTEGER NOT NULL, paused INTEGER NOT NULL DEFAULT 0,
  completed INTEGER NOT NULL DEFAULT 0, cursor TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE runs (
  id TEXT PRIMARY KEY, job_id TEXT NOT NULL, job_revision TEXT NOT NULL,
  definition BLOB NOT NULL, trigger TEXT NOT NULL, scheduled_for TEXT,
  state TEXT NOT NULL, infrastructure_result TEXT NOT NULL DEFAULT '',
  agent_result TEXT NOT NULL DEFAULT '', task_verdict TEXT NOT NULL DEFAULT 'unverified',
  accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '', pane_id TEXT NOT NULL DEFAULT '',
  branch TEXT NOT NULL DEFAULT '', worktree_path TEXT NOT NULL DEFAULT '',
  execution_mode TEXT NOT NULL DEFAULT 'agent', completion_marker TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '', error_detail TEXT NOT NULL DEFAULT '',
  unread INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(job_id) REFERENCES jobs(id)
);
INSERT INTO jobs(id,revision,definition,enabled,cursor,updated_at)
VALUES('job','rev1','{"id":"job"}',1,'2026-08-22T00:00:00Z','2026-08-22T00:00:00Z');
INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at)
VALUES('legacy-run','job','rev1','{"id":"job"}','manual','provisioning','2026-08-22T01:00:00Z','2026-08-22T01:00:00Z');
`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetRun(context.Background(), "legacy-run")
	if err != nil {
		t.Fatal(err)
	}
	if got.ProvisioningOwner != "" || !got.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("migrated claim=%+v", got)
	}
	interrupted, err := s.InterruptExpiredProvisioning(context.Background(), got.ID, ts("2026-08-22T01:01:00Z"))
	if err != nil || !interrupted {
		t.Fatalf("interrupted=%t err=%v", interrupted, err)
	}
	after, err := s.GetRun(context.Background(), got.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != StateInterrupted || after.ErrorCode != "restart_during_provisioning" {
		t.Fatalf("run=%+v", after)
	}
}

func claimTestVerifier(t *testing.T, s *Store, runID, owner string, now, lease time.Time) (string, string) {
	t.Helper()
	claim, receipt, err := s.ClaimVerifier(context.Background(), runID, owner, now, lease)
	if err != nil || claim == "" || receipt == "" {
		t.Fatalf("claim=%q receipt=%q err=%v", claim, receipt, err)
	}
	expected, err := s.VerifierReceiptPath(claim)
	if err != nil || receipt != expected || strings.Contains(filepath.Base(receipt), runID) {
		t.Fatalf("claim path=%q expected=%q err=%v", receipt, expected, err)
	}
	if _, err := os.Lstat(filepath.Dir(receipt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("claim touched receipt directory before ownership returned: %v", err)
	}
	return claim, receipt
}

func TestClaimVerifierUsesFreshGenerationAndSingleOwner(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateAccepted, StateRunning, "started", now); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateRunning, StateSettled, "settled", now); err != nil {
		t.Fatal(err)
	}
	lease := now.Add(time.Minute)
	claim, receipt := claimTestVerifier(t, s, run.ID, "owner-a", now, lease)
	got, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateVerifying || got.EffectOwner != "owner-a" || got.EffectClaim != claim || got.EffectReceipt != receipt || got.EffectKind != EffectVerifier || !got.EffectLeaseUntil.Equal(lease) {
		t.Fatalf("claimed run=%+v", got)
	}
	second, _, err := s.ClaimVerifier(context.Background(), run.ID, "owner-b", now, now.Add(time.Minute))
	if err != nil || second != "" {
		t.Fatalf("second claim=%q err=%v", second, err)
	}
	finished, err := s.FinishEffect(context.Background(), run.ID, StateVerifying, "owner-a", "wrong-generation", EffectVerifier, StateSucceeded, "completed", "completed", "passed", "", "", now)
	if err != nil || finished {
		t.Fatalf("stale finish=%t err=%v", finished, err)
	}
	finished, err = s.FinishEffect(context.Background(), run.ID, StateVerifying, "owner-a", claim, EffectVerifier, StateSucceeded, "completed", "completed", "passed", "", "verified", now)
	if err != nil || !finished {
		t.Fatalf("owner finish=%t err=%v", finished, err)
	}
}

func TestExpiredVerifierCannotBeReclaimedWithoutDurableResult(t *testing.T) {
	s := testStore(t)
	base := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, base.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), base)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateAccepted, StateRunning, "started", base); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateRunning, StateSettled, "settled", base); err != nil {
		t.Fatal(err)
	}
	claim, _ := claimTestVerifier(t, s, run.ID, "owner-a", base, base.Add(time.Minute))
	later := base.Add(2 * time.Minute)
	reclaimed, _, err := s.ClaimVerifier(context.Background(), run.ID, "owner-b", later, later.Add(time.Minute))
	if err != nil || reclaimed != "" {
		t.Fatalf("expired verifier was reclaimed: claim=%q err=%v", reclaimed, err)
	}
	finished, err := s.FinishExpiredVerifier(context.Background(), run.ID, StateInterrupted, "uncertain", "completed", "unverified", "restart_during_verifier", "no durable result", later)
	if err != nil || !finished {
		t.Fatalf("interrupted=%t err=%v", finished, err)
	}
	stale, err := s.FinishEffect(context.Background(), run.ID, StateVerifying, "owner-a", claim, EffectVerifier, StateSucceeded, "completed", "completed", "passed", "", "late", later)
	if err != nil || stale {
		t.Fatalf("stale finish=%t err=%v", stale, err)
	}
}

func TestVerifierCannotStealExpiredWorkspaceClose(t *testing.T) {
	s := testStore(t)
	base := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, base.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), base)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateAccepted, StateRunning, "started", base); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateRunning, StateSettled, "settled", base); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimEffect(context.Background(), run.ID, StateSettled, EffectWorkspaceClose, "closer", base, base.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("close claim=%q err=%v", claim, err)
	}
	verifierClaim, _, err := s.ClaimVerifier(context.Background(), run.ID, "verifier", base.Add(2*time.Minute), base.Add(3*time.Minute))
	if err != nil || verifierClaim != "" {
		t.Fatalf("verifier stole close claim=%q err=%v", verifierClaim, err)
	}
}

func TestExpiredCommandStartCannotOverrideWorkspaceCloseOwner(t *testing.T) {
	s := testStore(t)
	base := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, base.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), base)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := s.DecideAdmission(context.Background(), run.ID, "starter", "1", 1.25, base, base.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	saved, err := s.SaveProvisioningReceipt(context.Background(), run.ID, "starter", "w1", "p1", "auto/test", "/tmp/test", "command", "marker", base.Add(30*time.Second))
	if err != nil || !saved {
		t.Fatalf("saved=%t err=%v", saved, err)
	}
	later := base.Add(2 * time.Minute)
	claim, err := s.ClaimEffect(context.Background(), run.ID, StateStarting, EffectWorkspaceClose, "closer", later, later.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("close claim=%q err=%v", claim, err)
	}
	confirmed, err := s.ConfirmExpiredCommandStart(context.Background(), run.ID, "completion marker found", later)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed {
		t.Fatal("expired command start overrode a live workspace-close owner")
	}
}

func TestWorkspaceCloseGenerationFencesSameOwnerABA(t *testing.T) {
	s := testStore(t)
	base := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, base.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), base)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(context.Background(), run.ID, StateAccepted, StateRunning, "started", base); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimEffect(context.Background(), run.ID, StateRunning, EffectWorkspaceClose, "same-owner", base, base.Add(time.Minute))
	if err != nil || first == "" {
		t.Fatalf("first claim=%q err=%v", first, err)
	}
	later := base.Add(2 * time.Minute)
	second, err := s.ReclaimEffect(context.Background(), run.ID, StateRunning, EffectWorkspaceClose, "same-owner", later, later.Add(time.Minute))
	if err != nil || second == "" || second == first {
		t.Fatalf("second claim=%q first=%q err=%v", second, first, err)
	}
	stale, err := s.FinishEffect(context.Background(), run.ID, StateRunning, "same-owner", first, EffectWorkspaceClose, StateCancelled, "completed", "cancelled", "unverified", "cancelled", "stale", later)
	if err != nil || stale {
		t.Fatalf("stale finish=%t err=%v", stale, err)
	}
	finished, err := s.FinishEffect(context.Background(), run.ID, StateRunning, "same-owner", second, EffectWorkspaceClose, StateCancelled, "completed", "cancelled", "unverified", "cancelled", "current", later)
	if err != nil || !finished {
		t.Fatalf("current finish=%t err=%v", finished, err)
	}
}

func TestOperationalEventAtomicallyReopensUnreadInbox(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T01:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkRead(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRunEvent(context.Background(), run.ID, "workspace_close_failed", "close unavailable", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.Events(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Unread || len(events) == 0 || events[len(events)-1].Code != "workspace_close_failed" {
		t.Fatalf("run=%+v events=%+v", got, events)
	}
	if err := s.MarkRead(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRunEvent(context.Background(), run.ID, "workspace_close_failed", "close unavailable", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterEvents, err := s.Events(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Unread || len(afterEvents) != len(events) {
		t.Fatalf("duplicate evidence reopened inbox or grew events: run=%+v before=%d after=%d", after, len(events), len(afterEvents))
	}
}

func eventRequest(id string, now time.Time) AcceptRequest {
	return AcceptRequest{
		JobID: "job", JobConfigRevision: 1, JobRevision: "rev1", JobEnabled: true, OccurrenceKey: "event:" + id,
		Definition: []byte(`{"id":"job"}`), Trigger: "event", ScheduledFor: now,
		Overlap: "forbid", DayStart: now.Add(-time.Hour), MaxRunsPerDay: 10, Now: now,
	}
}

func TestConcurrentEventEnqueueIsIdempotentAcrossIndependentHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared-events.sqlite3")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	now := ts("2026-08-23T02:00:00Z")
	if _, err := first.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now); err != nil {
		t.Fatal(err)
	}

	stores := []*Store{first, second}
	results := make([]AcceptResult, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range stores {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results[index], errs[index] = stores[index].EnqueueEvent(context.Background(), eventRequest("health-20260823", now))
		}(index)
	}
	close(start)
	wait.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent enqueue leaked an error: %v", err)
		}
	}
	if results[0].Run == nil || results[1].Run == nil || results[0].Run.ID != results[1].Run.ID || results[0].Outcome != "accepted" || results[1].Outcome != "accepted" {
		t.Fatalf("results=%+v", results)
	}
	inserted := 0
	for _, result := range results {
		if result.Inserted {
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("inserted=%d results=%+v", inserted, results)
	}
	runs, err := first.ListRuns(context.Background(), "job", 10)
	if err != nil || len(runs) != 1 || runs[0].Trigger != "event" {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestEventEnqueueRefusesDisabledAndPausedAuthorityBeforeOccurrence(t *testing.T) {
	cases := []struct {
		name string
		want error
		set  func(*Store, time.Time) error
	}{
		{name: "disabled", want: ErrJobDisabled, set: func(s *Store, now time.Time) error {
			_, err := s.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), false, now)
			return err
		}},
		{name: "job paused", want: ErrJobPaused, set: func(s *Store, now time.Time) error {
			if _, err := s.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now); err != nil {
				return err
			}
			return s.SetPaused(context.Background(), "job", true)
		}},
		{name: "globally paused", want: ErrGlobalPaused, set: func(s *Store, now time.Time) error {
			if _, err := s.SyncJob(context.Background(), "job", "rev1", []byte(`{"id":"job"}`), true, now); err != nil {
				return err
			}
			return s.SetGlobalPaused(context.Background(), true)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			now := ts("2026-08-23T02:00:00Z")
			if err := tc.set(s, now); err != nil {
				t.Fatal(err)
			}
			req := eventRequest("blocked", now)
			if errors.Is(tc.want, ErrJobDisabled) {
				req.JobEnabled = false
			}
			if _, err := s.EnqueueEvent(context.Background(), req); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
			runs, err := s.ListRuns(context.Background(), "job", 10)
			if err != nil || len(runs) != 0 {
				t.Fatalf("runs=%+v err=%v", runs, err)
			}
			last, err := s.LastJobResult(context.Background(), "job")
			if err != nil || last != "" {
				t.Fatalf("last=%q err=%v; refusal created an occurrence", last, err)
			}
		})
	}
}

func TestEventAuthorityLowerRevisionDuplicateReturnsOriginalButNewIDIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event-authority.sqlite3")
	newer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	stale, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	now := ts("2026-08-23T02:00:00Z")

	enabled := eventRequest("health", now)
	enabled.JobConfigRevision = 1
	enabled.JobRevision = "snapshot-old-enabled"
	enabled.JobEnabled = true
	enabled.Definition = []byte(`{"id":"job","revision":1,"enabled":true}`)
	accepted, err := stale.EnqueueEvent(context.Background(), enabled)
	if err != nil || accepted.Run == nil {
		t.Fatalf("initial enabled event=%+v err=%v", accepted, err)
	}

	disabled := eventRequest("health", now.Add(time.Second))
	disabled.JobConfigRevision = 2
	disabled.JobRevision = "snapshot-new-disabled"
	disabled.JobEnabled = false
	disabled.Definition = []byte(`{"id":"job","revision":2,"enabled":false}`)
	duplicate, err := newer.EnqueueEvent(context.Background(), disabled)
	if err != nil || duplicate.Run == nil || duplicate.Run.ID != accepted.Run.ID {
		t.Fatalf("new disabled duplicate=%+v err=%v", duplicate, err)
	}

	staleDuplicate := enabled
	staleDuplicate.ScheduledFor = now.Add(2 * time.Second)
	staleDuplicate.Now = now.Add(2 * time.Second)
	retried, err := stale.EnqueueEvent(context.Background(), staleDuplicate)
	if err != nil || retried.Run == nil || retried.Run.ID != accepted.Run.ID || retried.Run.JobRevision != accepted.Run.JobRevision || string(retried.Run.Definition) != string(accepted.Run.Definition) || retried.Inserted {
		t.Fatalf("stale exact retry=%+v err=%v", retried, err)
	}

	staleEvent := staleDuplicate
	staleEvent.OccurrenceKey = "event:later-health"
	staleEvent.ScheduledFor = now.Add(3 * time.Second)
	staleEvent.Now = now.Add(3 * time.Second)
	if _, err := stale.EnqueueEvent(context.Background(), staleEvent); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale different event authority error=%v", err)
	}
	job, err := newer.Job(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if job.ConfigRevision != 2 || job.Revision != disabled.JobRevision || job.Enabled {
		t.Fatalf("stale caller changed saved authority: %+v", job)
	}
	runs, err := newer.ListRuns(context.Background(), "job", 10)
	if err != nil || len(runs) != 1 || runs[0].ID != accepted.Run.ID {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestEventAuthorityEqualRevisionConflictDuplicateReturnsOriginalButNewIDIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "event-authority.sqlite3")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	now := ts("2026-08-23T02:00:00Z")

	original := eventRequest("original", now)
	original.JobConfigRevision = 3
	original.JobRevision = "snapshot-a"
	original.JobEnabled = true
	original.Definition = []byte(`{"id":"job","revision":3,"enabled":true,"prompt":"a"}`)
	accepted, err := first.EnqueueEvent(context.Background(), original)
	if err != nil || accepted.Run == nil {
		t.Fatalf("original authority=%+v err=%v", accepted, err)
	}
	conflict := eventRequest("original", now.Add(time.Second))
	conflict.JobConfigRevision = 3
	conflict.JobRevision = "snapshot-b"
	conflict.JobEnabled = true
	conflict.Definition = []byte(`{"id":"job","revision":3,"enabled":true,"prompt":"b"}`)
	retried, err := second.EnqueueEvent(context.Background(), conflict)
	if err != nil || retried.Run == nil || retried.Run.ID != accepted.Run.ID || retried.Run.JobRevision != accepted.Run.JobRevision || string(retried.Run.Definition) != string(accepted.Run.Definition) || retried.Inserted {
		t.Fatalf("equal-revision exact retry=%+v err=%v", retried, err)
	}
	conflict.OccurrenceKey = "event:conflict"
	conflict.ScheduledFor = now.Add(2 * time.Second)
	conflict.Now = now.Add(2 * time.Second)
	if _, err := second.EnqueueEvent(context.Background(), conflict); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("equal-revision different event error=%v", err)
	}
	job, err := first.Job(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if job.ConfigRevision != 3 || job.Revision != original.JobRevision || !job.Enabled {
		t.Fatalf("conflicting snapshot changed saved authority: %+v", job)
	}
	runs, err := first.ListRuns(context.Background(), "job", 10)
	if err != nil || len(runs) != 1 || runs[0].ID != accepted.Run.ID {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestEventDailyLimitCountsFractionalInstantAfterLocalMidnight(t *testing.T) {
	s := testStore(t)
	dayStart := ts("2026-08-23T00:00:00+08:00")
	firstAt := dayStart.Add(500 * time.Millisecond)
	secondAt := dayStart.Add(600 * time.Millisecond)
	first := eventRequest("first", firstAt)
	first.DayStart = dayStart
	first.DayEnd = dayStart.Add(24 * time.Hour)
	first.Overlap = "allow"
	first.MaxRunsPerDay = 1
	firstResult, err := s.EnqueueEvent(context.Background(), first)
	if err != nil || firstResult.Run == nil {
		t.Fatalf("first=%+v err=%v", firstResult, err)
	}
	if firstResult.Run.AcceptedUnixNano != firstAt.UnixNano() {
		t.Fatalf("accepted instant=%d want=%d", firstResult.Run.AcceptedUnixNano, firstAt.UnixNano())
	}
	second := eventRequest("second", secondAt)
	second.DayStart = dayStart
	second.DayEnd = dayStart.Add(24 * time.Hour)
	second.Overlap = "allow"
	second.MaxRunsPerDay = 1
	secondResult, err := s.EnqueueEvent(context.Background(), second)
	if err != nil || secondResult.Outcome != "skipped_limit" || secondResult.Run != nil {
		t.Fatalf("second=%+v err=%v", secondResult, err)
	}
}

func TestEventEnqueuePreservesDailyLimitAndOverlap(t *testing.T) {
	t.Run("daily limit", func(t *testing.T) {
		s := testStore(t)
		now := ts("2026-08-23T02:00:00Z")
		syncJob(t, s, now)
		first := eventRequest("first", now)
		first.Overlap = "allow"
		first.MaxRunsPerDay = 1
		if result, err := s.EnqueueEvent(context.Background(), first); err != nil || result.Run == nil {
			t.Fatalf("first=%+v err=%v", result, err)
		}
		second := eventRequest("second", now)
		second.Overlap = "allow"
		second.MaxRunsPerDay = 1
		if result, err := s.EnqueueEvent(context.Background(), second); err != nil || result.Outcome != "skipped_limit" || result.Run != nil {
			t.Fatalf("second=%+v err=%v", result, err)
		}
	})
	t.Run("overlap", func(t *testing.T) {
		s := testStore(t)
		now := ts("2026-08-23T02:00:00Z")
		syncJob(t, s, now)
		if result, err := s.EnqueueEvent(context.Background(), eventRequest("first", now)); err != nil || result.Run == nil {
			t.Fatalf("first=%+v err=%v", result, err)
		}
		if result, err := s.EnqueueEvent(context.Background(), eventRequest("second", now)); err != nil || result.Outcome != "skipped_overlap" || result.Run != nil {
			t.Fatalf("second=%+v err=%v", result, err)
		}
	})
	t.Run("distinct identities may share an instant", func(t *testing.T) {
		s := testStore(t)
		now := ts("2026-08-23T02:00:00Z")
		syncJob(t, s, now)
		for _, id := range []string{"first", "second"} {
			req := eventRequest(id, now)
			req.Overlap = "allow"
			if result, err := s.EnqueueEvent(context.Background(), req); err != nil || result.Run == nil {
				t.Fatalf("%s result=%+v err=%v", id, result, err)
			}
		}
		runs, err := s.ListRuns(context.Background(), "job", 10)
		if err != nil || len(runs) != 2 {
			t.Fatalf("runs=%+v err=%v", runs, err)
		}
	})
}

func scheduledAuthorityRequest(now time.Time, configRevision int, hash string, definition []byte) AcceptRequest {
	return AcceptRequest{
		JobID: "job", JobConfigRevision: configRevision, JobRevision: hash, JobEnabled: true,
		OccurrenceKey: "cron:" + now.Format(time.RFC3339), Definition: definition, Trigger: "cron", ScheduledFor: now,
		Overlap: "forbid", DayStart: now.Add(-time.Hour), MaxRunsPerDay: 10, Now: now,
	}
}

func TestScheduledWritesRejectStaleAuthority(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := ts("2026-08-23T02:00:00Z")
	v1 := []byte(`{"id":"job","revision":1}`)
	v2 := []byte(`{"id":"job","revision":2}`)
	if _, err := s.SyncJobAuthority(ctx, "job", 1, "hash-v1", v1, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SyncJobAuthority(ctx, "job", 2, "hash-v2", v2, true, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	req := scheduledAuthorityRequest(now.Add(2*time.Minute), 1, "hash-v1", v1)
	if _, err := s.AcceptScheduledOccurrence(ctx, req); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale acceptance error=%v", err)
	}
	if _, err := s.RecordOccurrenceIfAuthority(ctx, req, "missed", "stale"); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale occurrence error=%v", err)
	}
	if err := s.SetCursorIfAuthority(ctx, "job", 1, "hash-v1", now.Add(3*time.Minute), true); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale cursor error=%v", err)
	}
	if err := s.SetCompletedIfAuthority(ctx, "job", 1, "hash-v1", true); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale completion error=%v", err)
	}
	state, err := s.Job(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if state.ConfigRevision != 2 || state.Completed || !state.Cursor.Equal(now) {
		t.Fatalf("newer authority was changed: %+v", state)
	}
	newerCursor := now.Add(5 * time.Minute)
	if err := s.SetCursorIfAuthority(ctx, "job", 2, "hash-v2", newerCursor, true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetCursorIfAuthority(ctx, "job", 2, "hash-v2", now.Add(4*time.Minute), true); err != nil {
		t.Fatal(err)
	}
	state, err = s.Job(ctx, "job")
	if err != nil || !state.Cursor.Equal(newerCursor) {
		t.Fatalf("same-revision cursor regressed: state=%+v err=%v", state, err)
	}
	runs, err := s.ListRuns(ctx, "job", 10)
	if err != nil || len(runs) != 0 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestAuthorityFencedOccurrencePreservesLastResultOrdering(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := ts("2026-08-23T02:00:00Z")
	definition := []byte(`{"id":"job","revision":1}`)
	if _, err := s.SyncJobAuthority(ctx, "job", 1, "hash-v1", definition, true, now); err != nil {
		t.Fatal(err)
	}
	first := scheduledAuthorityRequest(now, 1, "hash-v1", definition)
	if result, err := s.AcceptScheduledOccurrence(ctx, first); err != nil || result.Outcome != "accepted" {
		t.Fatalf("first=%+v err=%v", result, err)
	}
	later := scheduledAuthorityRequest(now.Add(time.Minute), 1, "hash-v1", definition)
	if inserted, err := s.RecordOccurrenceIfAuthority(ctx, later, "skipped_unchanged", "no source change"); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	last, err := s.LastJobResult(ctx, "job")
	if err != nil || last != "skipped_unchanged" {
		t.Fatalf("last=%q err=%v", last, err)
	}
}

func TestAdmissionRechecksPauseAuthorityBeforeClaim(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause func(*Store) error
	}{
		{name: "job", pause: func(s *Store) error { return s.SetPaused(context.Background(), "job", true) }},
		{name: "global", pause: func(s *Store) error { return s.SetGlobalPaused(context.Background(), true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			now := ts("2026-08-23T02:00:00Z")
			definition := []byte(`{"id":"job","revision":1}`)
			if _, err := s.SyncJobAuthority(context.Background(), "job", 1, "hash-v1", definition, true, now); err != nil {
				t.Fatal(err)
			}
			run, err := s.CreateManualRun(context.Background(), "job", "hash-v1", definition, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.pause(s); err != nil {
				t.Fatal(err)
			}
			called := false
			admitted, err := s.DecideAdmission(context.Background(), run.ID, "owner", "1", 1.25, now, now.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
				called = true
				return AdmissionDecision{Admit: true}, nil
			})
			if err != nil || admitted || called {
				t.Fatalf("admitted=%v callback=%v err=%v", admitted, called, err)
			}
			got, err := s.GetRun(context.Background(), run.ID)
			if err != nil || got.State != StateAccepted || got.ProvisioningOwner != "" {
				t.Fatalf("run=%+v err=%v", got, err)
			}
		})
	}
}

func TestExpiredProvisioningInterruptionCannotBypassPartialReceipt(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	base := ts("2026-08-23T02:00:00Z")
	definition := []byte(`{"id":"job","revision":1}`)
	if _, err := s.SyncJobAuthority(ctx, "job", 1, "hash-v1", definition, true, base); err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateManualRun(ctx, "job", "hash-v1", definition, base)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := s.DecideAdmission(ctx, run.ID, "owner", "1", 1.25, base, base.Add(time.Minute), func([]Run) (AdmissionDecision, error) {
		return AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%v err=%v", admitted, err)
	}
	saved, err := s.SavePartialProvisioningReceipt(ctx, run.ID, "owner", "w-partial", "p-partial", "auto/job/run", base.Add(30*time.Second))
	if err != nil || !saved {
		t.Fatalf("saved=%v err=%v", saved, err)
	}
	interrupted, err := s.InterruptExpiredProvisioning(ctx, run.ID, base.Add(2*time.Minute))
	if err != nil || interrupted {
		t.Fatalf("partial receipt was terminalized: interrupted=%v err=%v", interrupted, err)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil || got.State != StateProvisioning || got.WorkspaceID != "w-partial" {
		t.Fatalf("run=%+v err=%v", got, err)
	}
}

func TestManualRunAuthorityRefusesDisabledAndPausedJobs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*Store) error
		want    error
	}{
		{name: "disabled", want: ErrJobDisabled, prepare: func(*Store) error { return nil }},
		{name: "paused", want: ErrJobPaused, prepare: func(s *Store) error { return s.SetPaused(context.Background(), "job", true) }},
		{name: "global", want: ErrGlobalPaused, prepare: func(s *Store) error { return s.SetGlobalPaused(context.Background(), true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			now := ts("2026-08-23T02:00:00Z")
			definition := []byte(`{"id":"job","revision":1}`)
			enabled := tc.name != "disabled"
			if _, err := s.SyncJobAuthority(context.Background(), "job", 1, "hash-v1", definition, enabled, now); err != nil {
				t.Fatal(err)
			}
			if err := tc.prepare(s); err != nil {
				t.Fatal(err)
			}
			_, err := s.CreateManualRunIfAuthority(context.Background(), scheduledAuthorityRequest(now, 1, "hash-v1", definition))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			runs, listErr := s.ListRuns(context.Background(), "job", 10)
			if listErr != nil || len(runs) != 0 {
				t.Fatalf("runs=%+v err=%v", runs, listErr)
			}
		})
	}
}

// seedUnreadTerminalRun inserts one finished run for job "job" in the given
// terminal state; it stays unread unless read is true.
func seedUnreadTerminalRun(t *testing.T, s *Store, state string, read bool) {
	t.Helper()
	ctx := context.Background()
	now := ts("2026-08-23T01:00:00Z")
	run, err := s.CreateManualRun(ctx, "job", "hash-v1", []byte(`{"id":"job","revision":1}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if IsTerminalState(state) {
		if err := s.Finish(ctx, run.ID, StateAccepted, state, "completed", "completed", "unverified", "", "seeded", now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	} else if err := s.Transition(ctx, run.ID, StateAccepted, state, "seeded nonterminal", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if read {
		if err := s.MarkRead(ctx, run.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func syncGuardJob(t *testing.T, s *Store) {
	t.Helper()
	now := ts("2026-08-23T00:00:00Z")
	if _, err := s.SyncJobAuthority(context.Background(), "job", 1, "hash-v1", []byte(`{"id":"job","revision":1}`), true, now); err != nil {
		t.Fatal(err)
	}
}

func guardOccurrences(s *Store) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM occurrences WHERE job_id='job'`).Scan(&n)
	return n, err
}

func TestUnreadGuardCountsOnlyUnreadTerminalRuns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		steps []struct {
			state string
			read  bool
		}
		wantTripped bool
		wantOutcome string
	}{
		{name: "nonterminal unread runs never count", limit: 1, steps: []struct {
			state string
			read  bool
		}{{StateAccepted, false}, {StateRunning, false}}, wantTripped: false, wantOutcome: "skipped_overlap"},
		{name: "read terminal runs never count", limit: 1, steps: []struct {
			state string
			read  bool
		}{{StateSucceeded, true}, {StateFailed, true}}, wantTripped: false, wantOutcome: "accepted"},
		{name: "unread terminal runs count exactly", limit: 2, steps: []struct {
			state string
			read  bool
		}{{StateSucceeded, false}, {StateFailed, false}}, wantTripped: true, wantOutcome: "paused_unread_limit"},
		{name: "below the limit admits", limit: 3, steps: []struct {
			state string
			read  bool
		}{{StateSucceeded, false}, {StateFailed, false}}, wantTripped: false, wantOutcome: "accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			syncGuardJob(t, s)
			for _, step := range tc.steps {
				seedUnreadTerminalRun(t, s, step.state, step.read)
			}
			now := ts("2026-08-23T03:00:00Z")
			req := scheduledAuthorityRequest(now, 1, "hash-v1", []byte(`{"id":"job","revision":1}`))
			req.MaxUnreadTerminalRuns = tc.limit
			result, err := s.AcceptScheduledOccurrence(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.Job(context.Background(), "job")
			if err != nil {
				t.Fatal(err)
			}
			occurrences, occErr := guardOccurrences(s)
			if occErr != nil {
				t.Fatal(occErr)
			}
			if tc.wantTripped {
				if result.Run != nil || result.Inserted || result.Outcome != "paused_unread_limit" {
					t.Fatalf("tripped result=%+v", result)
				}
				if !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns || job.PauseAt.IsZero() {
					t.Fatalf("guard pause state=%+v", job)
				}
				if occurrences != 0 {
					t.Fatalf("tripped guard recorded %d occurrences", occurrences)
				}
			} else {
				if result.Outcome != tc.wantOutcome || job.Paused {
					t.Fatalf("outcome=%q want=%q job=%+v", result.Outcome, tc.wantOutcome, job)
				}
			}
		})
	}
}

func TestUnreadGuardIsIdempotentAndDoesNotDuplicateEffects(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	syncGuardJob(t, s)
	seedUnreadTerminalRun(t, s, StateSucceeded, false)
	now := ts("2026-08-23T03:00:00Z")
	definition := []byte(`{"id":"job","revision":1}`)
	for i := 0; i < 3; i++ {
		req := scheduledAuthorityRequest(now.Add(time.Duration(i)*time.Minute), 1, "hash-v1", definition)
		req.MaxUnreadTerminalRuns = 1
		if i == 0 {
			result, err := s.AcceptScheduledOccurrence(ctx, req)
			if err != nil || result.Outcome != "paused_unread_limit" {
				t.Fatalf("first evaluation result=%+v err=%v", result, err)
			}
		} else {
			// The job is already durably paused; repeated evaluation refuses at
			// the authority fence without pausing twice or admitting anything.
			if _, err := s.AcceptScheduledOccurrence(ctx, req); !errors.Is(err, ErrJobPaused) {
				t.Fatalf("repeat %d error=%v want ErrJobPaused", i, err)
			}
		}
	}
	job, err := s.Job(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns {
		t.Fatalf("job=%+v", job)
	}
	runs, err := s.ListRuns(ctx, "job", 20)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	occurrences, occErr := guardOccurrences(s)
	if occErr != nil || occurrences != 0 {
		t.Fatalf("occurrences=%d err=%v", occurrences, occErr)
	}
}

func TestUnreadGuardStaleAuthorityCannotPauseOrAdmit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := ts("2026-08-23T02:00:00Z")
	v1 := []byte(`{"id":"job","revision":1}`)
	v2 := []byte(`{"id":"job","revision":2}`)
	if _, err := s.SyncJobAuthority(ctx, "job", 1, "hash-v1", v1, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SyncJobAuthority(ctx, "job", 2, "hash-v2", v2, true, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	seedUnreadTerminalRun(t, s, StateSucceeded, false)

	stale := scheduledAuthorityRequest(now.Add(2*time.Minute), 1, "hash-v1", v1)
	stale.MaxUnreadTerminalRuns = 1
	if _, err := s.AcceptScheduledOccurrence(ctx, stale); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale scheduled error=%v", err)
	}
	// Stale manual runs use the same authority fence before the guard.
	staleManual := scheduledAuthorityRequest(now.Add(3*time.Minute), 1, "hash-v1", v1)
	staleManual.MaxUnreadTerminalRuns = 1
	if _, err := s.CreateManualRunIfAuthority(ctx, staleManual); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale manual error=%v", err)
	}
	// Stale event delivery with a new identity refuses at synchronization.
	staleEvent := scheduledAuthorityRequest(now.Add(4*time.Minute), 1, "hash-v1", v1)
	staleEvent.Trigger = "event"
	staleEvent.OccurrenceKey = "event:stale-guard"
	staleEvent.MaxUnreadTerminalRuns = 1
	if _, err := s.EnqueueEvent(ctx, staleEvent); !errors.Is(err, ErrJobRevisionChanged) {
		t.Fatalf("stale event error=%v", err)
	}

	job, err := s.Job(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if job.Paused || job.PauseReason != "" || !job.PauseAt.IsZero() {
		t.Fatalf("stale authority paused the job: %+v", job)
	}
	occurrences, occErr := guardOccurrences(s)
	if occErr != nil || occurrences != 0 {
		t.Fatalf("stale authority recorded %d occurrences err=%v", occurrences, occErr)
	}
}

func TestUnreadGuardPausesEventAndManualAdmission(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := ts("2026-08-23T02:00:00Z")
	definition := []byte(`{"id":"job","revision":1}`)
	if _, err := s.SyncJobAuthority(ctx, "job", 1, "hash-v1", definition, true, now); err != nil {
		t.Fatal(err)
	}
	seedUnreadTerminalRun(t, s, StateSucceeded, false)

	eventReq := scheduledAuthorityRequest(now.Add(time.Minute), 1, "hash-v1", definition)
	eventReq.Trigger = "event"
	eventReq.OccurrenceKey = "event:guard"
	eventReq.MaxUnreadTerminalRuns = 1
	result, err := s.EnqueueEvent(ctx, eventReq)
	if err != nil {
		t.Fatal(err)
	}
	if result.Run != nil || result.Inserted || result.Outcome != "paused_unread_limit" {
		t.Fatalf("event result=%+v", result)
	}
	job, err := s.Job(ctx, "job")
	if err != nil || !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns {
		t.Fatalf("event guard job=%+v err=%v", job, err)
	}
	if err := s.SetPaused(ctx, "job", false); err != nil {
		t.Fatal(err)
	}

	manualReq := scheduledAuthorityRequest(now.Add(2*time.Minute), 1, "hash-v1", definition)
	manualReq.Trigger = "manual"
	manualReq.MaxUnreadTerminalRuns = 1
	if _, err := s.CreateManualRunIfAuthority(ctx, manualReq); !errors.Is(err, ErrJobUnreadPaused) {
		t.Fatalf("manual error=%v", err)
	}
	job, err = s.Job(ctx, "job")
	if err != nil || !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns {
		t.Fatalf("manual guard job=%+v err=%v", job, err)
	}
	runs, listErr := s.ListRuns(ctx, "job", 20)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
}

func TestPauseReasonsManualVersusGuardAndResumeClears(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	syncGuardJob(t, s)
	if err := s.SetPaused(ctx, "job", true); err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(ctx, "job")
	if err != nil || !job.Paused || job.PauseReason != PauseReasonManual || job.PauseAt.IsZero() {
		t.Fatalf("manual pause job=%+v err=%v", job, err)
	}
	if err := s.SetPaused(ctx, "job", false); err != nil {
		t.Fatal(err)
	}
	job, err = s.Job(ctx, "job")
	if err != nil || job.Paused || job.PauseReason != "" || !job.PauseAt.IsZero() {
		t.Fatalf("resume did not clear pause job=%+v err=%v", job, err)
	}

	// A guard pause records its own reason; explicit resume clears it too.
	seedUnreadTerminalRun(t, s, StateSucceeded, false)
	now := ts("2026-08-23T03:00:00Z")
	req := scheduledAuthorityRequest(now, 1, "hash-v1", []byte(`{"id":"job","revision":1}`))
	req.MaxUnreadTerminalRuns = 1
	if _, err := s.AcceptScheduledOccurrence(ctx, req); err != nil {
		t.Fatal(err)
	}
	job, err = s.Job(ctx, "job")
	if err != nil || !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns {
		t.Fatalf("guard pause job=%+v err=%v", job, err)
	}
	if err := s.SetPaused(ctx, "job", false); err != nil {
		t.Fatal(err)
	}
	job, err = s.Job(ctx, "job")
	if err != nil || job.Paused || job.PauseReason != "" || !job.PauseAt.IsZero() {
		t.Fatalf("resume after guard job=%+v err=%v", job, err)
	}
}

func TestMarkingRunsReadDoesNotResumeGuardPausedJob(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	syncGuardJob(t, s)
	seedUnreadTerminalRun(t, s, StateSucceeded, false)
	now := ts("2026-08-23T03:00:00Z")
	req := scheduledAuthorityRequest(now, 1, "hash-v1", []byte(`{"id":"job","revision":1}`))
	req.MaxUnreadTerminalRuns = 1
	if _, err := s.AcceptScheduledOccurrence(ctx, req); err != nil {
		t.Fatal(err)
	}
	runs, err := s.ListRuns(ctx, "job", 5)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if err := s.MarkRead(ctx, runs[0].ID); err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(ctx, "job")
	if err != nil || !job.Paused || job.PauseReason != PauseReasonUnreadTerminalRuns {
		t.Fatalf("reading runs auto-resumed the job: %+v err=%v", job, err)
	}
	// After the operator reads everything, an explicit resume admits again.
	if err := s.SetPaused(ctx, "job", false); err != nil {
		t.Fatal(err)
	}
	later := scheduledAuthorityRequest(now.Add(time.Minute), 1, "hash-v1", []byte(`{"id":"job","revision":1}`))
	later.MaxUnreadTerminalRuns = 1
	result, err := s.AcceptScheduledOccurrence(ctx, later)
	if err != nil || result.Run == nil || result.Outcome != "accepted" {
		t.Fatalf("post-resume result=%+v err=%v", result, err)
	}
}

func TestOpenMigratesPauseColumnsPreservingHistoricalRowsAndStates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-pause.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE jobs (
  id TEXT PRIMARY KEY, revision TEXT NOT NULL, definition BLOB NOT NULL,
  enabled INTEGER NOT NULL, paused INTEGER NOT NULL DEFAULT 0,
  completed INTEGER NOT NULL DEFAULT 0, cursor TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE runs (
  id TEXT PRIMARY KEY, job_id TEXT NOT NULL, job_revision TEXT NOT NULL,
  definition BLOB NOT NULL, trigger TEXT NOT NULL, scheduled_for TEXT,
  state TEXT NOT NULL, infrastructure_result TEXT NOT NULL DEFAULT '',
  agent_result TEXT NOT NULL DEFAULT '', task_verdict TEXT NOT NULL DEFAULT 'unverified',
  accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '', pane_id TEXT NOT NULL DEFAULT '',
  branch TEXT NOT NULL DEFAULT '', worktree_path TEXT NOT NULL DEFAULT '',
  execution_mode TEXT NOT NULL DEFAULT 'agent', completion_marker TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '', error_detail TEXT NOT NULL DEFAULT '',
  unread INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(job_id) REFERENCES jobs(id)
);
INSERT INTO jobs(id,revision,definition,enabled,paused,cursor,updated_at)
VALUES('job','hash-v1','{"id":"job","revision":1}',1,1,'2026-08-22T00:00:00Z','2026-08-22T00:00:00Z');
INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at,unread)
VALUES('legacy-run','job','hash-v1','{"id":"job","revision":1}','manual','succeeded','2026-08-22T00:00:00Z','2026-08-22T00:00:00Z',1);
`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job, err := s.Job(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if !job.Paused || job.PauseReason != "" || !job.PauseAt.IsZero() {
		t.Fatalf("legacy paused job backfilled reason=%q at=%v", job.PauseReason, job.PauseAt)
	}
	run, err := s.GetRun(context.Background(), "legacy-run")
	if err != nil || run.State != StateSucceeded || !run.Unread {
		t.Fatalf("legacy run=%+v err=%v", run, err)
	}
}

func TestUnreadGuardFailsClosedWithoutAuthorityFence(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	syncGuardJob(t, s)
	seedUnreadTerminalRun(t, s, StateSucceeded, false)
	now := ts("2026-08-23T03:00:00Z")
	req := request(now)
	req.MaxUnreadTerminalRuns = 1
	_, err := s.AcceptOccurrence(ctx, req)
	if err == nil || !strings.Contains(err.Error(), "authority-fenced") {
		t.Fatalf("AcceptOccurrence error=%v", err)
	}
	job, err := s.Job(ctx, "job")
	if err != nil || job.Paused || job.PauseReason != "" {
		t.Fatalf("unfenced call paused the job: %+v err=%v", job, err)
	}
	runs, listErr := s.ListRuns(ctx, "job", 20)
	if listErr != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, listErr)
	}
	occurrences, occErr := guardOccurrences(s)
	if occErr != nil || occurrences != 0 {
		t.Fatalf("occurrences=%d err=%v", occurrences, occErr)
	}
	// Zero (the default) keeps the historical unfenced path usable.
	req.MaxUnreadTerminalRuns = 0
	result, err := s.AcceptOccurrence(ctx, req)
	if err != nil || result.Run == nil {
		t.Fatalf("zero-limit AcceptOccurrence result=%+v err=%v", result, err)
	}
}
