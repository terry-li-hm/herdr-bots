package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

const (
	receiptOne = "claude-opus-5@sha256:0ba2a1"
	receiptTwo = "claude-sonnet-5@sha256:5f1c3d"
)

// acceptedRun seeds a job and accepts a single occurrence at at, returning the
// run in StateAccepted.
func acceptedRun(t *testing.T, s *Store, at time.Time) Run {
	t.Helper()
	syncJob(t, s, at)
	result, err := s.AcceptOccurrence(context.Background(), request(at))
	if err != nil {
		t.Fatalf("AcceptOccurrence: %v", err)
	}
	if result.Run == nil {
		t.Fatalf("AcceptOccurrence: run = nil, want accepted run (outcome %q)", result.Outcome)
	}
	if result.Run.State != StateAccepted {
		t.Fatalf("state = %v, want %v", result.Run.State, StateAccepted)
	}
	return *result.Run
}

func attestationOf(t *testing.T, s *Store, id string) string {
	t.Helper()
	run, err := s.GetRun(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	return run.ModelAttestation
}

func TestSetModelAttestationRetryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	run := acceptedRun(t, s, ts("2026-08-25T09:00:00Z"))

	if err := s.SetModelAttestation(ctx, run.ID, receiptOne); err != nil {
		t.Fatalf("SetModelAttestation: %v", err)
	}
	if got := attestationOf(t, s, run.ID); got != receiptOne {
		t.Fatalf("attestation = %q, want %q", got, receiptOne)
	}

	if err := s.SetModelAttestation(ctx, run.ID, receiptOne); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if got := attestationOf(t, s, run.ID); got != receiptOne {
		t.Fatalf("attestation after retry = %q, want %q", got, receiptOne)
	}

	err := s.SetModelAttestation(ctx, run.ID, receiptTwo)
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("conflicting receipt: err = %v, want %v", err, ErrStateConflict)
	}
	if got := attestationOf(t, s, run.ID); got != receiptOne {
		t.Fatalf("attestation after conflict = %q, want original %q", got, receiptOne)
	}
}

func TestSetModelAttestationRejectsEmptyReceipt(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	run := acceptedRun(t, s, ts("2026-08-25T09:15:00Z"))

	if err := s.SetModelAttestation(ctx, run.ID, ""); err == nil {
		t.Fatal("SetModelAttestation with empty receipt: got nil error, want error")
	}
	if got := attestationOf(t, s, run.ID); got != "" {
		t.Fatalf("attestation = %q, want empty", got)
	}
}

func TestSetModelAttestationAfterFinishConflicts(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	run := acceptedRun(t, s, ts("2026-08-25T09:30:00Z"))

	if err := s.SetModelAttestation(ctx, run.ID, receiptOne); err != nil {
		t.Fatalf("SetModelAttestation: %v", err)
	}
	if err := s.Finish(ctx, run.ID, StateAccepted, StateSucceeded, "completed", "completed", "unverified", "", "", ts("2026-08-25T09:31:00Z")); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	finished, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if finished.State != StateSucceeded {
		t.Fatalf("state = %v, want %v", finished.State, StateSucceeded)
	}
	if finished.ModelAttestation != receiptOne {
		t.Fatalf("attestation = %q, want %q", finished.ModelAttestation, receiptOne)
	}

	for _, receipt := range []string{receiptOne, receiptTwo} {
		if err := s.SetModelAttestation(ctx, run.ID, receipt); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("SetModelAttestation(%q) after finish: err = %v, want %v", receipt, err, ErrStateConflict)
		}
	}
	if got := attestationOf(t, s, run.ID); got != receiptOne {
		t.Fatalf("attestation after conflicts = %q, want %q", got, receiptOne)
	}
}

func TestModelAttestationSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite3")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	run := acceptedRun(t, s, ts("2026-08-25T09:45:00Z"))
	if err := s.SetModelAttestation(ctx, run.ID, receiptOne); err != nil {
		t.Fatalf("SetModelAttestation: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	got, err := reopened.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun after reopen: %v", err)
	}
	if got.ModelAttestation != receiptOne {
		t.Fatalf("attestation after reopen = %q, want %q", got.ModelAttestation, receiptOne)
	}
	if got.State != StateAccepted {
		t.Fatalf("state after reopen = %v, want %v", got.State, StateAccepted)
	}
}
