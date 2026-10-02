package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// recoveryProvisioningRow inserts one synthetic provisioning run for the
// shared "job" authority, giving the test exact control over the owner, lease
// instant (a zero time is the legacy unowned spelling), planned branch, and
// saved definition.
func recoveryProvisioningRow(t *testing.T, s *Store, id, owner string, lease time.Time, branch string, definition []byte, now time.Time) Run {
	t.Helper()
	syncJob(t, s, now)
	if _, err := s.db.Exec(`INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,accepted_unix_nano,updated_at,provisioning_owner,provisioning_lease_until,branch) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, "job", "rev1", definition, "manual", StateProvisioning, formatTime(now), now.UnixNano(), formatTime(now), owner, leaseNanos(lease), branch); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func recoveryAssertNoTypedEvent(t *testing.T, s *Store, id string) {
	t.Helper()
	events, err := s.Events(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Code == RecoveryCodeProvisioning {
			t.Fatalf("refusal or lost CAS wrote a typed recovery event: %+v", event)
		}
	}
}

func recoveryAssertInterrupted(t *testing.T, got Run) {
	t.Helper()
	if got.State != StateInterrupted {
		t.Fatalf("state=%s want %s", got.State, StateInterrupted)
	}
	if got.InfrastructureResult != "uncertain" || got.AgentResult != "not_started" || got.TaskVerdict != "unverified" {
		t.Fatalf("interrupted classification=%+v", got)
	}
	if got.ErrorCode != RecoveryCodeProvisioning || got.ErrorDetail == "" || !strings.Contains(got.ErrorDetail, "explicit recovery") {
		t.Fatalf("error code=%q detail=%q", got.ErrorCode, got.ErrorDetail)
	}
	if got.ProvisioningOwner != "" || !got.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("claim was not released: owner=%q lease=%s", got.ProvisioningOwner, got.ProvisioningLeaseUntil)
	}
	if got.State == StateCancelled || got.State == StateSucceeded {
		t.Fatalf("explicit recovery must never retire as %s", got.State)
	}
	if got.AcceptanceLane != "mandatory" || got.AcceptanceReason != "state_interrupted" || !got.Unread {
		t.Fatalf("acceptance=%q/%q unread=%t", got.AcceptanceLane, got.AcceptanceReason, got.Unread)
	}
}

func TestInterruptRecoveredProvisioningRetiresExpiredAndLegacyClaims(t *testing.T) {
	now := ts("2026-08-22T09:00:00Z")
	recoverAt := now.Add(2 * time.Minute)
	definition := []byte(`{"id":"job","revision":1,"execution":{"repository":"/tmp/repo","workspace":"worktree"}}`)
	for name, tc := range map[string]struct {
		owner string
		lease time.Time
	}{
		"expired owned claim":  {owner: "stopped-daemon", lease: now.Add(time.Minute)},
		"legacy unowned claim": {owner: "", lease: time.Time{}},
		"unowned stale lease":  {owner: "", lease: now.Add(-time.Hour)},
		"owned zero lease":     {owner: "stopped-daemon", lease: time.Time{}},
		"owned long expired":   {owner: "stopped-daemon", lease: now.Add(-24 * time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			observed := recoveryProvisioningRow(t, s, "job-run", tc.owner, tc.lease, "auto/job/planned", definition, now)
			got, err := s.InterruptRecoveredProvisioning(context.Background(), observed, recoverAt)
			if err != nil || !got {
				t.Fatalf("recovered=%t err=%v", got, err)
			}
			after, err := s.GetRun(context.Background(), "job-run")
			if err != nil {
				t.Fatal(err)
			}
			recoveryAssertInterrupted(t, after)
			events, err := s.Events(context.Background(), "job-run")
			if err != nil || len(events) != 1 {
				t.Fatalf("events=%+v err=%v", events, err)
			}
			if events[0].FromState != StateProvisioning || events[0].ToState != StateInterrupted || events[0].Code != RecoveryCodeProvisioning {
				t.Fatalf("recovery event=%+v", events[0])
			}
		})
	}
}

func TestInterruptRecoveredProvisioningRefusesIneligibleObservedRows(t *testing.T) {
	now := ts("2026-08-22T09:00:00Z")
	definition := []byte(`{"id":"job","revision":1,"execution":{"repository":"/tmp/repo","workspace":"worktree"}}`)
	for name, tc := range map[string]struct {
		observed func(t *testing.T, s *Store, id string) Run
	}{
		"live provisioning lease": {observed: func(t *testing.T, s *Store, id string) Run {
			return recoveryProvisioningRow(t, s, id, "live-daemon", now.Add(time.Hour), "auto/job/planned", definition, now)
		}},
		"unowned future lease": {observed: func(t *testing.T, s *Store, id string) Run {
			return recoveryProvisioningRow(t, s, id, "", now.Add(time.Hour), "auto/job/planned", definition, now)
		}},
		"workspace receipt present": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET workspace_id='w-raced' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"pane receipt present": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET pane_id='p-raced' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"worktree path receipt alone": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET worktree_path='/wt/raced' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"effect owner alone": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_owner='closer' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"effect claim alone": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_claim='c1' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"effect kind alone": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_kind='workspace_close' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"effect receipt alone": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_receipt='{}' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"effect claimed": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_owner='closer',effect_claim='claim',effect_kind='workspace_close',effect_lease_until=?,effect_receipt='{}' WHERE id=?`, now.Add(time.Hour).UnixNano(), id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"live effect lease": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if _, err := s.db.Exec(`UPDATE runs SET effect_lease_until=? WHERE id=?`, now.Add(time.Hour).UnixNano(), id); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"terminal state": {observed: func(t *testing.T, s *Store, id string) Run {
			recoveryProvisioningRow(t, s, id, "stopped-daemon", now.Add(-time.Minute), "auto/job/planned", definition, now)
			if err := s.Finish(context.Background(), id, StateProvisioning, StateInterrupted, "uncertain", "not_started", "unverified", "restart_during_provisioning", "already retired", now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
		"accepted state": {observed: func(t *testing.T, s *Store, id string) Run {
			syncJob(t, s, now)
			if _, err := s.db.Exec(`INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,accepted_unix_nano,updated_at,branch) VALUES(?,?,?,?,?,?,?,?,?,?)`,
				id, "job", "rev1", definition, "manual", StateAccepted, formatTime(now), now.UnixNano(), formatTime(now), "auto/job/planned"); err != nil {
				t.Fatal(err)
			}
			return must(s.GetRun(context.Background(), id))
		}},
	} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			observed := tc.observed(t, s, "job-run")
			before := must(s.GetRun(context.Background(), "job-run"))
			recovered, err := s.InterruptRecoveredProvisioning(context.Background(), observed, now.Add(2*time.Minute))
			if err == nil || recovered {
				t.Fatalf("recovered=%t err=%v want a refusal", recovered, err)
			}
			after := must(s.GetRun(context.Background(), "job-run"))
			if after.State != before.State || after.UpdatedAt != before.UpdatedAt || after.ErrorCode != before.ErrorCode {
				t.Fatalf("refusal mutated the row: before=%+v after=%+v", before, after)
			}
			recoveryAssertNoTypedEvent(t, s, "job-run")
		})
	}
}

func TestInterruptRecoveredProvisioningCASRacesFailClosed(t *testing.T) {
	now := ts("2026-08-22T09:00:00Z")
	recoverAt := now.Add(2 * time.Minute)
	definition := []byte(`{"id":"job","revision":1,"execution":{"repository":"/tmp/repo","workspace":"worktree"}}`)
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, s *Store, id string)
		want   func(t *testing.T, after Run)
	}{
		"concurrent owner change": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET provisioning_owner='other-daemon' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			// Only the owner changed and the lease stayed expired, so a CAS
			// without exact ownership fencing would still have matched.
			if after.State != StateProvisioning || after.ProvisioningOwner != "other-daemon" {
				t.Fatalf("ownership was not fenced exactly: %+v", after)
			}
		}},
		"concurrent lease renewal": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET provisioning_lease_until=? WHERE id=?`, recoverAt.Add(time.Minute).UnixNano(), id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning || after.ProvisioningOwner != "stopped-daemon" {
				t.Fatalf("renewed claim was retired: %+v", after)
			}
		}},
		"concurrent state transition": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET state='starting' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateStarting {
				t.Fatalf("transition was clobbered: %+v", after)
			}
		}},
		"concurrent receipt write": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET workspace_id='w-raced' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning || after.WorkspaceID != "w-raced" {
				t.Fatalf("receipt was clobbered: %+v", after)
			}
		}},
		"concurrent branch change": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET branch='auto/job/replanned' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning || after.Branch != "auto/job/replanned" {
				t.Fatalf("branch was clobbered: %+v", after)
			}
		}},
		"concurrent definition change": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET definition=? WHERE id=?`, []byte(`{"id":"job","revision":2}`), id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning || string(after.Definition) != `{"id":"job","revision":2}` {
				t.Fatalf("definition was clobbered: %+v", after)
			}
		}},
		"concurrent effect claim": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET effect_owner='closer',effect_claim='c1',effect_kind='workspace_close',effect_lease_until=? WHERE id=?`, recoverAt.Add(time.Minute).UnixNano(), id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning || after.EffectKind != EffectWorkspaceClose || after.EffectOwner != "closer" {
				t.Fatalf("effect claim was clobbered: %+v", after)
			}
		}},
		"concurrent updated_at bump": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`UPDATE runs SET updated_at=? WHERE id=?`, formatTime(recoverAt.Add(time.Second)), id); err != nil {
				t.Fatal(err)
			}
		}, want: func(t *testing.T, after Run) {
			if after.State != StateProvisioning {
				t.Fatalf("row was retired after an unrelated update: %+v", after)
			}
		}},
		"row deleted": {mutate: func(t *testing.T, s *Store, id string) {
			if _, err := s.db.Exec(`DELETE FROM runs WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}, want: nil},
	} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			observed := recoveryProvisioningRow(t, s, "job-run", "stopped-daemon", now.Add(time.Minute), "auto/job/planned", definition, now)
			tc.mutate(t, s, "job-run")
			recovered, err := s.InterruptRecoveredProvisioning(context.Background(), observed, recoverAt)
			if tc.want == nil {
				// The deleted row makes the acceptance classification fail closed
				// with ErrNoRows before any mutation: nothing transitioned, so
				// this specific error is the correct outcome and is accepted.
				if recovered || !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("recovered=%t err=%v want a fail-closed %v", recovered, err, sql.ErrNoRows)
				}
			} else if err != nil || recovered {
				t.Fatalf("recovered=%t err=%v want a lost CAS", recovered, err)
			}
			recoveryAssertNoTypedEvent(t, s, "job-run")
			if tc.want == nil {
				if _, getErr := s.GetRun(context.Background(), "job-run"); !errors.Is(getErr, sql.ErrNoRows) {
					t.Fatalf("deleted row reappeared: %v", getErr)
				}
				return
			}
			after := must(s.GetRun(context.Background(), "job-run"))
			tc.want(t, after)
		})
	}
}

func TestInterruptRecoveredProvisioningDuplicateRetriesChangeNothing(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T09:00:00Z")
	recoverAt := now.Add(2 * time.Minute)
	definition := []byte(`{"id":"job","revision":1,"execution":{"repository":"/tmp/repo","workspace":"worktree"}}`)
	observed := recoveryProvisioningRow(t, s, "job-run", "stopped-daemon", now.Add(time.Minute), "auto/job/planned", definition, now)
	if recovered, err := s.InterruptRecoveredProvisioning(context.Background(), observed, recoverAt); err != nil || !recovered {
		t.Fatalf("first recovery=%t err=%v", recovered, err)
	}
	retired := must(s.GetRun(context.Background(), "job-run"))
	// A stale retry against the already-retired row must report a lost CAS...
	if recovered, err := s.InterruptRecoveredProvisioning(context.Background(), observed, recoverAt.Add(time.Minute)); err != nil || recovered {
		t.Fatalf("stale retry=%t err=%v", recovered, err)
	}
	// ...and a fresh read of the terminal row must be refused outright.
	fresh := must(s.GetRun(context.Background(), "job-run"))
	if _, err := s.InterruptRecoveredProvisioning(context.Background(), fresh, recoverAt.Add(time.Minute)); err == nil {
		t.Fatal("terminal row was accepted for recovery")
	}
	after := must(s.GetRun(context.Background(), "job-run"))
	if after.State != retired.State || after.UpdatedAt != retired.UpdatedAt || after.Unread != retired.Unread {
		t.Fatalf("retry mutated the retired row: before=%+v after=%+v", retired, after)
	}
	events, err := s.Events(context.Background(), "job-run")
	if err != nil || len(events) != 1 {
		t.Fatalf("retries wrote %d events, want 1: err=%v", len(events), err)
	}
}

func TestInterruptRecoveredProvisioningTargetsOnlyItsRun(t *testing.T) {
	s := testStore(t)
	now := ts("2026-08-22T09:00:00Z")
	definition := []byte(`{"id":"job","revision":1,"execution":{"repository":"/tmp/repo","workspace":"worktree"}}`)
	target := recoveryProvisioningRow(t, s, "job-target", "stopped-daemon", now.Add(time.Minute), "auto/job/planned", definition, now)
	if _, err := s.db.Exec(`INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,accepted_unix_nano,updated_at,provisioning_owner,provisioning_lease_until,branch) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"job-bystander", "job", "rev1", definition, "manual", StateProvisioning, formatTime(now.Add(time.Second)), now.Add(time.Second).UnixNano(), formatTime(now.Add(time.Second)), "live-daemon", now.Add(time.Hour).UnixNano(), "auto/job/other"); err != nil {
		t.Fatal(err)
	}
	jobBefore := must(s.Job(context.Background(), "job"))
	if recovered, err := s.InterruptRecoveredProvisioning(context.Background(), target, now.Add(2*time.Minute)); err != nil || !recovered {
		t.Fatalf("recovered=%t err=%v", recovered, err)
	}
	bystander := must(s.GetRun(context.Background(), "job-bystander"))
	if bystander.State != StateProvisioning || bystander.ProvisioningOwner != "live-daemon" || bystander.Branch != "auto/job/other" || !bystander.ProvisioningLeaseUntil.After(now) {
		t.Fatalf("unrelated run was touched: %+v", bystander)
	}
	recoveryAssertNoTypedEvent(t, s, "job-bystander")
	jobAfter := must(s.Job(context.Background(), "job"))
	if jobAfter.Revision != jobBefore.Revision || jobAfter.Enabled != jobBefore.Enabled || jobAfter.Paused != jobBefore.Paused || jobAfter.Completed != jobBefore.Completed || !jobAfter.Cursor.Equal(jobBefore.Cursor) || string(jobAfter.Definition) != string(jobBefore.Definition) {
		t.Fatalf("job authority changed: before=%+v after=%+v", jobBefore, jobAfter)
	}
}
