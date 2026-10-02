package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Recovery event and error code for an owner-directed explicit recovery of a
// stuck provisioning run. It is deliberately distinct from every
// reconciliation code so durable evidence never claims the daemon reconciled
// a run an operator retired by hand.
const (
	RecoveryCodeProvisioning   = "explicit_provisioning_recovery"
	recoveryDetailProvisioning = "explicit recovery verified the planned branch no longer names a workspace or worktree and retired the run as interrupted"
)

// InterruptRecoveredProvisioning retires exactly one observed provisioning row
// as terminal interrupted after the caller has independently verified that the
// planned branch names no workspace or worktree. It is modeled on
// InterruptExpiredProvisioning, but the compare-and-set binds the
// recovery-relevant fields of the observed row -- exact state, provisioning
// owner and lease (including the legacy spellings of an empty owner and a
// zero lease), planned branch, saved definition, updated_at, the empty
// workspace/pane/worktree receipts, and the effect ownership/claim/kind/
// receipt fields together with the observed effect lease instant, which may
// be zero or expired. It does not bind every column of the row. Lease expiry
// and the absence of every receipt and effect are rechecked at update time,
// so a concurrent lease renewal, ownership change, receipt write, definition
// or branch change, effect claim, or state transition fails closed with no
// event and no unrelated mutation. Success is reported only after exactly one
// row actually transitioned; a retry against a row that already moved
// returns false.
func (s *Store) InterruptRecoveredProvisioning(ctx context.Context, observed Run, now time.Time) (bool, error) {
	if observed.ID == "" {
		return false, errors.New("explicit recovery requires the observed run row")
	}
	if observed.State != StateProvisioning {
		return false, fmt.Errorf("explicit recovery target %s is in %s, not %s", observed.ID, observed.State, StateProvisioning)
	}
	if observed.WorkspaceID != "" || observed.PaneID != "" || observed.WorktreePath != "" {
		return false, fmt.Errorf("explicit recovery target %s already carries a workspace, pane, or worktree receipt", observed.ID)
	}
	if observed.EffectOwner != "" || observed.EffectClaim != "" || observed.EffectKind != "" || observed.EffectReceipt != "" {
		return false, fmt.Errorf("explicit recovery target %s already carries a run effect", observed.ID)
	}
	if !observed.EffectLeaseUntil.IsZero() && observed.EffectLeaseUntil.After(now) {
		return false, fmt.Errorf("explicit recovery target %s has a live effect lease until %s", observed.ID, observed.EffectLeaseUntil)
	}
	// Any future lease refuses recovery, including the inconsistent legacy
	// shape of an emptied owner whose stored lease never expired.
	if observed.ProvisioningLeaseUntil.After(now) {
		return false, fmt.Errorf("explicit recovery target %s has a live provisioning lease until %s", observed.ID, observed.ProvisioningLeaseUntil)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, observed.ID, StateInterrupted, "unverified")
	if err != nil {
		return false, err
	}
	// A zero lease time is the stored legacy spelling of "no lease at all";
	// binding the observed instant keeps both spellings exact in the CAS.
	provisioningLease := leaseNanos(observed.ProvisioningLeaseUntil)
	effectLease := leaseNanos(observed.EffectLeaseUntil)
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?, infrastructure_result='uncertain', agent_result='not_started', task_verdict='unverified', error_code=?, error_detail=?, provisioning_owner='', provisioning_lease_until=0, acceptance_lane=?, acceptance_reason=?, unread=?, updated_at=? WHERE id=? AND state=? AND workspace_id=? AND pane_id=? AND worktree_path=? AND branch=? AND definition=? AND updated_at=? AND provisioning_owner=? AND provisioning_lease_until=? AND effect_owner=? AND effect_claim=? AND effect_kind=? AND effect_receipt=? AND effect_lease_until=? AND workspace_id='' AND pane_id='' AND worktree_path='' AND effect_owner='' AND effect_claim='' AND effect_kind='' AND effect_receipt='' AND provisioning_lease_until<=?`,
		StateInterrupted, RecoveryCodeProvisioning, recoveryDetailProvisioning, lane, reason, unread, stamp,
		observed.ID, observed.State, observed.WorkspaceID, observed.PaneID, observed.WorktreePath, observed.Branch, observed.Definition, formatTime(observed.UpdatedAt), observed.ProvisioningOwner, provisioningLease, observed.EffectOwner, observed.EffectClaim, observed.EffectKind, observed.EffectReceipt, effectLease, now.UnixNano())
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, observed.ID, StateProvisioning, StateInterrupted, stamp, RecoveryCodeProvisioning, recoveryDetailProvisioning); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// leaseNanos is the stored representation of a lease instant: a zero time is
// persisted as integer zero, every other instant as its Unix nanoseconds.
func leaseNanos(lease time.Time) int64 {
	if lease.IsZero() {
		return 0
	}
	return lease.UnixNano()
}
