package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

// Explicit recovery refusals are typed so the owner-facing CLI can distinguish
// a bad target (wrong state, live claim, receipt or effect already present,
// unusable saved inputs) from an observation that refused to conclude
// (lookup failure, found workspace, inconsistent result).
var (
	ErrRecoveryNotProvisioning    = errors.New("run is not in the provisioning state")
	ErrRecoveryLeaseLive          = errors.New("provisioning claim is still live")
	ErrRecoveryReceiptPresent     = errors.New("run already carries a workspace, pane, or worktree receipt")
	ErrRecoveryEffectPresent      = errors.New("run already carries a claimed or leased effect")
	ErrRecoveryInvalidSavedInputs = errors.New("saved recovery inputs are missing or invalid")
	ErrRecoveryLookupFailed       = errors.New("workspace lookup failed")
	ErrRecoveryWorkspaceFound     = errors.New("planned branch still names a workspace or worktree")
	ErrRecoveryLookupInconsistent = errors.New("workspace lookup returned an inconsistent result")
)

// RecoverProvisioning retires exactly one targeted run that is stuck in the
// provisioning state whose stored lease is expired or zero (the legacy
// unowned spelling); any future lease is refused, owned or not, including the
// inconsistent shape of an emptied owner with a still-live lease. It is not
// daemon reconciliation: it never evaluates schedules, dispatches, probes
// auth or models, creates or closes workspaces, deletes files, or launches
// agents. Its only external observation is one bounded FindWorkspaceByBranch
// for the run's saved planned branch, and only verified absence permits the
// terminal transition to interrupted -- never cancelled and never succeeded.
// The durable transition itself is the row-bound store CAS, so any concurrent
// mutation of the run fails closed.
func (e *Engine) RecoverProvisioning(ctx context.Context, runID string, now time.Time) (store.Run, error) {
	run, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return store.Run{}, err
	}
	if run.State != store.StateProvisioning {
		return store.Run{}, fmt.Errorf("%w: run %s is in %s; explicit recovery targets only provisioning runs", ErrRecoveryNotProvisioning, runID, run.State)
	}
	// Any future lease refuses recovery, including an inconsistent row whose
	// owner was emptied while its stored lease never expired.
	if run.ProvisioningLeaseUntil.After(now) {
		return store.Run{}, fmt.Errorf("%w: run %s has a lease live until %s held by %q", ErrRecoveryLeaseLive, runID, run.ProvisioningLeaseUntil, run.ProvisioningOwner)
	}
	if run.WorkspaceID != "" || run.PaneID != "" || run.WorktreePath != "" {
		return store.Run{}, fmt.Errorf("%w: run %s has workspace=%q pane=%q worktree=%q", ErrRecoveryReceiptPresent, runID, run.WorkspaceID, run.PaneID, run.WorktreePath)
	}
	if run.EffectOwner != "" || run.EffectClaim != "" || run.EffectKind != "" || run.EffectReceipt != "" {
		return store.Run{}, fmt.Errorf("%w: run %s has effect %q owned by %q", ErrRecoveryEffectPresent, runID, run.EffectKind, run.EffectOwner)
	}
	if !run.EffectLeaseUntil.IsZero() && run.EffectLeaseUntil.After(now) {
		return store.Run{}, fmt.Errorf("%w: run %s has a live effect lease until %s", ErrRecoveryEffectPresent, runID, run.EffectLeaseUntil)
	}
	if run.Branch == "" {
		return store.Run{}, fmt.Errorf("%w: run %s has no saved planned branch", ErrRecoveryInvalidSavedInputs, runID)
	}
	var job config.Job
	if err := json.Unmarshal(run.Definition, &job); err != nil {
		return store.Run{}, fmt.Errorf("%w: run %s has an invalid saved snapshot: %v", ErrRecoveryInvalidSavedInputs, runID, err)
	}
	if job.ID == "" {
		return store.Run{}, fmt.Errorf("%w: run %s saved snapshot has no job id", ErrRecoveryInvalidSavedInputs, runID)
	}
	if job.ID != run.JobID {
		return store.Run{}, fmt.Errorf("%w: run %s saved snapshot names job %q but the run belongs to %q", ErrRecoveryInvalidSavedInputs, runID, job.ID, run.JobID)
	}
	if job.Execution.Repository == "" || !filepath.IsAbs(job.Execution.Repository) {
		return store.Run{}, fmt.Errorf("%w: run %s saved repository %q is not an absolute path", ErrRecoveryInvalidSavedInputs, runID, job.Execution.Repository)
	}
	if job.Execution.Workspace != config.WorkspaceWorktree {
		return store.Run{}, fmt.Errorf("%w: run %s saved workspace mode %q is not %s", ErrRecoveryInvalidSavedInputs, runID, job.Execution.Workspace, config.WorkspaceWorktree)
	}
	findCtx, findCancel := context.WithTimeout(ctx, provisioningOperationLimit)
	receipt, found, findErr := e.Herdr.FindWorkspaceByBranch(findCtx, job.Execution.Repository, run.Branch)
	findCancel()
	if findErr != nil {
		return store.Run{}, fmt.Errorf("%w: run %s branch %q: %v", ErrRecoveryLookupFailed, runID, run.Branch, findErr)
	}
	if found {
		if receipt.WorkspaceID == "" {
			return store.Run{}, fmt.Errorf("%w: run %s branch %q names a Git worktree at %q without a Herdr workspace identity; manual cleanup is required", ErrRecoveryWorkspaceFound, runID, run.Branch, receipt.Path)
		}
		return store.Run{}, fmt.Errorf("%w: run %s branch %q names workspace %q at %q", ErrRecoveryWorkspaceFound, runID, run.Branch, receipt.WorkspaceID, receipt.Path)
	}
	if receipt.WorkspaceID != "" || receipt.PaneID != "" || receipt.Branch != "" || receipt.Path != "" {
		return store.Run{}, fmt.Errorf("%w: run %s branch %q lookup reported absence with a nonempty receipt workspace=%q pane=%q branch=%q path=%q", ErrRecoveryLookupInconsistent, runID, run.Branch, receipt.WorkspaceID, receipt.PaneID, receipt.Branch, receipt.Path)
	}
	recovered, err := e.Store.InterruptRecoveredProvisioning(ctx, run, now)
	if err != nil {
		return store.Run{}, err
	}
	if !recovered {
		return store.Run{}, fmt.Errorf("%w: run %s changed during explicit recovery", store.ErrStateConflict, runID)
	}
	final, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return store.Run{}, err
	}
	if final.State != store.StateInterrupted {
		return store.Run{}, fmt.Errorf("explicit recovery did not retire run %s as %s: %s", runID, store.StateInterrupted, final.State)
	}
	return final, nil
}
