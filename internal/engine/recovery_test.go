package engine

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/herdr"
	"github.com/terry-li-hm/herdr-bots/internal/store"
	_ "modernc.org/sqlite"
)

// recoveryHerdr wraps the shared fake so a test can script the one external
// observation explicit recovery performs, including failures, found
// workspaces, and races that mutate durable state during the lookup.
type recoveryHerdr struct {
	*fakeHerdr
	findCalls int
	find      func(ctx context.Context, repo, branch string) (herdr.Receipt, bool, error)
}

func (f *recoveryHerdr) FindWorkspaceByBranch(ctx context.Context, repo, branch string) (herdr.Receipt, bool, error) {
	f.findCalls++
	if f.find != nil {
		return f.find(ctx, repo, branch)
	}
	return f.fakeHerdr.FindWorkspaceByBranch(ctx, repo, branch)
}

func (f *recoveryHerdr) externalCalls() (provisions, closes, submits, starts int) {
	f.fakeHerdr.mu.Lock()
	defer f.fakeHerdr.mu.Unlock()
	return f.fakeHerdr.provisions, f.fakeHerdr.closeCount, f.fakeHerdr.submitCount, f.fakeHerdr.startAgentCount
}

func newRecoveryFixture(t *testing.T) (*Engine, *store.Store, *recoveryHerdr, config.Job) {
	t.Helper()
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &recoveryHerdr{fakeHerdr: &fakeHerdr{path: repo}}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	eng.DiskCapacity = func(string) (DiskCapacity, error) { return DiskCapacity{FreeGiB: 100, Device: 1}, nil }
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return eng, state, client, cfg.Jobs[0]
}

// recoveryClaimedRun admits one run through the durable admission CAS, giving
// it a live provisioning owner and lease plus a saved planned branch.
func recoveryClaimedRun(t *testing.T, state *store.Store, job config.Job, owner string, now time.Time) store.Run {
	t.Helper()
	ctx := context.Background()
	snapshot, revision, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.SyncJob(ctx, job.ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, job.ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := state.DecideAdmission(ctx, run.ID, owner, "1", 1.25, now, now.Add(time.Minute), func([]store.Run) (store.AdmissionDecision, error) {
		return store.AdmissionDecision{Admit: true}, nil
	})
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	branch := "auto/" + job.ID + "/planned"
	if _, err := state.SaveProvisioningPlan(ctx, run.ID, owner, branch, now); err != nil {
		t.Fatal(err)
	}
	return mustGetRun(t, state, run.ID)
}

// recoveryUnownedRun builds a legacy unowned provisioning row through public
// store APIs: no owner, no lease, and only the planned branch persisted.
func recoveryUnownedRun(t *testing.T, state *store.Store, definition []byte, branch string, now time.Time) store.Run {
	t.Helper()
	ctx := context.Background()
	if _, err := state.SyncJob(ctx, "docs-drift", "rev1", []byte(`{"id":"docs-drift","revision":1}`), true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, "docs-drift", "rev1", definition, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Transition(ctx, run.ID, store.StateAccepted, store.StateProvisioning, "test legacy claim", now); err != nil {
		t.Fatal(err)
	}
	if branch != "" {
		if err := state.SetReceipt(ctx, run.ID, "", "", branch, "", "agent", ""); err != nil {
			t.Fatal(err)
		}
	}
	return mustGetRun(t, state, run.ID)
}

func mustGetRun(t *testing.T, state *store.Store, id string) store.Run {
	t.Helper()
	run, err := state.GetRun(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func recoveryTypedEventCount(t *testing.T, state *store.Store, id string) int {
	t.Helper()
	events, err := state.Events(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Code == store.RecoveryCodeProvisioning {
			count++
		}
	}
	return count
}

func TestRecoverProvisioningRetiresExpiredOwnedRun(t *testing.T) {
	eng, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	run := recoveryClaimedRun(t, state, job, eng.owner, base)
	got, err := eng.RecoverProvisioning(context.Background(), run.ID, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.StateInterrupted || got.State == store.StateCancelled || got.State == store.StateSucceeded {
		t.Fatalf("state=%s want terminal interrupted, never cancelled or succeeded", got.State)
	}
	if got.ErrorCode != store.RecoveryCodeProvisioning || got.TaskVerdict != "unverified" || got.InfrastructureResult != "uncertain" || got.AgentResult != "not_started" {
		t.Fatalf("classification=%+v", got)
	}
	if got.ProvisioningOwner != "" || !got.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("claim was not released: %+v", got)
	}
	if got.WorkspaceID != "" || got.PaneID != "" || got.WorktreePath != "" || got.Branch != run.Branch {
		t.Fatalf("recovery fabricated or dropped receipts: %+v", got)
	}
	provisions, closes, submits, starts := client.externalCalls()
	if provisions != 0 || closes != 0 || submits != 0 || starts != 0 || client.findCalls != 1 {
		t.Fatalf("external calls provisions=%d closes=%d submits=%d starts=%d finds=%d", provisions, closes, submits, starts, client.findCalls)
	}
	if count := recoveryTypedEventCount(t, state, run.ID); count != 1 {
		t.Fatalf("typed recovery events=%d want 1", count)
	}
}

func TestRecoverProvisioningRetiresLegacyUnownedClaim(t *testing.T) {
	_, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	snapshot, _, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	run := recoveryUnownedRun(t, state, snapshot, "auto/docs-drift/legacy", base)
	if run.ProvisioningOwner != "" || !run.ProvisioningLeaseUntil.IsZero() {
		t.Fatalf("fixture is not a legacy unowned claim: %+v", run)
	}
	got, err := New(state, client.fakeHerdr, fakeCommands{}, "").RecoverProvisioning(context.Background(), run.ID, base)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.StateInterrupted || got.ErrorCode != store.RecoveryCodeProvisioning {
		t.Fatalf("run=%+v", got)
	}
}

// recoveryForceState walks a fresh run to a target nonterminal state through
// durable transitions, or terminalizes it from accepted for terminal targets.
func recoveryForceState(t *testing.T, state *store.Store, run store.Run, target string, now time.Time) {
	t.Helper()
	ctx := context.Background()
	if target == store.StateAccepted {
		return
	}
	if store.IsTerminalState(target) {
		if err := state.Finish(ctx, run.ID, store.StateAccepted, target, "completed", "not_started", "unverified", "", "", now); err != nil {
			t.Fatal(err)
		}
		return
	}
	current := store.StateAccepted
	for _, step := range []string{store.StateProvisioning, store.StateStarting, store.StateRunning, store.StateSettled, store.StateVerifying} {
		if err := state.Transition(ctx, run.ID, current, step, "test state walk", now); err != nil {
			t.Fatal(err)
		}
		current = step
		if step == target {
			return
		}
	}
	t.Fatalf("unsupported target state %s", target)
}

func TestRecoverProvisioningRefusesEveryNonProvisioningState(t *testing.T) {
	for _, target := range []string{
		store.StateAccepted, store.StateStarting, store.StateRunning, store.StateSettled, store.StateVerifying,
		store.StateSucceeded, store.StateFailed, store.StateBlocked, store.StateTimedOut, store.StateCancelled, store.StateInterrupted,
	} {
		t.Run(target, func(t *testing.T) {
			eng, state, client, job := newRecoveryFixture(t)
			base := time.Now().UTC()
			snapshot, revision, err := job.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.SyncJob(context.Background(), job.ID, revision, snapshot, true, base); err != nil {
				t.Fatal(err)
			}
			run, err := state.CreateManualRun(context.Background(), job.ID, revision, snapshot, base)
			if err != nil {
				t.Fatal(err)
			}
			recoveryForceState(t, state, run, target, base)
			_, err = eng.RecoverProvisioning(context.Background(), run.ID, base.Add(2*time.Minute))
			if !errors.Is(err, ErrRecoveryNotProvisioning) {
				t.Fatalf("state=%s error=%v", target, err)
			}
			after := mustGetRun(t, state, run.ID)
			if after.State != target || after.UpdatedAt.IsZero() {
				t.Fatalf("refusal changed the row: want %s got %+v", target, after)
			}
			if client.findCalls != 0 {
				t.Fatalf("state refusal still performed %d lookups", client.findCalls)
			}
			provisions, closes, submits, starts := client.externalCalls()
			if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
				t.Fatalf("state refusal made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
			}
		})
	}
}

func TestRecoverProvisioningRefusesLiveLease(t *testing.T) {
	eng, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	run := recoveryClaimedRun(t, state, job, eng.owner, base)
	_, err := eng.RecoverProvisioning(context.Background(), run.ID, base.Add(30*time.Second))
	if !errors.Is(err, ErrRecoveryLeaseLive) {
		t.Fatalf("error=%v", err)
	}
	after := mustGetRun(t, state, run.ID)
	if after.State != store.StateProvisioning || after.ProvisioningOwner != eng.owner || !after.ProvisioningLeaseUntil.After(base) {
		t.Fatalf("live claim was not preserved: %+v", after)
	}
	if client.findCalls != 0 {
		t.Fatalf("live-lease refusal performed %d lookups", client.findCalls)
	}
	provisions, closes, submits, starts := client.externalCalls()
	if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
		t.Fatalf("live-lease refusal made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
	}
}

// recoveryExecSQL writes a row shape that no public store API can express,
// directly on the same database file, so engine-level fencing can be observed
// against the inconsistent rows recovery must still refuse.
func recoveryExecSQL(t *testing.T, dbPath, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverProvisioningRefusesUnownedFutureLease(t *testing.T) {
	repo := t.TempDir()
	configPath := writeJobs(t, repo, true)
	dbPath := filepath.Join(t.TempDir(), "state.db")
	state, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	client := &recoveryHerdr{fakeHerdr: &fakeHerdr{path: repo}}
	eng := New(state, client, fakeCommands{models: "provider model\nopenai-codex gpt-5.6-sol\n"}, configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	run := recoveryClaimedRun(t, state, cfg.Jobs[0], "gone-daemon", base)
	// Empty only the owner: the stored lease stays in the future, which is
	// exactly the inconsistent legacy shape the unconditional fence targets.
	recoveryExecSQL(t, dbPath, `UPDATE runs SET provisioning_owner='' WHERE id=?`, run.ID)
	observed := mustGetRun(t, state, run.ID)
	if observed.ProvisioningOwner != "" || !observed.ProvisioningLeaseUntil.After(base.Add(30*time.Second)) {
		t.Fatalf("fixture is not an unowned row with a future lease: %+v", observed)
	}
	_, err = eng.RecoverProvisioning(context.Background(), run.ID, base.Add(30*time.Second))
	if !errors.Is(err, ErrRecoveryLeaseLive) {
		t.Fatalf("error=%v", err)
	}
	after := mustGetRun(t, state, run.ID)
	if after.State != store.StateProvisioning || after.ProvisioningOwner != "" || !after.ProvisioningLeaseUntil.Equal(observed.ProvisioningLeaseUntil) {
		t.Fatalf("unowned future lease was not fenced: %+v", after)
	}
	if count := recoveryTypedEventCount(t, state, run.ID); count != 0 {
		t.Fatalf("refusal wrote %d typed events", count)
	}
	if client.findCalls != 0 {
		t.Fatalf("refusal performed %d lookups", client.findCalls)
	}
	provisions, closes, submits, starts := client.externalCalls()
	if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
		t.Fatalf("refusal made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
	}
}

func TestRecoverProvisioningRefusesMissingOrInvalidSavedInputs(t *testing.T) {
	base := time.Now().UTC()
	valid := `{"id":"docs-drift","revision":1,"execution":{"repository":"/tmp/synthetic-repo","workspace":"worktree"}}`
	for name, tc := range map[string]struct {
		definition string
		branch     string
	}{
		"no planned branch":   {definition: valid, branch: ""},
		"invalid snapshot":    {definition: `{"id":"docs-drift":`, branch: "auto/docs-drift/planned"},
		"missing job id":      {definition: `{"revision":1,"execution":{"repository":"/tmp/synthetic-repo","workspace":"worktree"}}`, branch: "auto/docs-drift/planned"},
		"job id mismatch":     {definition: `{"id":"other-job","revision":1,"execution":{"repository":"/tmp/synthetic-repo","workspace":"worktree"}}`, branch: "auto/docs-drift/planned"},
		"empty repository":    {definition: `{"id":"docs-drift","execution":{"repository":"","workspace":"worktree"}}`, branch: "auto/docs-drift/planned"},
		"relative repository": {definition: `{"id":"docs-drift","execution":{"repository":"relative/repo","workspace":"worktree"}}`, branch: "auto/docs-drift/planned"},
		"non-worktree mode":   {definition: `{"id":"docs-drift","execution":{"repository":"/tmp/synthetic-repo","workspace":"root"}}`, branch: "auto/docs-drift/planned"},
	} {
		t.Run(name, func(t *testing.T) {
			eng, state, client, _ := newRecoveryFixture(t)
			run := recoveryUnownedRun(t, state, []byte(tc.definition), tc.branch, base)
			_, err := eng.RecoverProvisioning(context.Background(), run.ID, base)
			if !errors.Is(err, ErrRecoveryInvalidSavedInputs) {
				t.Fatalf("error=%v", err)
			}
			after := mustGetRun(t, state, run.ID)
			if after.State != store.StateProvisioning || after.Branch != tc.branch || string(after.Definition) != tc.definition {
				t.Fatalf("refusal changed the row: %+v", after)
			}
			if client.findCalls != 0 {
				t.Fatalf("invalid inputs still performed %d lookups", client.findCalls)
			}
		})
	}
}

func TestRecoverProvisioningRefusesReceiptAndEffect(t *testing.T) {
	eng, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	recoverAt := base.Add(2 * time.Minute)
	ctx := context.Background()

	receiptRun := recoveryClaimedRun(t, state, job, eng.owner, base)
	if err := state.SetReceipt(ctx, receiptRun.ID, "w-receipt", "", receiptRun.Branch, "/tmp/wt", "agent", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.RecoverProvisioning(ctx, receiptRun.ID, recoverAt); !errors.Is(err, ErrRecoveryReceiptPresent) {
		t.Fatalf("workspace receipt error=%v", err)
	}
	if after := mustGetRun(t, state, receiptRun.ID); after.State != store.StateProvisioning || after.WorkspaceID != "w-receipt" {
		t.Fatalf("receipt refusal changed the row: %+v", after)
	}

	paneRun := recoveryClaimedRun(t, state, job, eng.owner, base)
	if err := state.SetReceipt(ctx, paneRun.ID, "", "p-receipt", paneRun.Branch, "", "agent", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.RecoverProvisioning(ctx, paneRun.ID, recoverAt); !errors.Is(err, ErrRecoveryReceiptPresent) {
		t.Fatalf("pane receipt error=%v", err)
	}

	effectRun := recoveryClaimedRun(t, state, job, eng.owner, base)
	claim, err := state.ClaimWorkspaceClose(ctx, effectRun.ID, store.StateProvisioning, "closer", `{"terminal_state":"interrupted"}`, recoverAt, recoverAt.Add(time.Minute))
	if err != nil || claim == "" {
		t.Fatalf("claim=%q err=%v", claim, err)
	}
	if _, err := eng.RecoverProvisioning(ctx, effectRun.ID, recoverAt.Add(time.Second)); !errors.Is(err, ErrRecoveryEffectPresent) {
		t.Fatalf("effect error=%v", err)
	}
	after := mustGetRun(t, state, effectRun.ID)
	if after.State != store.StateProvisioning || after.EffectKind != store.EffectWorkspaceClose || after.EffectOwner != "closer" {
		t.Fatalf("effect refusal changed the row: %+v", after)
	}
	if client.findCalls != 0 {
		t.Fatalf("receipt/effect refusals still performed %d lookups", client.findCalls)
	}
	provisions, closes, submits, starts := client.externalCalls()
	if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
		t.Fatalf("refusals made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
	}
}

func TestRecoverProvisioningRefusesFoundWorkspaces(t *testing.T) {
	for name, tc := range map[string]struct {
		receipt herdr.Receipt
		message string
	}{
		"owned workspace": {
			receipt: herdr.Receipt{WorkspaceID: "w-orphan", Branch: "auto/docs-drift/planned", Path: "/tmp/wt"},
			message: "w-orphan",
		},
		"unowned worktree": {
			receipt: herdr.Receipt{Branch: "auto/docs-drift/planned", Path: "/tmp/wt"},
			message: "without a Herdr workspace identity",
		},
	} {
		t.Run(name, func(t *testing.T) {
			eng, state, client, job := newRecoveryFixture(t)
			base := time.Now().UTC()
			run := recoveryClaimedRun(t, state, job, eng.owner, base)
			client.find = func(context.Context, string, string) (herdr.Receipt, bool, error) {
				return tc.receipt, true, nil
			}
			_, err := eng.RecoverProvisioning(context.Background(), run.ID, base.Add(2*time.Minute))
			if !errors.Is(err, ErrRecoveryWorkspaceFound) || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error=%v want mention of %q", err, tc.message)
			}
			after := mustGetRun(t, state, run.ID)
			if after.State != store.StateProvisioning || after.WorkspaceID != "" {
				t.Fatalf("found-workspace refusal changed the row: %+v", after)
			}
			if count := recoveryTypedEventCount(t, state, run.ID); count != 0 {
				t.Fatalf("refusal wrote %d typed events", count)
			}
			provisions, closes, submits, starts := client.externalCalls()
			if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
				t.Fatalf("refusal made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
			}
		})
	}
}

func TestRecoverProvisioningRefusesLookupFailureAndInconsistentResult(t *testing.T) {
	eng, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	ctx := context.Background()

	failureRun := recoveryClaimedRun(t, state, job, eng.owner, base)
	client.find = func(context.Context, string, string) (herdr.Receipt, bool, error) {
		return herdr.Receipt{}, false, errors.New("worktree list unavailable")
	}
	if _, err := eng.RecoverProvisioning(ctx, failureRun.ID, base.Add(2*time.Minute)); !errors.Is(err, ErrRecoveryLookupFailed) {
		t.Fatalf("lookup failure error=%v", err)
	}
	if after := mustGetRun(t, state, failureRun.ID); after.State != store.StateProvisioning {
		t.Fatalf("lookup failure changed the row: %+v", after)
	}

	inconsistentRun := recoveryClaimedRun(t, state, job, eng.owner, base)
	client.find = func(context.Context, string, string) (herdr.Receipt, bool, error) {
		return herdr.Receipt{WorkspaceID: "w-ghost"}, false, nil
	}
	_, err := eng.RecoverProvisioning(ctx, inconsistentRun.ID, base.Add(2*time.Minute))
	if !errors.Is(err, ErrRecoveryLookupInconsistent) {
		t.Fatalf("inconsistent result error=%v", err)
	}
	if after := mustGetRun(t, state, inconsistentRun.ID); after.State != store.StateProvisioning || after.WorkspaceID != "" {
		t.Fatalf("inconsistent result changed the row: %+v", after)
	}
}

func TestRecoverProvisioningCASRaceFailsClosed(t *testing.T) {
	for name, race := range map[string]func(ctx context.Context, state *store.Store, run store.Run){
		"receipt written during lookup": func(ctx context.Context, state *store.Store, run store.Run) {
			if err := state.SetReceipt(ctx, run.ID, "w-raced", "", run.Branch, "/tmp/wt", "agent", ""); err != nil {
				t.Fatal(err)
			}
		},
		"branch changed during lookup": func(ctx context.Context, state *store.Store, run store.Run) {
			if err := state.SetReceipt(ctx, run.ID, "", "", "auto/docs-drift/replanned", "", "agent", ""); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			eng, state, client, job := newRecoveryFixture(t)
			base := time.Now().UTC()
			run := recoveryClaimedRun(t, state, job, eng.owner, base)
			client.find = func(ctx context.Context, _, _ string) (herdr.Receipt, bool, error) {
				race(ctx, state, run)
				return herdr.Receipt{}, false, nil
			}
			_, err := eng.RecoverProvisioning(context.Background(), run.ID, base.Add(2*time.Minute))
			if !errors.Is(err, store.ErrStateConflict) {
				t.Fatalf("race error=%v want a state conflict", err)
			}
			after := mustGetRun(t, state, run.ID)
			if after.State != store.StateProvisioning {
				t.Fatalf("raced row was retired anyway: %+v", after)
			}
			if count := recoveryTypedEventCount(t, state, run.ID); count != 0 {
				t.Fatalf("lost CAS wrote %d typed events", count)
			}
			provisions, closes, submits, starts := client.externalCalls()
			if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
				t.Fatalf("raced recovery made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
			}
		})
	}
}

func TestRecoverProvisioningDuplicateChangesNothing(t *testing.T) {
	eng, state, client, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	run := recoveryClaimedRun(t, state, job, eng.owner, base)
	recoverAt := base.Add(2 * time.Minute)
	if _, err := eng.RecoverProvisioning(context.Background(), run.ID, recoverAt); err != nil {
		t.Fatal(err)
	}
	retired := mustGetRun(t, state, run.ID)
	if _, err := eng.RecoverProvisioning(context.Background(), run.ID, recoverAt.Add(time.Minute)); !errors.Is(err, ErrRecoveryNotProvisioning) {
		t.Fatalf("duplicate error=%v", err)
	}
	after := mustGetRun(t, state, run.ID)
	if after.State != retired.State || after.UpdatedAt != retired.UpdatedAt || after.ErrorCode != retired.ErrorCode {
		t.Fatalf("duplicate changed the retired row: before=%+v after=%+v", retired, after)
	}
	if count := recoveryTypedEventCount(t, state, run.ID); count != 1 {
		t.Fatalf("duplicate wrote %d typed events, want 1", count)
	}
	if client.findCalls != 1 {
		t.Fatalf("duplicate performed %d lookups, want 1", client.findCalls)
	}
	provisions, closes, submits, starts := client.externalCalls()
	if provisions != 0 || closes != 0 || submits != 0 || starts != 0 {
		t.Fatalf("duplicate made external calls: provisions=%d closes=%d submits=%d starts=%d", provisions, closes, submits, starts)
	}
}

func TestRecoverProvisioningTargetsOnlyItsRun(t *testing.T) {
	eng, state, _, job := newRecoveryFixture(t)
	base := time.Now().UTC()
	ctx := context.Background()
	target := recoveryClaimedRun(t, state, job, eng.owner, base)

	// A second job keeps an accepted run and its authority untouched.
	other := job
	other.ID = "other-job"
	otherSnapshot, otherRevision, err := other.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.SyncJob(ctx, other.ID, otherRevision, otherSnapshot, true, base); err != nil {
		t.Fatal(err)
	}
	bystander, err := state.CreateManualRun(ctx, other.ID, otherRevision, otherSnapshot, base)
	if err != nil {
		t.Fatal(err)
	}
	jobBefore, err := state.Job(ctx, other.ID)
	if err != nil {
		t.Fatal(err)
	}

	got, err := eng.RecoverProvisioning(ctx, target.ID, base.Add(2*time.Minute))
	if err != nil || got.State != store.StateInterrupted {
		t.Fatalf("run=%+v err=%v", got, err)
	}
	afterBystander := mustGetRun(t, state, bystander.ID)
	if afterBystander.State != store.StateAccepted || afterBystander.WorkspaceID != "" {
		t.Fatalf("unrelated run was touched: %+v", afterBystander)
	}
	jobAfter, err := state.Job(ctx, other.ID)
	if err != nil {
		t.Fatal(err)
	}
	if jobAfter.Revision != jobBefore.Revision || jobAfter.Enabled != jobBefore.Enabled || jobAfter.Paused != jobBefore.Paused || !jobAfter.Cursor.Equal(jobBefore.Cursor) {
		t.Fatalf("unrelated job authority changed: before=%+v after=%+v", jobBefore, jobAfter)
	}
	if count := recoveryTypedEventCount(t, state, bystander.ID); count != 0 {
		t.Fatalf("unrelated run gained %d typed events", count)
	}
}
