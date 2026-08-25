package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	"github.com/terry-li-hm/herdr-bots/internal/store"
)

// The accepted instant is deliberately late in the UTC day: 2026-03-09 23:30Z
// is 2026-03-10 in Asia/Hong_Kong, so a {date} source that resolves against UTC
// instead of the job's timezone names a different file and fails to stage.
const (
	boundedTestAccepted  = "2026-03-09T23:30:00Z"
	boundedTestLocalDate = "2026-03-10"

	boundedTestContent     = "region,count\nhk,42\n"
	boundedTestDestination = config.BoundedInputsDir + "/intake.csv"
)

// boundedTestJob is the declared surface under test: a Hong Kong schedule, the
// inputs staged before the run, and the write scope the run must stay inside.
func boundedTestJob(repo string, inputs []config.InputSnapshot, allowed []string) config.Job {
	enabled := true
	grace := 0
	return config.Job{
		ID:       "bounded-review",
		Revision: 1,
		Enabled:  &enabled,
		Schedule: config.Schedule{
			Kind:                config.ScheduleCron,
			Expression:          "0 9 * * *",
			Timezone:            "Asia/Hong_Kong",
			CatchUpGraceMinutes: &grace,
		},
		Execution: config.Execution{
			Repository:        repo,
			Workspace:         config.WorkspaceWorktree,
			Harness:           config.HarnessClaudeCode,
			Model:             "claude-opus-5",
			Thinking:          "high",
			PermissionProfile: config.PermissionRepoWrite,
			Inputs:            inputs,
			AllowedWritePaths: allowed,
		},
		Prompt:         "Review the staged data.",
		TimeoutMinutes: 30,
		Overlap:        "forbid",
		Limits:         config.Limits{MaxRunsPerDay: 1},
	}
}

// newBoundedRun opens a real store, saves the job the run is accepted under,
// and returns an accepted nonterminal run whose worktree receipt is the job's
// repository. Only the store is needed by the bounded-review helpers.
func newBoundedRun(t *testing.T, job config.Job) (*Engine, *store.Store, store.Run) {
	t.Helper()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	snapshot, revision, err := job.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := mustTime(boundedTestAccepted)
	if _, err := state.SyncJob(ctx, job.ID, revision, snapshot, true, now); err != nil {
		t.Fatal(err)
	}
	run, err := state.CreateManualRun(ctx, job.ID, revision, snapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != store.StateAccepted {
		t.Fatalf("run=%+v", run)
	}
	run.WorktreePath = job.Execution.Repository
	return &Engine{Store: state}, state, run
}

type boundedFixture struct {
	engine *Engine
	store  *store.Store
	run    store.Run
	job    config.Job
	repo   string
	source string
}

// newBoundedFixture builds the common setup: a git repository holding one
// committed tracked file, one dated source outside it, and an accepted run for
// a job declaring that source as an input and allowed as its write scope.
func newBoundedFixture(t *testing.T, allowed []string) boundedFixture {
	t.Helper()
	repo := t.TempDir()
	initGitRepo(t, repo)
	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "intake-"+boundedTestLocalDate+".csv")
	writeBoundedFile(t, source, boundedTestContent)
	job := boundedTestJob(repo, []config.InputSnapshot{{
		Source:      filepath.Join(sourceDir, "intake-{date}.csv"),
		Destination: boundedTestDestination,
	}}, allowed)
	engine, state, run := newBoundedRun(t, job)
	return boundedFixture{engine: engine, store: state, run: run, job: job, repo: repo, source: source}
}

// stage copies the declared input into the workspace and carries the durable
// receipt on the run, exactly as dispatch does before an agent is started.
func (f boundedFixture) stage(t *testing.T) boundedFixture {
	t.Helper()
	receipt, err := f.engine.stageBoundedInputs(context.Background(), f.run, f.job)
	if err != nil {
		t.Fatal(err)
	}
	f.run.InputReceipt = receipt
	return f
}

func (f boundedFixture) staged() string { return filepath.Join(f.repo, boundedTestDestination) }

func writeBoundedFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func boundedCommit(t *testing.T, repo, message string) {
	t.Helper()
	git(t, repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", message)
}

func assertBoundedAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s still exists: %v", path, err)
	}
}

func assertBoundedContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s content=%q want=%q", path, got, want)
	}
}

// assertBoundedSameBytes proves the retained evidence is byte-for-byte the
// source it was staged from.
func assertBoundedSameBytes(t *testing.T, path, source string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s content=%q want=%q", path, got, want)
	}
}

// boundedFileState is the state a change receipt must record for a path
// holding exactly content.
func boundedFileState(path, content string) boundedPathState {
	digest := sha256.Sum256([]byte(content))
	return boundedPathState{
		Path:   path,
		Kind:   boundedPathKindFile,
		Size:   int64(len(content)),
		SHA256: hex.EncodeToString(digest[:]),
	}
}

// boundedDeletedState is the state a change receipt must record for a path git
// named because it is gone: no kind, nothing to size, nothing to digest.
func boundedDeletedState(path string) boundedPathState {
	return boundedPathState{Path: path}
}

func boundedStoredReceipts(t *testing.T, state *store.Store, runID string) (string, string) {
	t.Helper()
	run, err := state.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return run.InputReceipt, run.ChangeReceipt
}

func TestStageBoundedInputsRecordsLocalDateAndExactDigest(t *testing.T) {
	f := newBoundedFixture(t, []string{"reports/", "docs/notes.md"})
	receipt, err := f.engine.stageBoundedInputs(context.Background(), f.run, f.job)
	if err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256([]byte(boundedTestContent))
	want, err := json.Marshal(boundedInputReceipt{
		Version:        boundedReceiptVersion,
		OccurrenceDate: boundedTestLocalDate,
		Files: []boundedInputFile{{
			Source:      f.source,
			Destination: boundedTestDestination,
			Size:        int64(len(boundedTestContent)),
			SHA256:      hex.EncodeToString(digest[:]),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt != string(want) {
		t.Fatalf("receipt=%s want=%s", receipt, want)
	}
	if stored, _ := boundedStoredReceipts(t, f.store, f.run.ID); stored != receipt {
		t.Fatalf("persisted input receipt=%q want=%q", stored, receipt)
	}

	info, err := os.Lstat(f.staged())
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != boundedInputFileMode {
		t.Fatalf("staged mode=%v want a regular %04o file", info.Mode(), boundedInputFileMode)
	}
	assertBoundedContent(t, f.staged(), boundedTestContent)

	// The prompt names what the agent may open and what it may change, never
	// the machine path the snapshot was copied from.
	prompt := boundedReviewPrompt(f.job.Prompt, f.job, receipt)
	for _, want := range []string{boundedTestDestination, hex.EncodeToString(digest[:]), "- docs/notes.md", "- reports/"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt is missing %q: %s", want, prompt)
		}
	}
	for _, forbidden := range []string{f.source, filepath.Dir(f.source)} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt disclosed source path %q: %s", forbidden, prompt)
		}
	}
}

func TestStageBoundedInputsRefusesUnsafeSourceOrDestination(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		// prepare arranges the unsafe condition and returns the check proving
		// nothing outside this call's own creations was written or replaced.
		prepare func(t *testing.T, source, staged string) func(t *testing.T)
	}{
		{
			name: "source is a symlink",
			want: "source is a symlink",
			prepare: func(t *testing.T, source, staged string) func(t *testing.T) {
				target := filepath.Join(t.TempDir(), "target.csv")
				writeBoundedFile(t, target, boundedTestContent)
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, source); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) { assertBoundedAbsent(t, staged) }
			},
		},
		{
			name: "source exceeds the per-input bound",
			want: "per-input limit",
			prepare: func(t *testing.T, source, staged string) func(t *testing.T) {
				if err := os.Truncate(source, maxBoundedInputFileBytes+1); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) { assertBoundedAbsent(t, staged) }
			},
		},
		{
			name: "destination already exists",
			want: "already exists and is never overwritten",
			prepare: func(t *testing.T, source, staged string) func(t *testing.T) {
				writeBoundedFile(t, staged, "existing\n")
				return func(t *testing.T) { assertBoundedContent(t, staged, "existing\n") }
			},
		},
		{
			name: "reserved parent is a symlink",
			want: "reserved parent " + config.BoundedInputsDir + " is a symlink",
			prepare: func(t *testing.T, source, staged string) func(t *testing.T) {
				outside := t.TempDir()
				reserved := filepath.Dir(staged)
				if err := os.MkdirAll(filepath.Dir(reserved), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, reserved); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					entries, err := os.ReadDir(outside)
					if err != nil {
						t.Fatal(err)
					}
					if len(entries) != 0 {
						t.Fatalf("staging followed a symlinked parent: %v", entries)
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBoundedFixture(t, []string{"reports/"})
			after := tc.prepare(t, f.source, f.staged())

			receipt, err := f.engine.stageBoundedInputs(context.Background(), f.run, f.job)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want it to contain %q", err, tc.want)
			}
			if receipt != "" {
				t.Fatalf("failed staging returned receipt %q", receipt)
			}
			if stored, _ := boundedStoredReceipts(t, f.store, f.run.ID); stored != "" {
				t.Fatalf("failed staging persisted receipt %q", stored)
			}
			after(t)
		})
	}
}

func TestVerifyBoundedReviewRecordsSortedChangesAndRetainsStagedInput(t *testing.T) {
	f := newBoundedFixture(t, []string{"reports/", "README.md"})
	writeBoundedFile(t, filepath.Join(f.repo, "reports", "tracked.md"), "tracked\n")
	git(t, f.repo, "add", "reports/tracked.md")
	boundedCommit(t, f.repo, "add reports")
	f = f.stage(t)

	writeBoundedFile(t, filepath.Join(f.repo, "README.md"), "reviewed\n")            // tracked, exact-file entry
	writeBoundedFile(t, filepath.Join(f.repo, "reports", "tracked.md"), "updated\n") // tracked, directory prefix
	writeBoundedFile(t, filepath.Join(f.repo, "reports", "new.md"), "added\n")       // untracked, directory prefix

	payload, err := f.engine.verifyBoundedReview(context.Background(), f.run, f.job)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(boundedChangeReceipt{
		Version:           boundedReceiptVersion,
		AllowedWritePaths: []string{"README.md", "reports/"},
		ChangedPaths:      []string{"README.md", "reports/new.md", "reports/tracked.md"},
		States: []boundedPathState{
			boundedFileState("README.md", "reviewed\n"),
			boundedFileState("reports/new.md", "added\n"),
			boundedFileState("reports/tracked.md", "updated\n"),
		},
		Verdict: boundedVerdictWithinScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload != string(want) {
		t.Fatalf("receipt=%s want=%s", payload, want)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != payload {
		t.Fatalf("persisted change receipt=%q want=%q", stored, payload)
	}
	// The verified snapshot is deliberately retained as evidence: the staged
	// bytes must still match the source the receipt was computed from.
	assertBoundedSameBytes(t, f.staged(), f.source)
}

// The change receipt is bound to content, so the write-once rule can tell an
// idempotent repeat from a rewrite even when neither observation changed which
// paths git names.
func TestVerifyBoundedReviewBindsChangeReceiptToObservedContent(t *testing.T) {
	f := newBoundedFixture(t, []string{"reports/"}).stage(t)
	report := filepath.Join(f.repo, "reports", "summary.md")
	writeBoundedFile(t, report, "first\n")

	ctx := context.Background()
	first, err := f.engine.verifyBoundedReview(ctx, f.run, f.job)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(boundedChangeReceipt{
		Version:           boundedReceiptVersion,
		AllowedWritePaths: []string{"reports/"},
		ChangedPaths:      []string{"reports/summary.md"},
		States:            []boundedPathState{boundedFileState("reports/summary.md", "first\n")},
		Verdict:           boundedVerdictWithinScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != string(want) {
		t.Fatalf("receipt=%s want=%s", first, want)
	}

	// Observing an untouched worktree twice recomputes identical bytes, which
	// the durable receipt accepts as the repeat it is.
	again, err := f.engine.verifyBoundedReview(ctx, f.run, f.job)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("second observation of an unchanged worktree receipt=%s want=%s", again, first)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != first {
		t.Fatalf("persisted change receipt=%q want=%q", stored, first)
	}

	// Editing a file the scope already allows names the same path and different
	// content, so the second observation conflicts with the durable receipt
	// instead of quietly replacing it.
	writeBoundedFile(t, report, "second\n")
	payload, err := f.engine.verifyBoundedReview(ctx, f.run, f.job)
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("err=%v want it to be a %v", err, store.ErrStateConflict)
	}
	if payload != "" {
		t.Fatalf("conflicting observation returned receipt %q", payload)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != first {
		t.Fatalf("conflicting observation replaced the receipt: stored=%q want=%q", stored, first)
	}
	// The conflict is a refusal to rewrite the receipt, not a cleanup: the
	// mutated file and the staged evidence are both left exactly as they are.
	assertBoundedContent(t, report, "second\n")
	assertBoundedContent(t, f.staged(), boundedTestContent)
}

func TestVerifyBoundedReviewFailsOutOfScopeChangeBeforeAnyReceipt(t *testing.T) {
	for _, tc := range []struct{ name, path, content string }{
		{name: "tracked change", path: "README.md", content: "rewritten\n"},
		{name: "untracked file", path: "notes.md", content: "added\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBoundedFixture(t, []string{"reports/"}).stage(t)
			writeBoundedFile(t, filepath.Join(f.repo, tc.path), tc.content)

			payload, err := f.engine.verifyBoundedReview(context.Background(), f.run, f.job)
			if err == nil || !strings.Contains(err.Error(), "outside the declared write scope") || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("err=%v want it to name %q as out of scope", err, tc.path)
			}
			if payload != "" {
				t.Fatalf("refused verification returned receipt %q", payload)
			}
			if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != "" {
				t.Fatalf("refused verification persisted change receipt %q", stored)
			}
			// A refused verdict leaves the evidence in place rather than cleaning
			// up after a run whose writes were never accounted for.
			assertBoundedContent(t, f.staged(), boundedTestContent)
		})
	}
}

func TestVerifyBoundedReviewFailsWhenStagedInputNoLongerMatchesItsReceipt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		want    string
		corrupt func(t *testing.T, staged, source string)
	}{
		{
			name: "content mutated in place",
			want: "no longer matches its receipt",
			corrupt: func(t *testing.T, staged, source string) {
				// Same length, different bytes: only the digest can catch this.
				writeBoundedFile(t, staged, "region,count\nhk,43\n")
			},
		},
		{
			name: "input deleted",
			want: "stage=verify-input",
			corrupt: func(t *testing.T, staged, source string) {
				if err := os.Remove(staged); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "input replaced by a symlink",
			want: "staged input was replaced by a symlink",
			corrupt: func(t *testing.T, staged, source string) {
				if err := os.Remove(staged); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(source, staged); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBoundedFixture(t, []string{"reports/"}).stage(t)
			tc.corrupt(t, f.staged(), f.source)

			payload, err := f.engine.verifyBoundedReview(context.Background(), f.run, f.job)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want it to contain %q", err, tc.want)
			}
			if payload != "" {
				t.Fatalf("refused verification returned receipt %q", payload)
			}
			if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != "" {
				t.Fatalf("refused verification persisted change receipt %q", stored)
			}
		})
	}
}

func TestVerifyBoundedReviewRefusesUndeclaredFileUnderReservedInputs(t *testing.T) {
	f := newBoundedFixture(t, []string{"reports/"}).stage(t)
	planted := filepath.Join(f.repo, config.BoundedInputsDir, "extra.csv")
	writeBoundedFile(t, planted, "planted\n")

	payload, err := f.engine.verifyBoundedReview(context.Background(), f.run, f.job)
	if err == nil || !strings.Contains(err.Error(), "is not a staged input") || !strings.Contains(err.Error(), "extra.csv") {
		t.Fatalf("err=%v want it to reject the planted file", err)
	}
	if payload != "" {
		t.Fatalf("refused verification returned receipt %q", payload)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != "" {
		t.Fatalf("refused verification persisted change receipt %q", stored)
	}
	assertBoundedContent(t, f.staged(), boundedTestContent)
	assertBoundedContent(t, planted, "planted\n")
}

func TestVerifyBoundedReviewCountsBothSidesOfATrackedRename(t *testing.T) {
	f := newBoundedFixture(t, nil)
	writeBoundedFile(t, filepath.Join(f.repo, "old.md"), "moved content\n")
	git(t, f.repo, "add", "old.md")
	boundedCommit(t, f.repo, "add old")
	git(t, f.repo, "mv", "old.md", "new.md")

	ctx := context.Background()
	// A scope naming only where the content arrived does not cover where it
	// left, so the run is refused.
	destinationOnly := boundedTestJob(f.repo, nil, []string{"new.md"})
	payload, err := f.engine.verifyBoundedReview(ctx, f.run, destinationOnly)
	if err == nil || !strings.Contains(err.Error(), "old.md") {
		t.Fatalf("err=%v want it to name the vacated path", err)
	}
	if payload != "" {
		t.Fatalf("refused verification returned receipt %q", payload)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != "" {
		t.Fatalf("refused verification persisted change receipt %q", stored)
	}

	bothSides := boundedTestJob(f.repo, nil, []string{"new.md", "old.md"})
	payload, err = f.engine.verifyBoundedReview(ctx, f.run, bothSides)
	if err != nil {
		t.Fatal(err)
	}
	// The vacated side of the rename is a path git named and the filesystem no
	// longer has, so it is recorded as a deletion rather than refused.
	want, err := json.Marshal(boundedChangeReceipt{
		Version:           boundedReceiptVersion,
		AllowedWritePaths: []string{"new.md", "old.md"},
		ChangedPaths:      []string{"new.md", "old.md"},
		States: []boundedPathState{
			boundedFileState("new.md", "moved content\n"),
			boundedDeletedState("old.md"),
		},
		Verdict: boundedVerdictWithinScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload != string(want) {
		t.Fatalf("receipt=%s want=%s", payload, want)
	}
	if _, stored := boundedStoredReceipts(t, f.store, f.run.ID); stored != payload {
		t.Fatalf("persisted change receipt=%q want=%q", stored, payload)
	}
}

func TestBoundedReviewIsInertWhenNeitherFieldIsDeclared(t *testing.T) {
	// The worktree path does not exist and is not a repository, so any staging,
	// git enumeration, or receipt write would fail instead of returning cleanly.
	repo := filepath.Join(t.TempDir(), "absent-worktree")
	job := boundedTestJob(repo, nil, nil)
	engine, state, run := newBoundedRun(t, job)
	ctx := context.Background()

	receipt, err := engine.stageBoundedInputs(ctx, run, job)
	if receipt != "" || err != nil {
		t.Fatalf("stage receipt=%q err=%v", receipt, err)
	}
	payload, err := engine.verifyBoundedReview(ctx, run, job)
	if payload != "" || err != nil {
		t.Fatalf("verify receipt=%q err=%v", payload, err)
	}
	if input, change := boundedStoredReceipts(t, state, run.ID); input != "" || change != "" {
		t.Fatalf("undeclared job persisted receipts input=%q change=%q", input, change)
	}
	if prompt := boundedReviewPrompt(job.Prompt, job, ""); prompt != job.Prompt {
		t.Fatalf("prompt=%q want the base prompt %q", prompt, job.Prompt)
	}
}
