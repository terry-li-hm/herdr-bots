package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

const (
	inputReceiptOne  = "inputs@sha256:11aa22"
	inputReceiptTwo  = "inputs@sha256:33bb44"
	changeReceiptOne = "changes@sha256:55cc66"
	changeReceiptTwo = "changes@sha256:77dd88"
)

// boundedReceiptField projects one immutable bounded-review receipt so every
// invariant below is proved identically for both fields.
type boundedReceiptField struct {
	name    string
	column  string
	set     func(*Store, context.Context, string, string) error
	read    func(Run) string
	other   func(Run) string
	first   string
	drifted string
}

func boundedReceiptFields() []boundedReceiptField {
	return []boundedReceiptField{
		{
			name:    "input",
			column:  "input_receipt",
			set:     (*Store).SetInputReceipt,
			read:    func(run Run) string { return run.InputReceipt },
			other:   func(run Run) string { return run.ChangeReceipt },
			first:   inputReceiptOne,
			drifted: inputReceiptTwo,
		},
		{
			name:    "change",
			column:  "change_receipt",
			set:     (*Store).SetChangeReceipt,
			read:    func(run Run) string { return run.ChangeReceipt },
			other:   func(run Run) string { return run.InputReceipt },
			first:   changeReceiptOne,
			drifted: changeReceiptTwo,
		},
	}
}

func boundedReceiptOf(t *testing.T, s *Store, field boundedReceiptField, id string) string {
	t.Helper()
	run, err := s.GetRun(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	return field.read(run)
}

func TestBoundedReceiptFirstWriteIsDurableAndIndependent(t *testing.T) {
	ctx := context.Background()
	for _, field := range boundedReceiptFields() {
		t.Run(field.name, func(t *testing.T) {
			s := testStore(t)
			run := acceptedRun(t, s, ts("2026-08-25T10:00:00Z"))

			if got := boundedReceiptOf(t, s, field, run.ID); got != "" {
				t.Fatalf("%s before write = %q, want empty", field.column, got)
			}
			if err := field.set(s, ctx, run.ID, field.first); err != nil {
				t.Fatalf("set %s: %v", field.column, err)
			}
			got, err := s.GetRun(ctx, run.ID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if field.read(got) != field.first {
				t.Fatalf("%s = %q, want %q", field.column, field.read(got), field.first)
			}
			// One receipt never writes the other, and neither touches the
			// independent model attestation.
			if field.other(got) != "" {
				t.Fatalf("companion receipt = %q, want empty", field.other(got))
			}
			if got.ModelAttestation != "" {
				t.Fatalf("model attestation = %q, want empty", got.ModelAttestation)
			}
			if got.State != StateAccepted {
				t.Fatalf("state = %v, want %v", got.State, StateAccepted)
			}
		})
	}
}

func TestBoundedReceiptRetryIsIdempotentAndDriftConflicts(t *testing.T) {
	ctx := context.Background()
	for _, field := range boundedReceiptFields() {
		t.Run(field.name, func(t *testing.T) {
			s := testStore(t)
			run := acceptedRun(t, s, ts("2026-08-25T10:15:00Z"))

			if err := field.set(s, ctx, run.ID, field.first); err != nil {
				t.Fatalf("set %s: %v", field.column, err)
			}
			if err := field.set(s, ctx, run.ID, field.first); err != nil {
				t.Fatalf("identical retry: %v", err)
			}
			if got := boundedReceiptOf(t, s, field, run.ID); got != field.first {
				t.Fatalf("%s after retry = %q, want %q", field.column, got, field.first)
			}

			err := field.set(s, ctx, run.ID, field.drifted)
			if !errors.Is(err, ErrStateConflict) {
				t.Fatalf("drifted receipt: err = %v, want %v", err, ErrStateConflict)
			}
			if got := boundedReceiptOf(t, s, field, run.ID); got != field.first {
				t.Fatalf("%s after conflict = %q, want original %q", field.column, got, field.first)
			}
		})
	}
}

func TestBoundedReceiptRejectsEmptyReceiptAndMissingRun(t *testing.T) {
	ctx := context.Background()
	for _, field := range boundedReceiptFields() {
		t.Run(field.name, func(t *testing.T) {
			s := testStore(t)
			run := acceptedRun(t, s, ts("2026-08-25T10:30:00Z"))

			if err := field.set(s, ctx, run.ID, ""); err == nil {
				t.Fatalf("empty %s: got nil error, want error", field.column)
			}
			if got := boundedReceiptOf(t, s, field, run.ID); got != "" {
				t.Fatalf("%s = %q, want empty", field.column, got)
			}

			if err := field.set(s, ctx, "absent-run", field.first); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("missing run: err = %v, want %v", err, ErrStateConflict)
			}
			if err := field.set(s, ctx, "absent-run", ""); err == nil {
				t.Fatalf("missing run with empty %s: got nil error, want error", field.column)
			}
		})
	}
}

func TestBoundedReceiptIsClosedInEveryTerminalState(t *testing.T) {
	ctx := context.Background()
	terminals := []string{StateSucceeded, StateFailed, StateBlocked, StateTimedOut, StateCancelled, StateInterrupted}
	for _, field := range boundedReceiptFields() {
		for _, terminal := range terminals {
			t.Run(field.name+"/"+terminal, func(t *testing.T) {
				s := testStore(t)
				now := ts("2026-08-25T10:45:00Z")
				syncJob(t, s, now.Add(-time.Hour))
				run, err := s.CreateManualRun(ctx, "job", "rev1", []byte(`{"id":"job"}`), now)
				if err != nil {
					t.Fatalf("CreateManualRun: %v", err)
				}
				if err := field.set(s, ctx, run.ID, field.first); err != nil {
					t.Fatalf("set %s before finish: %v", field.column, err)
				}
				if err := s.Finish(ctx, run.ID, StateAccepted, terminal, "completed", "completed", "unverified", "", "", now.Add(time.Minute)); err != nil {
					t.Fatalf("Finish to %s: %v", terminal, err)
				}

				// A terminal run closes the receipt: even the identical value
				// conflicts, and the durable receipt is never rewritten.
				for _, receipt := range []string{field.first, field.drifted} {
					if err := field.set(s, ctx, run.ID, receipt); !errors.Is(err, ErrStateConflict) {
						t.Fatalf("set %s(%q) in %s: err = %v, want %v", field.column, receipt, terminal, err, ErrStateConflict)
					}
				}
				got, err := s.GetRun(ctx, run.ID)
				if err != nil {
					t.Fatalf("GetRun: %v", err)
				}
				if got.State != terminal {
					t.Fatalf("state = %v, want %v", got.State, terminal)
				}
				if field.read(got) != field.first {
					t.Fatalf("%s after conflicts = %q, want %q", field.column, field.read(got), field.first)
				}
			})
		}
	}
}

func TestBoundedReceiptsThatWereNeverWrittenStayClosedWhenTerminal(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := ts("2026-08-25T11:00:00Z")
	syncJob(t, s, now.Add(-time.Hour))
	run, err := s.CreateManualRun(ctx, "job", "rev1", []byte(`{"id":"job"}`), now)
	if err != nil {
		t.Fatalf("CreateManualRun: %v", err)
	}
	if err := s.Finish(ctx, run.ID, StateAccepted, StateInterrupted, "uncertain", "not_started", "unverified", "restart", "", now.Add(time.Minute)); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	for _, field := range boundedReceiptFields() {
		if err := field.set(s, ctx, run.ID, field.first); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("first %s after finish: err = %v, want %v", field.column, err, ErrStateConflict)
		}
		if got := boundedReceiptOf(t, s, field, run.ID); got != "" {
			t.Fatalf("%s = %q, want empty", field.column, got)
		}
	}
}

func TestBoundedReceiptsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite3")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	run := acceptedRun(t, s, ts("2026-08-25T11:15:00Z"))
	if err := s.SetInputReceipt(ctx, run.ID, inputReceiptOne); err != nil {
		t.Fatalf("SetInputReceipt: %v", err)
	}
	if err := s.SetChangeReceipt(ctx, run.ID, changeReceiptOne); err != nil {
		t.Fatalf("SetChangeReceipt: %v", err)
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
	if got.InputReceipt != inputReceiptOne || got.ChangeReceipt != changeReceiptOne {
		t.Fatalf("receipts after reopen = %q/%q, want %q/%q", got.InputReceipt, got.ChangeReceipt, inputReceiptOne, changeReceiptOne)
	}
	if got.State != StateAccepted {
		t.Fatalf("state after reopen = %v, want %v", got.State, StateAccepted)
	}
	// The reopened store still refuses drift on both durable receipts.
	if err := reopened.SetInputReceipt(ctx, run.ID, inputReceiptTwo); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("input drift after reopen: err = %v, want %v", err, ErrStateConflict)
	}
	if err := reopened.SetChangeReceipt(ctx, run.ID, changeReceiptTwo); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("change drift after reopen: err = %v, want %v", err, ErrStateConflict)
	}
}

// boundedReceiptColumn is one PRAGMA table_info row, so the additive schema is
// proved from the database itself rather than from a projection that could hide
// a missing column.
type boundedReceiptColumn struct {
	kind         string
	notNull      int
	defaultValue sql.NullString
}

func boundedReceiptColumns(t *testing.T, s *Store) map[string]boundedReceiptColumn {
	t.Helper()
	facts := map[string]boundedReceiptColumn{}
	rows, err := s.db.Query(`PRAGMA table_info(runs)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(runs): %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, primaryKey int
		var name string
		var fact boundedReceiptColumn
		if err := rows.Scan(&cid, &name, &fact.kind, &fact.notNull, &fact.defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		if name == "input_receipt" || name == "change_receipt" {
			facts[name] = fact
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info rows: %v", err)
	}
	return facts
}

func assertBoundedReceiptColumns(t *testing.T, s *Store) {
	t.Helper()
	facts := boundedReceiptColumns(t, s)
	for _, column := range []string{"input_receipt", "change_receipt"} {
		fact, ok := facts[column]
		if !ok {
			t.Fatalf("runs.%s is missing", column)
		}
		if fact.kind != "TEXT" || fact.notNull != 1 {
			t.Fatalf("runs.%s type=%q notnull=%d, want TEXT and 1", column, fact.kind, fact.notNull)
		}
		if !fact.defaultValue.Valid || fact.defaultValue.String != "''" {
			t.Fatalf("runs.%s default=%v, want ''", column, fact.defaultValue)
		}
	}
}

func TestCurrentStoreDeclaresBoundedReceiptColumns(t *testing.T) {
	assertBoundedReceiptColumns(t, testStore(t))
}

func TestOpenAddsBoundedReceiptColumnsToLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-receipts.sqlite3")
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
  unread INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(job_id) REFERENCES jobs(id)
);
INSERT INTO jobs(id,revision,definition,enabled,cursor,updated_at)
VALUES('job','rev7','{"id":"job","revision":7}',1,'2026-08-25T00:00:00Z','2026-08-25T00:00:00Z');
INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at)
VALUES('legacy-run','job','rev7','{"id":"job","revision":7}','manual','accepted','2026-08-25T00:00:00Z','2026-08-25T00:00:00Z');
`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open legacy database: %v", err)
	}
	defer s.Close()
	assertBoundedReceiptColumns(t, s)

	legacy, err := s.GetRun(context.Background(), "legacy-run")
	if err != nil {
		t.Fatalf("GetRun legacy: %v", err)
	}
	if legacy.InputReceipt != "" || legacy.ChangeReceipt != "" {
		t.Fatalf("legacy receipts = %q/%q, want empty", legacy.InputReceipt, legacy.ChangeReceipt)
	}
	if legacy.State != StateAccepted {
		t.Fatalf("legacy state = %v, want %v", legacy.State, StateAccepted)
	}

	// A raw insert that omits the new columns must still satisfy their NOT NULL
	// constraints through the column-level defaults.
	if _, err := s.db.Exec(`INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,updated_at)
VALUES('defaulted-run','job','rev7','{"id":"job","revision":7}','manual','accepted','2026-08-25T02:00:00Z','2026-08-25T02:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	defaulted, err := s.GetRun(context.Background(), "defaulted-run")
	if err != nil {
		t.Fatalf("GetRun defaulted: %v", err)
	}
	if defaulted.InputReceipt != "" || defaulted.ChangeReceipt != "" {
		t.Fatalf("defaulted receipts = %q/%q, want empty", defaulted.InputReceipt, defaulted.ChangeReceipt)
	}

	// The migrated legacy run still accepts a first write for both receipts.
	if err := s.SetInputReceipt(context.Background(), "legacy-run", inputReceiptOne); err != nil {
		t.Fatalf("SetInputReceipt on legacy run: %v", err)
	}
	if err := s.SetChangeReceipt(context.Background(), "legacy-run", changeReceiptOne); err != nil {
		t.Fatalf("SetChangeReceipt on legacy run: %v", err)
	}
	migrated, err := s.GetRun(context.Background(), "legacy-run")
	if err != nil {
		t.Fatalf("GetRun migrated: %v", err)
	}
	if migrated.InputReceipt != inputReceiptOne || migrated.ChangeReceipt != changeReceiptOne {
		t.Fatalf("migrated receipts = %q/%q, want %q/%q", migrated.InputReceipt, migrated.ChangeReceipt, inputReceiptOne, changeReceiptOne)
	}
}
