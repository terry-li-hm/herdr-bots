package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/config"
	_ "modernc.org/sqlite"
)

const (
	MaxProvisioningLease = 2 * time.Minute
	MaxEffectLease       = 10 * time.Minute

	EffectVerifier       = "verifier"
	EffectWorkspaceClose = "workspace_close"

	StateAccepted     = "accepted"
	StateProvisioning = "provisioning"
	StateStarting     = "starting"
	StateRunning      = "running"
	StateSettled      = "settled"
	StateVerifying    = "verifying"
	StateSucceeded    = "succeeded"
	StateFailed       = "failed"
	StateBlocked      = "blocked"
	StateTimedOut     = "timed_out"
	StateCancelled    = "cancelled"
	StateInterrupted  = "interrupted"
)

var (
	ErrStateConflict      = errors.New("run state compare-and-set conflict")
	ErrJobDisabled        = errors.New("event job is disabled")
	ErrJobPaused          = errors.New("event job is paused")
	ErrGlobalPaused       = errors.New("automation execution is globally paused")
	ErrJobRevisionChanged = errors.New("saved job authority changed during synchronization")
	// ErrJobUnreadPaused reports that the opt-in unread-work guard paused the
	// job instead of admitting another run. Marking runs read never resumes the
	// job; only an explicit resume does.
	ErrJobUnreadPaused = errors.New("job paused by the unread terminal-run guard")
)

// Durable pause reasons persisted with the pause timestamp.
const (
	PauseReasonManual             = "manual"
	PauseReasonUnreadTerminalRuns = "unread_terminal_runs"
)

func IsTerminalState(state string) bool { return terminalStates[state] }

var terminalStates = map[string]bool{
	StateSucceeded: true, StateFailed: true, StateBlocked: true,
	StateTimedOut: true, StateCancelled: true, StateInterrupted: true,
}

type Store struct {
	db       *sql.DB
	stateDir string
}

type JobState struct {
	ID             string
	ConfigRevision int
	Revision       string
	Definition     []byte
	Enabled        bool
	Paused         bool
	PauseReason    string
	PauseAt        time.Time
	Completed      bool
	Cursor         time.Time
}

type Run struct {
	ID                     string
	JobID                  string
	JobRevision            string
	Definition             []byte
	Trigger                string
	ScheduledFor           *time.Time
	State                  string
	InfrastructureResult   string
	AgentResult            string
	TaskVerdict            string
	AcceptedAt             time.Time
	AcceptedUnixNano       int64
	UpdatedAt              time.Time
	WorkspaceID            string
	PaneID                 string
	Branch                 string
	WorktreePath           string
	ExecutionMode          string
	CompletionMarker       string
	SourceBaseRevision     string
	SourceRevision         string
	InputContext           string
	ErrorCode              string
	ErrorDetail            string
	ProvisioningOwner      string
	ProvisioningLeaseUntil time.Time
	EffectOwner            string
	EffectClaim            string
	EffectKind             string
	EffectLeaseUntil       time.Time
	EffectReceipt          string
	DiskDevice             string
	DiskReserveGiB         float64
	AcceptanceLane         string
	AcceptanceReason       string
	Unread                 bool
	// ModelAttestation is the durable receipt binding a finished agent
	// invocation to the configured model. It is written once and never
	// rewritten with a different value.
	ModelAttestation string
	// InputReceipt is the durable receipt bounding the inputs a review run was
	// allowed to read. ChangeReceipt is the durable receipt bounding the writes
	// it was allowed to make. Both are written once and never rewritten with a
	// different value.
	InputReceipt  string
	ChangeReceipt string
}

// runColumns is the canonical run projection; every scanner and query must
// use it so persisted admission fields stay coherent across reads.
const runColumns = `id,job_id,job_revision,definition,trigger,scheduled_for,state,infrastructure_result,agent_result,task_verdict,accepted_at,accepted_unix_nano,updated_at,workspace_id,pane_id,branch,worktree_path,execution_mode,completion_marker,source_base_revision,source_revision,input_context,error_code,error_detail,provisioning_owner,provisioning_lease_until,effect_owner,effect_claim,effect_kind,effect_lease_until,effect_receipt,disk_device,disk_reserve_gib,acceptance_lane,acceptance_reason,unread,model_attestation,input_receipt,change_receipt`

type AcceptRequest struct {
	JobID              string
	JobConfigRevision  int
	JobRevision        string
	JobEnabled         bool
	OccurrenceKey      string
	Definition         []byte
	Trigger            string
	ScheduledFor       time.Time
	Overlap            string
	DayStart           time.Time
	DayEnd             time.Time
	MaxRunsPerDay      int
	SourceBaseRevision string
	SourceRevision     string
	InputContext       string
	Now                time.Time
	// MaxUnreadTerminalRuns is the opt-in unread-work guard limit. Zero means
	// no guard; admission behaves exactly as before the guard existed.
	MaxUnreadTerminalRuns int
}

type SuccessfulSource struct {
	JobRevision    string
	SourceRevision string
}

type AcceptResult struct {
	Inserted bool
	Outcome  string
	Run      *Run
	Detail   string
}

type AdmissionDecision struct {
	Admit         bool
	FailureCode   string
	FailureDetail string
	HoldCode      string
	HoldDetail    string
}

type AdmissionCheck func(active []Run) (AdmissionDecision, error)

type Event struct {
	ID        int64
	RunID     string
	FromState string
	ToState   string
	At        time.Time
	Code      string
	Detail    string
}

func Open(path string) (*Store, error) {
	stateDir := os.TempDir()
	var migrationLock *os.File
	if path != ":memory:" {
		stateDir = filepath.Dir(path)
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			return nil, err
		}
		var err error
		migrationLock, err = os.OpenFile(path+".migrate.lock", os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if err := syscall.Flock(int(migrationLock.Fd()), syscall.LOCK_EX); err != nil {
			migrationLock.Close()
			return nil, err
		}
		defer func() {
			_ = syscall.Flock(int(migrationLock.Fd()), syscall.LOCK_UN)
			_ = migrationLock.Close()
		}()
	}
	canonicalStateDir, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	canonicalStateDir, err = filepath.EvalSymlinks(canonicalStateDir)
	if err != nil {
		return nil, fmt.Errorf("resolve state directory: %w", err)
	}
	stateInfo, err := os.Lstat(canonicalStateDir)
	if err != nil {
		return nil, err
	}
	if !stateInfo.IsDir() || stateInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state directory is not a real directory")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, stateDir: canonicalStateDir}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout=250; PRAGMA foreign_keys=ON;`); err != nil {
		conn.Close()
		db.Close()
		return nil, err
	}
	if err := retrySQLiteBusy(ctx, func() error {
		_, err := conn.ExecContext(ctx, `PRAGMA journal_mode=WAL`)
		return err
	}); err != nil {
		conn.Close()
		db.Close()
		return nil, err
	}
	if err := s.migrate(ctx, conn); err != nil {
		conn.Close()
		db.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		conn.Close()
		db.Close()
		return nil, err
	}
	if err := conn.Close(); err != nil {
		db.Close()
		return nil, err
	}
	if path != ":memory:" {
		if err := os.Chmod(path, 0o600); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) StateDir() string { return s.stateDir }

// VerifierReceiptPath derives the only eligible marker path for an effect
// generation. The claim is random lowercase hexadecimal, so the basename can
// never contain a separator or traversal component.
func (s *Store) VerifierReceiptPath(claim string) (string, error) {
	if !validEffectClaim(claim) {
		return "", errors.New("invalid verifier claim")
	}
	if s.stateDir == "" || !filepath.IsAbs(s.stateDir) {
		return "", errors.New("state directory is not canonical")
	}
	return filepath.Join(s.stateDir, "verifiers", "verifier-"+claim+".result"), nil
}

type migrationConnection interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Store) migrate(ctx context.Context, conn *sql.Conn) (err error) {
	if err = retrySQLiteBusy(ctx, func() error {
		_, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`)
		return err
	}); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	_, err = conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  config_revision INTEGER NOT NULL DEFAULT 0,
  revision TEXT NOT NULL,
  definition BLOB NOT NULL,
  enabled INTEGER NOT NULL,
  paused INTEGER NOT NULL DEFAULT 0,
  pause_reason TEXT NOT NULL DEFAULT '',
  pause_at TEXT NOT NULL DEFAULT '',
  completed INTEGER NOT NULL DEFAULT 0,
  cursor TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS occurrences (
  job_id TEXT NOT NULL,
  occurrence_key TEXT NOT NULL,
  scheduled_for TEXT NOT NULL,
  outcome TEXT NOT NULL,
  run_id TEXT,
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  created_unix_nano INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(job_id, occurrence_key),
  FOREIGN KEY(job_id) REFERENCES jobs(id)
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  job_revision TEXT NOT NULL,
  definition BLOB NOT NULL,
  trigger TEXT NOT NULL,
  scheduled_for TEXT,
  state TEXT NOT NULL,
  infrastructure_result TEXT NOT NULL DEFAULT '',
  agent_result TEXT NOT NULL DEFAULT '',
  task_verdict TEXT NOT NULL DEFAULT 'unverified',
  accepted_at TEXT NOT NULL,
  accepted_unix_nano INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '',
  pane_id TEXT NOT NULL DEFAULT '',
  branch TEXT NOT NULL DEFAULT '',
  worktree_path TEXT NOT NULL DEFAULT '',
  execution_mode TEXT NOT NULL DEFAULT 'agent',
  completion_marker TEXT NOT NULL DEFAULT '',
  source_base_revision TEXT NOT NULL DEFAULT '',
  source_revision TEXT NOT NULL DEFAULT '',
  input_context TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_detail TEXT NOT NULL DEFAULT '',
  provisioning_owner TEXT NOT NULL DEFAULT '',
  provisioning_lease_until INTEGER NOT NULL DEFAULT 0,
  effect_owner TEXT NOT NULL DEFAULT '',
  effect_claim TEXT NOT NULL DEFAULT '',
  effect_kind TEXT NOT NULL DEFAULT '',
  effect_lease_until INTEGER NOT NULL DEFAULT 0,
  effect_receipt TEXT NOT NULL DEFAULT '',
  disk_device TEXT NOT NULL DEFAULT '',
  disk_reserve_gib REAL NOT NULL DEFAULT 0,
  acceptance_lane TEXT NOT NULL DEFAULT '',
  acceptance_reason TEXT NOT NULL DEFAULT '',
  unread INTEGER NOT NULL DEFAULT 1,
  model_attestation TEXT NOT NULL DEFAULT '',
  input_receipt TEXT NOT NULL DEFAULT '',
  change_receipt TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(job_id) REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS runs_job_state ON runs(job_id, state);
CREATE INDEX IF NOT EXISTS runs_accepted ON runs(accepted_at DESC);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  at TEXT NOT NULL,
  code TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`)
	if err != nil {
		return err
	}
	columns := []struct{ table, column, definition string }{
		{"jobs", "config_revision", "INTEGER NOT NULL DEFAULT 0"},
		{"runs", "accepted_unix_nano", "INTEGER NOT NULL DEFAULT 0"},
		{"occurrences", "created_unix_nano", "INTEGER NOT NULL DEFAULT 0"},
		{"runs", "execution_mode", "TEXT NOT NULL DEFAULT 'agent'"},
		{"runs", "completion_marker", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "provisioning_owner", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "provisioning_lease_until", "INTEGER NOT NULL DEFAULT 0"},
		{"runs", "effect_owner", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "effect_claim", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "effect_kind", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "effect_lease_until", "INTEGER NOT NULL DEFAULT 0"},
		{"runs", "effect_receipt", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "disk_device", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "disk_reserve_gib", "REAL NOT NULL DEFAULT 0"},
		{"runs", "acceptance_lane", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "acceptance_reason", "TEXT NOT NULL DEFAULT ''"},
		{"events", "code", "TEXT NOT NULL DEFAULT ''"},
		{"occurrences", "occurrence_key", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "source_base_revision", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "source_revision", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "input_context", "TEXT NOT NULL DEFAULT ''"},
		{"jobs", "pause_reason", "TEXT NOT NULL DEFAULT ''"},
		{"jobs", "pause_at", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "model_attestation", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "input_receipt", "TEXT NOT NULL DEFAULT ''"},
		{"runs", "change_receipt", "TEXT NOT NULL DEFAULT ''"},
	}
	for _, column := range columns {
		if err = ensureColumn(ctx, conn, column.table, column.column, column.definition); err != nil {
			return err
		}
	}
	if err = backfillTypedAuthority(ctx, conn); err != nil {
		return err
	}
	if err = backfillAcceptanceClassification(ctx, conn); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `UPDATE occurrences SET occurrence_key=scheduled_for WHERE occurrence_key=''`); err != nil {
		return err
	}
	if err = ensureOccurrenceKeyAuthority(ctx, conn); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS occurrences_job_scheduled ON occurrences(job_id, scheduled_for)`); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS occurrences_job_created_instant ON occurrences(job_id, created_unix_nano)`); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS runs_job_accepted_instant ON runs(job_id, accepted_unix_nano)`); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value) VALUES('admission_lock','0'),('authority_lock','0')`); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}

func retrySQLiteBusy(ctx context.Context, operation func() error) error {
	delay := 10 * time.Millisecond
	for {
		err := operation()
		if err == nil || !isSQLiteBusy(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("sqlite contention exceeded initialization deadline: %w", errors.Join(err, ctx.Err()))
		case <-timer.C:
		}
		if delay < 250*time.Millisecond {
			delay *= 2
		}
	}
}

func isSQLiteBusy(err error) bool {
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		code := coded.Code() & 0xff
		return code == 5 || code == 6
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "sqlite_busy") || strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked")
}

func backfillTypedAuthority(ctx context.Context, conn migrationConnection) error {
	jobRows, err := conn.QueryContext(ctx, `SELECT id,definition FROM jobs WHERE config_revision=0`)
	if err != nil {
		return err
	}
	type jobBackfill struct {
		id       string
		revision int
	}
	var jobs []jobBackfill
	for jobRows.Next() {
		var id string
		var definition []byte
		if err := jobRows.Scan(&id, &definition); err != nil {
			jobRows.Close()
			return err
		}
		revision, err := configRevisionFromDefinition(definition)
		if err != nil {
			jobRows.Close()
			return fmt.Errorf("backfill job %s config revision: %w", id, err)
		}
		jobs = append(jobs, jobBackfill{id: id, revision: revision})
	}
	if err := jobRows.Err(); err != nil {
		jobRows.Close()
		return err
	}
	if err := jobRows.Close(); err != nil {
		return err
	}
	for _, job := range jobs {
		if _, err := conn.ExecContext(ctx, `UPDATE jobs SET config_revision=? WHERE id=? AND config_revision=0`, job.revision, job.id); err != nil {
			return err
		}
	}

	runRows, err := conn.QueryContext(ctx, `SELECT id,accepted_at FROM runs WHERE accepted_unix_nano=0`)
	if err != nil {
		return err
	}
	type runBackfill struct {
		id      string
		instant int64
	}
	var runs []runBackfill
	for runRows.Next() {
		var id, accepted string
		if err := runRows.Scan(&id, &accepted); err != nil {
			runRows.Close()
			return err
		}
		instant, err := parseTime(accepted)
		if err != nil {
			runRows.Close()
			return fmt.Errorf("backfill run %s accepted instant: %w", id, err)
		}
		runs = append(runs, runBackfill{id: id, instant: instant.UnixNano()})
	}
	if err := runRows.Err(); err != nil {
		runRows.Close()
		return err
	}
	if err := runRows.Close(); err != nil {
		return err
	}
	for _, run := range runs {
		if _, err := conn.ExecContext(ctx, `UPDATE runs SET accepted_unix_nano=? WHERE id=? AND accepted_unix_nano=0`, run.instant, run.id); err != nil {
			return err
		}
	}

	occurrenceRows, err := conn.QueryContext(ctx, `SELECT job_id,occurrence_key,scheduled_for,created_at FROM occurrences WHERE created_unix_nano=0`)
	if err != nil {
		return err
	}
	type occurrenceBackfill struct {
		jobID, key, scheduledFor string
		instant                  int64
	}
	var occurrences []occurrenceBackfill
	for occurrenceRows.Next() {
		var jobID, key, scheduledFor, created string
		if err := occurrenceRows.Scan(&jobID, &key, &scheduledFor, &created); err != nil {
			occurrenceRows.Close()
			return err
		}
		instant, err := parseTime(created)
		if err != nil {
			occurrenceRows.Close()
			return fmt.Errorf("backfill occurrence %s/%s created instant: %w", jobID, key, err)
		}
		occurrences = append(occurrences, occurrenceBackfill{jobID: jobID, key: key, scheduledFor: scheduledFor, instant: instant.UnixNano()})
	}
	if err := occurrenceRows.Err(); err != nil {
		occurrenceRows.Close()
		return err
	}
	if err := occurrenceRows.Close(); err != nil {
		return err
	}
	for _, occurrence := range occurrences {
		if _, err := conn.ExecContext(ctx, `UPDATE occurrences SET created_unix_nano=? WHERE job_id=? AND occurrence_key=? AND scheduled_for=? AND created_unix_nano=0`, occurrence.instant, occurrence.jobID, occurrence.key, occurrence.scheduledFor); err != nil {
			return err
		}
	}
	return nil
}

func backfillAcceptanceClassification(ctx context.Context, conn migrationConnection) error {
	runRows, err := conn.QueryContext(ctx, `SELECT id,definition,state,task_verdict,acceptance_lane,acceptance_reason,unread FROM runs WHERE (acceptance_lane='' OR acceptance_reason='') AND state NOT IN ('accepted','provisioning','starting','running','settled','verifying')`)
	if err != nil {
		return err
	}
	type acceptanceBackfill struct {
		id, lane, reason string
		unread           int
	}
	var runs []acceptanceBackfill
	for runRows.Next() {
		var id, state, verdict, lane, reason string
		var definition []byte
		var unread int
		if err := runRows.Scan(&id, &definition, &state, &verdict, &lane, &reason, &unread); err != nil {
			runRows.Close()
			return err
		}
		computedLane, computedReason, reviewUnread := classifyTerminalRun(id, definition, state, verdict)
		laneWasBlank := strings.TrimSpace(lane) == ""
		reasonWasBlank := strings.TrimSpace(reason) == ""
		if laneWasBlank {
			lane = computedLane
		}
		if reasonWasBlank {
			reason = computedReason
		}
		// Only wholly unclassified legacy rows receive the lane's initial inbox
		// state. A partial precursor row may already reflect a later MarkRead or
		// RecordRunEvent update, so migration preserves its unread bit exactly.
		if laneWasBlank && reasonWasBlank {
			unread = boolInt(reviewUnread)
		}
		runs = append(runs, acceptanceBackfill{id: id, lane: lane, reason: reason, unread: unread})
	}
	if err := runRows.Err(); err != nil {
		runRows.Close()
		return err
	}
	if err := runRows.Close(); err != nil {
		return err
	}
	for _, run := range runs {
		if _, err := conn.ExecContext(ctx, `UPDATE runs SET acceptance_lane=CASE WHEN acceptance_lane='' THEN ? ELSE acceptance_lane END, acceptance_reason=CASE WHEN acceptance_reason='' THEN ? ELSE acceptance_reason END, unread=? WHERE id=? AND (acceptance_lane='' OR acceptance_reason='')`, run.lane, run.reason, run.unread, run.id); err != nil {
			return err
		}
	}
	return nil
}

func configRevisionFromDefinition(definition []byte) (int, error) {
	var header struct {
		Revision int `json:"revision"`
	}
	if err := json.Unmarshal(definition, &header); err != nil {
		return 0, err
	}
	if header.Revision == 0 {
		return 1, nil
	}
	if header.Revision < 1 {
		return 0, fmt.Errorf("revision must be positive")
	}
	return header.Revision, nil
}

func ensureOccurrenceKeyAuthority(ctx context.Context, conn migrationConnection) error {
	rows, err := conn.QueryContext(ctx, `PRAGMA table_info(occurrences)`)
	if err != nil {
		return err
	}
	primary := map[int]string{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if primaryKey > 0 {
			primary[primaryKey] = name
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if primary[1] == "job_id" && primary[2] == "occurrence_key" {
		return nil
	}
	_, err = conn.ExecContext(ctx, `
CREATE TABLE occurrences_by_key (
  job_id TEXT NOT NULL,
  occurrence_key TEXT NOT NULL,
  scheduled_for TEXT NOT NULL,
  outcome TEXT NOT NULL,
  run_id TEXT,
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  created_unix_nano INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(job_id, occurrence_key),
  FOREIGN KEY(job_id) REFERENCES jobs(id)
);
INSERT INTO occurrences_by_key(job_id,occurrence_key,scheduled_for,outcome,run_id,detail,created_at,created_unix_nano)
SELECT job_id,occurrence_key,scheduled_for,outcome,run_id,detail,created_at,created_unix_nano FROM occurrences;
DROP TABLE occurrences;
ALTER TABLE occurrences_by_key RENAME TO occurrences;
`)
	return err
}

func ensureColumn(ctx context.Context, conn migrationConnection, table, column, definition string) error {
	rows, err := conn.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		found = found || name == column
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = conn.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition)
	return err
}

func (s *Store) SyncJob(ctx context.Context, id, revision string, definition []byte, enabled bool, now time.Time) (JobState, error) {
	configRevision, err := configRevisionFromDefinition(definition)
	if err != nil {
		return JobState{}, err
	}
	return s.SyncJobAuthority(ctx, id, configRevision, revision, definition, enabled, now)
}

// SyncJobAuthority preserves mutable current-job synchronization while fencing
// stale or conflicting configuration snapshots by explicit monotonic revision.
func (s *Store) SyncJobAuthority(ctx context.Context, id string, configRevision int, snapshotHash string, definition []byte, enabled bool, now time.Time) (JobState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return JobState{}, err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return JobState{}, err
	}
	if err := syncJobAuthorityTx(ctx, tx, id, configRevision, snapshotHash, definition, enabled, now); err != nil {
		return JobState{}, err
	}
	if err := tx.Commit(); err != nil {
		return JobState{}, err
	}
	return s.Job(ctx, id)
}

func syncJobAuthorityTx(ctx context.Context, tx *sql.Tx, id string, configRevision int, snapshotHash string, definition []byte, enabled bool, now time.Time) error {
	definitionRevision, err := configRevisionFromDefinition(definition)
	if err != nil {
		return fmt.Errorf("%w: job %s snapshot revision is invalid: %v", ErrJobRevisionChanged, id, err)
	}
	if configRevision < 1 || definitionRevision != configRevision {
		return fmt.Errorf("%w: job %s config revision %d does not match snapshot revision %d", ErrJobRevisionChanged, id, configRevision, definitionRevision)
	}
	if snapshotHash == "" {
		return fmt.Errorf("%w: job %s snapshot hash is empty", ErrJobRevisionChanged, id)
	}
	var savedRevision int
	var savedHash string
	var savedEnabled int
	err = tx.QueryRowContext(ctx, `SELECT config_revision,revision,enabled FROM jobs WHERE id=?`, id).Scan(&savedRevision, &savedHash, &savedEnabled)
	if errors.Is(err, sql.ErrNoRows) {
		stamp := formatTime(now)
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs(id,config_revision,revision,definition,enabled,cursor,updated_at) VALUES(?,?,?,?,?,?,?)`, id, configRevision, snapshotHash, definition, boolInt(enabled), stamp, stamp)
		return err
	}
	if err != nil {
		return err
	}
	if configRevision < savedRevision {
		return fmt.Errorf("%w: job %s revision %d is older than saved revision %d", ErrJobRevisionChanged, id, configRevision, savedRevision)
	}
	if configRevision == savedRevision && (snapshotHash != savedHash || boolInt(enabled) != savedEnabled) {
		return fmt.Errorf("%w: job %s revision %d has a different snapshot hash", ErrJobRevisionChanged, id, configRevision)
	}
	if configRevision == savedRevision {
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET updated_at=? WHERE id=?`, formatTime(now), id)
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET config_revision=?,revision=?,definition=?,enabled=?,completed=0,updated_at=? WHERE id=?`, configRevision, snapshotHash, definition, boolInt(enabled), formatTime(now), id)
	return err
}

func (s *Store) Job(ctx context.Context, id string) (JobState, error) {
	var out JobState
	var enabled, paused, completed int
	var cursor, pauseReason, pauseAt string
	err := s.db.QueryRowContext(ctx, `SELECT id,config_revision,revision,definition,enabled,paused,completed,cursor,pause_reason,pause_at FROM jobs WHERE id=?`, id).
		Scan(&out.ID, &out.ConfigRevision, &out.Revision, &out.Definition, &enabled, &paused, &completed, &cursor, &pauseReason, &pauseAt)
	if err != nil {
		return out, err
	}
	out.Enabled, out.Paused, out.Completed = enabled != 0, paused != 0, completed != 0
	out.PauseReason = pauseReason
	if pauseAt != "" {
		out.PauseAt, err = parseTime(pauseAt)
		if err != nil {
			return out, err
		}
	}
	out.Cursor, err = parseTime(cursor)
	return out, err
}

func (s *Store) SetCursor(ctx context.Context, id string, cursor time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET cursor=?, updated_at=? WHERE id=?`, formatTime(cursor), formatTime(time.Now()), id)
	return err
}

func (s *Store) SetCursorIfAuthority(ctx context.Context, id string, configRevision int, snapshotHash string, cursor time.Time, requireRunnable bool) error {
	return s.updateJobIfAuthority(ctx, id, configRevision, snapshotHash, requireRunnable, func(tx *sql.Tx) error {
		var saved string
		if err := tx.QueryRowContext(ctx, `SELECT cursor FROM jobs WHERE id=?`, id).Scan(&saved); err != nil {
			return err
		}
		current, err := parseTime(saved)
		if err != nil {
			return err
		}
		if !cursor.After(current) {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET cursor=?, updated_at=? WHERE id=?`, formatTime(cursor), formatTime(time.Now()), id)
		return err
	})
}

func (s *Store) SetCompleted(ctx context.Context, id string, completed bool) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET completed=?, updated_at=? WHERE id=?`, boolInt(completed), formatTime(time.Now()), id)
	return err
}

func (s *Store) SetCompletedIfAuthority(ctx context.Context, id string, configRevision int, snapshotHash string, completed bool) error {
	return s.updateJobIfAuthority(ctx, id, configRevision, snapshotHash, true, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE jobs SET completed=?, updated_at=? WHERE id=?`, boolInt(completed), formatTime(time.Now()), id)
		return err
	})
}

func (s *Store) updateJobIfAuthority(ctx context.Context, id string, configRevision int, snapshotHash string, requireRunnable bool, update func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return err
	}
	if err := requireJobAuthorityTx(ctx, tx, id, configRevision, snapshotHash, requireRunnable); err != nil {
		return err
	}
	if err := update(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// SetPaused changes a job's manual pause state. Pausing records the manual
// reason and timestamp; resuming clears both. The unread-work guard records
// its own durable reason inside the admission transaction instead.
func (s *Store) SetPaused(ctx context.Context, id string, paused bool) error {
	if paused {
		now := time.Now()
		res, err := s.db.ExecContext(ctx, `UPDATE jobs SET paused=1,pause_reason=?,pause_at=?,updated_at=? WHERE id=?`, PauseReasonManual, formatTime(now), formatTime(now), id)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	}
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET paused=0,pause_reason='',pause_at='',updated_at=? WHERE id=?`, formatTime(time.Now()), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) SetGlobalPaused(ctx context.Context, paused bool) error {
	value := "0"
	if paused {
		value = "1"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('global_paused',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, value)
	return err
}

func (s *Store) GlobalPaused(ctx context.Context) (bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='global_paused'`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return value == "1", err
}

func (s *Store) RecordMissed(ctx context.Context, jobID, occurrenceKey string, at, now time.Time, detail string) (bool, error) {
	return s.RecordOccurrence(ctx, jobID, occurrenceKey, at, now, "missed", detail)
}

func (s *Store) RecordOccurrence(ctx context.Context, jobID, occurrenceKey string, at, now time.Time, outcome, detail string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO occurrences(job_id,occurrence_key,scheduled_for,outcome,detail,created_at,created_unix_nano) VALUES(?,?,?,?,?,?,?)`, jobID, occurrenceKey, formatTime(at), outcome, detail, formatTime(now), now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) AcceptOccurrence(ctx context.Context, req AcceptRequest) (AcceptResult, error) {
	if req.OccurrenceKey == "" {
		return AcceptResult{}, fmt.Errorf("occurrence key is required")
	}
	// AcceptOccurrence is not authority-fenced. The unread-work guard must
	// recheck job authority inside the same serialized admission transaction,
	// so fail closed rather than pausing or admitting on stale authority.
	if req.MaxUnreadTerminalRuns != 0 {
		return AcceptResult{}, errors.New("unread terminal-run guard requires authority-fenced admission")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptResult{}, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return AcceptResult{}, err
	}
	return acceptOccurrenceTx(ctx, tx, req)
}

func (s *Store) AcceptScheduledOccurrence(ctx context.Context, req AcceptRequest) (AcceptResult, error) {
	if req.OccurrenceKey == "" || req.Trigger == "event" {
		return AcceptResult{}, errors.New("scheduled acceptance requires a non-event occurrence key")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptResult{}, err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return AcceptResult{}, err
	}
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return AcceptResult{}, err
	}
	if err := requireJobAuthorityTx(ctx, tx, req.JobID, req.JobConfigRevision, req.JobRevision, true); err != nil {
		return AcceptResult{}, err
	}
	return acceptOccurrenceTx(ctx, tx, req)
}

func (s *Store) RecordOccurrenceIfAuthority(ctx context.Context, req AcceptRequest, outcome, detail string) (bool, error) {
	if req.OccurrenceKey == "" {
		return false, errors.New("occurrence key is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return false, err
	}
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	if err := requireJobAuthorityTx(ctx, tx, req.JobID, req.JobConfigRevision, req.JobRevision, true); err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO occurrences(job_id,occurrence_key,scheduled_for,outcome,detail,created_at,created_unix_nano) VALUES(?,?,?,?,?,?,?)`, req.JobID, req.OccurrenceKey, formatTime(req.ScheduledFor), outcome, detail, formatTime(req.Now), req.Now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n == 1, nil
}

// EnqueueEvent checks durable pause and enablement authority in the same
// serialized transaction that accepts the event occurrence. Duplicate event
// identities return the original outcome and run before current pause state is
// considered, so idempotent delivery remains deterministic.
func (s *Store) EnqueueEvent(ctx context.Context, req AcceptRequest) (AcceptResult, error) {
	if req.Trigger != "event" || req.OccurrenceKey == "" {
		return AcceptResult{}, errors.New("event enqueue requires an event occurrence key and trigger")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptResult{}, err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return AcceptResult{}, err
	}
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return AcceptResult{}, err
	}
	existing, found, err := existingOccurrence(ctx, tx, req.JobID, req.OccurrenceKey)
	if err != nil {
		return AcceptResult{}, err
	}
	if found {
		syncErr := syncJobAuthorityTx(ctx, tx, req.JobID, req.JobConfigRevision, req.JobRevision, req.Definition, req.JobEnabled, req.Now)
		if syncErr != nil {
			if errors.Is(syncErr, ErrJobRevisionChanged) {
				// The immutable occurrence is authoritative for an exact retry.
				// Roll back the conflicting synchronization attempt and return
				// the original outcome without changing current job authority.
				return existing, nil
			}
			return AcceptResult{}, syncErr
		}
		if err := tx.Commit(); err != nil {
			return AcceptResult{}, err
		}
		return existing, nil
	}
	if err := syncJobAuthorityTx(ctx, tx, req.JobID, req.JobConfigRevision, req.JobRevision, req.Definition, req.JobEnabled, req.Now); err != nil {
		return AcceptResult{}, err
	}

	var enabled, paused int
	if err := tx.QueryRowContext(ctx, `SELECT enabled,paused FROM jobs WHERE id=?`, req.JobID).Scan(&enabled, &paused); err != nil {
		return AcceptResult{}, err
	}
	var refusal error
	if enabled == 0 {
		refusal = fmt.Errorf("%w: job %s", ErrJobDisabled, req.JobID)
	} else if paused != 0 {
		refusal = fmt.Errorf("%w: job %s", ErrJobPaused, req.JobID)
	} else {
		var globalPaused string
		err = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='global_paused'`).Scan(&globalPaused)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return AcceptResult{}, err
		}
		if globalPaused == "1" {
			refusal = ErrGlobalPaused
		}
	}
	if refusal != nil {
		if err := tx.Commit(); err != nil {
			return AcceptResult{}, err
		}
		return AcceptResult{}, refusal
	}
	return acceptOccurrenceTx(ctx, tx, req)
}

// countUnreadTerminalRunsTx counts only unread runs already in terminal
// states. Runs begin unread in nonterminal states, so accepted, provisioning,
// starting, running, settled, and verifying runs never count.
func countUnreadTerminalRunsTx(ctx context.Context, tx *sql.Tx, jobID string) (int, error) {
	var unread int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE job_id=? AND unread=1 AND state IN ('succeeded','failed','blocked','timed_out','cancelled','interrupted')`, jobID).Scan(&unread); err != nil {
		return 0, err
	}
	return unread, nil
}

// enforceUnreadGuardTx is the unread-work admission guard. It must run inside
// the same serialized admission transaction that already fenced job authority,
// so a stale revision or concurrent authority change can neither pause the job
// nor admit work. When the configured limit of unread terminal runs is reached
// it atomically pauses the job and commits without creating a run, an
// occurrence, or any other side effect; repeated evaluation is idempotent.
func enforceUnreadGuardTx(ctx context.Context, tx *sql.Tx, req AcceptRequest) (AcceptResult, bool, error) {
	if req.MaxUnreadTerminalRuns <= 0 {
		return AcceptResult{}, false, nil
	}
	unread, err := countUnreadTerminalRunsTx(ctx, tx, req.JobID)
	if err != nil {
		return AcceptResult{}, false, err
	}
	if unread < req.MaxUnreadTerminalRuns {
		return AcceptResult{}, false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET paused=1,pause_reason=?,pause_at=?,updated_at=? WHERE id=?`, PauseReasonUnreadTerminalRuns, formatTime(req.Now), formatTime(req.Now), req.JobID); err != nil {
		return AcceptResult{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptResult{}, false, err
	}
	detail := fmt.Sprintf("%d unread terminal runs reached the configured limit %d; job paused until explicit resume", unread, req.MaxUnreadTerminalRuns)
	return AcceptResult{Outcome: "paused_unread_limit", Detail: detail}, true, nil
}

func requireJobAuthorityTx(ctx context.Context, tx *sql.Tx, id string, configRevision int, snapshotHash string, requireRunnable bool) error {
	var savedRevision, enabled, paused int
	var savedHash string
	if err := tx.QueryRowContext(ctx, `SELECT config_revision,revision,enabled,paused FROM jobs WHERE id=?`, id).Scan(&savedRevision, &savedHash, &enabled, &paused); err != nil {
		return err
	}
	if savedRevision != configRevision || savedHash != snapshotHash {
		return fmt.Errorf("%w: job %s expected revision %d/%s but current authority is %d/%s", ErrJobRevisionChanged, id, configRevision, snapshotHash, savedRevision, savedHash)
	}
	if !requireRunnable {
		return nil
	}
	if enabled == 0 {
		return fmt.Errorf("%w: job %s", ErrJobDisabled, id)
	}
	if paused != 0 {
		return fmt.Errorf("%w: job %s", ErrJobPaused, id)
	}
	var globalPaused string
	err := tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='global_paused'`).Scan(&globalPaused)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if globalPaused == "1" {
		return ErrGlobalPaused
	}
	return nil
}

func lockAuthorityWrites(ctx context.Context, tx *sql.Tx) error {
	res, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='authority_lock'`)
	if err != nil {
		return err
	}
	if count, _ := res.RowsAffected(); count != 1 {
		return errors.New("authority serialization lock is missing")
	}
	return nil
}

func lockOccurrenceWrites(ctx context.Context, tx *sql.Tx) error {
	res, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='admission_lock'`)
	if err != nil {
		return err
	}
	if count, _ := res.RowsAffected(); count != 1 {
		return errors.New("occurrence serialization lock is missing")
	}
	return nil
}

func existingOccurrence(ctx context.Context, tx *sql.Tx, jobID, occurrenceKey string) (AcceptResult, bool, error) {
	var outcome, runID, detail string
	err := tx.QueryRowContext(ctx, `SELECT outcome,COALESCE(run_id,''),detail FROM occurrences WHERE job_id=? AND occurrence_key=?`, jobID, occurrenceKey).Scan(&outcome, &runID, &detail)
	if errors.Is(err, sql.ErrNoRows) {
		return AcceptResult{}, false, nil
	}
	if err != nil {
		return AcceptResult{}, false, err
	}
	result := AcceptResult{Outcome: outcome, Detail: detail}
	if runID != "" {
		run, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, runID))
		if err != nil {
			return AcceptResult{}, false, err
		}
		result.Run = &run
	}
	return result, true, nil
}

func acceptOccurrenceTx(ctx context.Context, tx *sql.Tx, req AcceptRequest) (AcceptResult, error) {
	if existing, found, err := existingOccurrence(ctx, tx, req.JobID, req.OccurrenceKey); err != nil {
		return AcceptResult{}, err
	} else if found {
		return existing, nil
	}

	// The unread-work guard runs before any admission decision records an
	// occurrence, so a tripped limit creates no run, occurrence, or other side
	// effect: it only pauses the job atomically.
	if guard, tripped, err := enforceUnreadGuardTx(ctx, tx, req); err != nil {
		return AcceptResult{}, err
	} else if tripped {
		return guard, nil
	}

	dayEnd := req.DayEnd
	if dayEnd.IsZero() {
		dayEnd = req.DayStart.Add(24 * time.Hour)
	}
	var dayCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE job_id=? AND accepted_unix_nano>=? AND accepted_unix_nano<?`, req.JobID, req.DayStart.UnixNano(), dayEnd.UnixNano()).Scan(&dayCount); err != nil {
		return AcceptResult{}, err
	}
	if dayCount >= req.MaxRunsPerDay {
		return finishOccurrence(tx, req, "skipped_limit", "daily run limit reached", "")
	}

	var executing, queued int
	if err := tx.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN state='accepted' THEN 0 ELSE 1 END),0),
		COALESCE(SUM(CASE WHEN state='accepted' THEN 1 ELSE 0 END),0)
		FROM runs WHERE job_id=? AND state IN ('accepted','provisioning','starting','running','settled','verifying')`, req.JobID).Scan(&executing, &queued); err != nil {
		return AcceptResult{}, err
	}
	if req.Overlap == "forbid" && executing+queued > 0 {
		return finishOccurrence(tx, req, "skipped_overlap", "previous run is still active", "")
	}
	if req.Overlap == "queue_one" && queued > 0 {
		return finishOccurrence(tx, req, "skipped_overlap", "one run is already queued", "")
	}

	id, err := newRunID(req.JobID, req.Now)
	if err != nil {
		return AcceptResult{}, err
	}
	run := Run{ID: id, JobID: req.JobID, JobRevision: req.JobRevision, Definition: req.Definition, Trigger: req.Trigger, ScheduledFor: &req.ScheduledFor, State: StateAccepted, TaskVerdict: "unverified", AcceptedAt: req.Now, AcceptedUnixNano: req.Now.UnixNano(), UpdatedAt: req.Now, SourceBaseRevision: req.SourceBaseRevision, SourceRevision: req.SourceRevision, InputContext: req.InputContext, Unread: true}
	_, err = tx.ExecContext(ctx, `INSERT INTO runs(id,job_id,job_revision,definition,trigger,scheduled_for,state,accepted_at,accepted_unix_nano,updated_at,source_base_revision,source_revision,input_context) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, req.JobID, req.JobRevision, req.Definition, req.Trigger, formatTime(req.ScheduledFor), StateAccepted, formatTime(req.Now), req.Now.UnixNano(), formatTime(req.Now), req.SourceBaseRevision, req.SourceRevision, req.InputContext)
	if err != nil {
		return AcceptResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,'',?,?,?)`, id, StateAccepted, formatTime(req.Now), "occurrence accepted"); err != nil {
		return AcceptResult{}, err
	}
	result, err := finishOccurrence(tx, req, "accepted", "", id)
	if err != nil {
		return AcceptResult{}, err
	}
	result.Run = &run
	return result, nil
}

func (s *Store) CreateManualRunIfAuthority(ctx context.Context, req AcceptRequest) (Run, error) {
	id, err := newRunID(req.JobID, req.Now)
	if err != nil {
		return Run{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback()
	if err := lockAuthorityWrites(ctx, tx); err != nil {
		return Run{}, err
	}
	if err := requireJobAuthorityTx(ctx, tx, req.JobID, req.JobConfigRevision, req.JobRevision, true); err != nil {
		return Run{}, err
	}
	// The unread-work guard rechecks inside the same authority-fenced manual
	// admission transaction. A tripped guard pauses the job atomically and
	// creates nothing.
	if req.MaxUnreadTerminalRuns > 0 {
		unread, countErr := countUnreadTerminalRunsTx(ctx, tx, req.JobID)
		if countErr != nil {
			return Run{}, countErr
		}
		if unread >= req.MaxUnreadTerminalRuns {
			if _, err = tx.ExecContext(ctx, `UPDATE jobs SET paused=1,pause_reason=?,pause_at=?,updated_at=? WHERE id=?`, PauseReasonUnreadTerminalRuns, formatTime(req.Now), formatTime(req.Now), req.JobID); err != nil {
				return Run{}, err
			}
			if err := tx.Commit(); err != nil {
				return Run{}, err
			}
			return Run{}, fmt.Errorf("%w: job %s has %d unread terminal runs at the configured limit %d", ErrJobUnreadPaused, req.JobID, unread, req.MaxUnreadTerminalRuns)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,accepted_unix_nano,updated_at) VALUES(?,?,?,?, 'manual',?,?,?,?)`, id, req.JobID, req.JobRevision, req.Definition, StateAccepted, formatTime(req.Now), req.Now.UnixNano(), formatTime(req.Now)); err != nil {
		return Run{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,'',?,?,?)`, id, StateAccepted, formatTime(req.Now), "manual run accepted"); err != nil {
		return Run{}, err
	}
	if err := tx.Commit(); err != nil {
		return Run{}, err
	}
	return Run{ID: id, JobID: req.JobID, JobRevision: req.JobRevision, Definition: req.Definition, Trigger: "manual", State: StateAccepted, TaskVerdict: "unverified", AcceptedAt: req.Now, AcceptedUnixNano: req.Now.UnixNano(), UpdatedAt: req.Now, Unread: true}, nil
}

func (s *Store) CreateManualRun(ctx context.Context, jobID, revision string, definition []byte, now time.Time) (Run, error) {
	return s.createAttendedRun(ctx, jobID, revision, definition, "manual", now)
}

func (s *Store) CreateCanaryRun(ctx context.Context, jobID, revision string, definition []byte, now time.Time) (Run, error) {
	return s.createAttendedRun(ctx, jobID, revision, definition, "canary", now)
}

func (s *Store) createAttendedRun(ctx context.Context, jobID, revision string, definition []byte, trigger string, now time.Time) (Run, error) {
	if trigger != "manual" && trigger != "canary" {
		return Run{}, fmt.Errorf("unsupported attended trigger %q", trigger)
	}
	id, err := newRunID(jobID, now)
	if err != nil {
		return Run{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO runs(id,job_id,job_revision,definition,trigger,state,accepted_at,accepted_unix_nano,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`, id, jobID, revision, definition, trigger, StateAccepted, formatTime(now), now.UnixNano(), formatTime(now)); err != nil {
		return Run{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,'',?,?,?)`, id, StateAccepted, formatTime(now), trigger+" run accepted"); err != nil {
		return Run{}, err
	}
	if err := tx.Commit(); err != nil {
		return Run{}, err
	}
	return Run{ID: id, JobID: jobID, JobRevision: revision, Definition: definition, Trigger: trigger, State: StateAccepted, TaskVerdict: "unverified", AcceptedAt: now, AcceptedUnixNano: now.UnixNano(), UpdatedAt: now, Unread: true}, nil
}

func finishOccurrence(tx *sql.Tx, req AcceptRequest, outcome, detail, runID string) (AcceptResult, error) {
	_, err := tx.Exec(`INSERT INTO occurrences(job_id,occurrence_key,scheduled_for,outcome,run_id,detail,created_at,created_unix_nano) VALUES(?,?,?,?,?,?,?,?)`, req.JobID, req.OccurrenceKey, formatTime(req.ScheduledFor), outcome, nullable(runID), detail, formatTime(req.Now), req.Now.UnixNano())
	if err != nil {
		return AcceptResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptResult{}, err
	}
	return AcceptResult{Inserted: true, Outcome: outcome, Detail: detail}, nil
}

func classifyTerminalRun(runID string, definition []byte, state, verdict string) (string, string, bool) {
	normalizedVerdict := strings.TrimSpace(verdict)
	if normalizedVerdict == "" {
		normalizedVerdict = "unverified"
	}
	if normalizedVerdict == "failed" {
		return config.AcceptanceMandatory, "verifier_failed", true
	}
	if state != StateSucceeded {
		if strings.TrimSpace(state) == "" {
			return config.AcceptanceMandatory, "state_unknown", true
		}
		switch state {
		case StateFailed, StateBlocked, StateTimedOut, StateCancelled, StateInterrupted:
			return config.AcceptanceMandatory, "state_" + state, true
		case StateAccepted, StateProvisioning, StateStarting, StateRunning, StateSettled, StateVerifying:
			return config.AcceptanceMandatory, "state_nonterminal", true
		default:
			return config.AcceptanceMandatory, "state_unknown", true
		}
	}
	if normalizedVerdict != "passed" {
		return config.AcceptanceMandatory, "unverified", true
	}
	var snapshot config.Job
	if err := json.Unmarshal(definition, &snapshot); err != nil {
		return config.AcceptanceMandatory, "snapshot_invalid", true
	}
	return snapshot.ClassifyTerminalRun(runID, state, normalizedVerdict)
}

func terminalAcceptanceTx(ctx context.Context, tx *sql.Tx, id, state, verdict string) (string, string, int, error) {
	var definition []byte
	if err := tx.QueryRowContext(ctx, `SELECT definition FROM runs WHERE id=?`, id).Scan(&definition); err != nil {
		return "", "", 0, err
	}
	lane, reason, unread := classifyTerminalRun(id, definition, state, verdict)
	return lane, reason, boolInt(unread), nil
}

func (s *Store) Transition(ctx context.Context, id, from, to, detail string, now time.Time) error {
	if terminalStates[from] {
		return fmt.Errorf("run %s is terminal in %s", id, from)
	}
	if terminalStates[to] {
		return fmt.Errorf("run %s cannot transition to terminal state %s; use Finish", id, to)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?, updated_at=? WHERE id=? AND state=? AND effect_owner=''`, to, formatTime(now), id, from)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: run %s did not transition from %s to %s", ErrStateConflict, id, from, to)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, from, to, formatTime(now), detail); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Finish(ctx context.Context, id, from, state, infra, agent, verdict, code, detail string, now time.Time) error {
	if !terminalStates[state] {
		return fmt.Errorf("%s is not terminal", state)
	}
	if terminalStates[from] {
		return fmt.Errorf("run %s is already terminal", id)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, state, verdict)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?, infrastructure_result=?, agent_result=?, task_verdict=?, error_code=?, error_detail=?, provisioning_owner='', provisioning_lease_until=0, effect_owner='', effect_claim='', effect_kind='', effect_lease_until=0, effect_receipt='', acceptance_lane=?, acceptance_reason=?, unread=?, updated_at=? WHERE id=? AND state=? AND effect_owner=''`, state, infra, agent, verdict, code, detail, lane, reason, unread, formatTime(now), id, from)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: run %s did not finish from %s", ErrStateConflict, id, from)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, from, state, formatTime(now), detail); err != nil {
		return err
	}
	return tx.Commit()
}

// ClaimEffect acquires only an effect with no prior invocation. An expired
// effect must go through effect-specific recovery before it can be reclaimed,
// so an ambiguous external call is never retried automatically.
func (s *Store) ClaimEffect(ctx context.Context, id, from, kind, owner string, now, leaseUntil time.Time) (string, error) {
	if terminalStates[from] {
		return "", fmt.Errorf("run %s is already terminal", id)
	}
	if err := validateEffectLease(kind, owner, now, leaseUntil); err != nil {
		return "", err
	}
	claim, err := newEffectClaim()
	if err != nil {
		return "", err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET effect_owner=?,effect_claim=?,effect_kind=?,effect_lease_until=?,effect_receipt='',updated_at=? WHERE id=? AND state=? AND effect_owner='' AND effect_claim='' AND effect_kind='' AND effect_receipt='' AND (provisioning_owner='' OR provisioning_lease_until<=?)`, owner, claim, kind, leaseUntil.UnixNano(), formatTime(now), id, from, now.UnixNano())
	if err != nil {
		return "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return "", nil
	}
	return claim, nil
}

func (s *Store) ClaimWorkspaceClose(ctx context.Context, id, from, owner, intent string, now, leaseUntil time.Time) (string, error) {
	if terminalStates[from] {
		return "", fmt.Errorf("run %s is already terminal", id)
	}
	if intent == "" {
		return "", errors.New("workspace-close intent is required")
	}
	if err := validateEffectLease(EffectWorkspaceClose, owner, now, leaseUntil); err != nil {
		return "", err
	}
	claim, err := newEffectClaim()
	if err != nil {
		return "", err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET effect_owner=?,effect_claim=?,effect_kind=?,effect_lease_until=?,effect_receipt=?,updated_at=? WHERE id=? AND state=? AND effect_owner='' AND effect_claim='' AND effect_kind='' AND effect_receipt='' AND (provisioning_owner='' OR provisioning_lease_until<=?)`, owner, claim, EffectWorkspaceClose, leaseUntil.UnixNano(), intent, formatTime(now), id, from, now.UnixNano())
	if err != nil {
		return "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return "", nil
	}
	return claim, nil
}

// ClaimLateProvisioningCleanup atomically adopts a known workspace only when
// no durable receipt or cleanup owner already exists. A delayed provisioning
// result may arrive after reconciliation terminalized the run; terminal rows
// with no workspace identity are eligible and retain their original verdict.
func (s *Store) ClaimLateProvisioningCleanup(ctx context.Context, id, owner, workspaceID, paneID, branch, path, terminalState, infrastructure, agent, verdict, code, detail string, now, leaseUntil time.Time) (Run, string, error) {
	var zero Run
	if workspaceID == "" || branch == "" {
		return zero, "", errors.New("late provisioning cleanup requires workspace identity and branch")
	}
	if err := validateEffectLease(EffectWorkspaceClose, owner, now, leaseUntil); err != nil {
		return zero, "", err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, "", err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return zero, "", err
	}
	run, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, id))
	if err != nil {
		return zero, "", err
	}
	if run.WorkspaceID != "" || run.EffectKind != "" {
		if err := tx.Commit(); err != nil {
			return zero, "", err
		}
		return run, "", nil
	}
	eligible := run.State == StateProvisioning && (run.ProvisioningOwner == owner || !run.ProvisioningLeaseUntil.After(now))
	if terminalStates[run.State] {
		eligible = true
		terminalState, infrastructure, agent, verdict, code, detail = run.State, run.InfrastructureResult, run.AgentResult, run.TaskVerdict, run.ErrorCode, run.ErrorDetail
	}
	if !eligible {
		if err := tx.Commit(); err != nil {
			return zero, "", err
		}
		return run, "", nil
	}
	intentPayload := struct {
		TerminalState    string `json:"terminal_state"`
		Infrastructure   string `json:"infrastructure"`
		Agent            string `json:"agent"`
		Verdict          string `json:"verdict"`
		Code             string `json:"code"`
		Detail           string `json:"detail"`
		AcceptanceLane   string `json:"acceptance_lane,omitempty"`
		AcceptanceReason string `json:"acceptance_reason,omitempty"`
		Unread           *bool  `json:"unread,omitempty"`
	}{TerminalState: terminalState, Infrastructure: infrastructure, Agent: agent, Verdict: verdict, Code: code, Detail: detail}
	if terminalStates[run.State] {
		intentPayload.AcceptanceLane = run.AcceptanceLane
		intentPayload.AcceptanceReason = run.AcceptanceReason
		unread := run.Unread
		intentPayload.Unread = &unread
	}
	intent, err := json.Marshal(intentPayload)
	if err != nil {
		return zero, "", err
	}
	claim, err := newEffectClaim()
	if err != nil {
		return zero, "", err
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET workspace_id=?,pane_id=?,branch=?,worktree_path=?,effect_owner=?,effect_claim=?,effect_kind=?,effect_lease_until=?,effect_receipt=?,unread=1,updated_at=? WHERE id=? AND state=? AND workspace_id='' AND effect_owner='' AND effect_claim='' AND effect_kind='' AND effect_receipt=''`, workspaceID, paneID, branch, path, owner, claim, EffectWorkspaceClose, leaseUntil.UnixNano(), string(intent), stamp, id, run.State)
	if err != nil {
		return zero, "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return zero, "", nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, run.State, run.State, stamp, "late_workspace_cleanup_claimed", "known provisioning workspace adopted for durable cleanup"); err != nil {
		return zero, "", err
	}
	if err := tx.Commit(); err != nil {
		return zero, "", err
	}
	run.WorkspaceID, run.PaneID, run.Branch, run.WorktreePath = workspaceID, paneID, branch, path
	run.EffectOwner, run.EffectClaim, run.EffectKind, run.EffectLeaseUntil, run.EffectReceipt = owner, claim, EffectWorkspaceClose, leaseUntil, string(intent)
	return run, claim, nil
}

// ReclaimEffect fences an expired workspace-close invocation after recovery
// has independently proved the workspace is already gone. Verifiers are never
// reclaimed because they have no durable external completion marker.
func (s *Store) ReclaimEffect(ctx context.Context, id, from, kind, owner string, now, leaseUntil time.Time) (string, error) {
	if kind != EffectWorkspaceClose {
		return "", fmt.Errorf("effect %q cannot be reclaimed", kind)
	}
	if err := validateEffectLease(kind, owner, now, leaseUntil); err != nil {
		return "", err
	}
	claim, err := newEffectClaim()
	if err != nil {
		return "", err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET effect_owner=?,effect_claim=?,effect_lease_until=?,updated_at=? WHERE id=? AND state=? AND effect_owner<>'' AND effect_kind=? AND effect_lease_until<=? AND (provisioning_owner='' OR provisioning_lease_until<=?)`, owner, claim, leaseUntil.UnixNano(), formatTime(now), id, from, kind, now.UnixNano(), now.UnixNano())
	if err != nil {
		return "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return "", nil
	}
	return claim, nil
}

// ClaimVerifier atomically persists a fresh generation and its claim-derived
// marker path. No caller can know or touch that path before winning this CAS.
func (s *Store) ClaimVerifier(ctx context.Context, id, owner string, now, leaseUntil time.Time) (string, string, error) {
	if err := validateEffectLease(EffectVerifier, owner, now, leaseUntil); err != nil {
		return "", "", err
	}
	claim, err := newEffectClaim()
	if err != nil {
		return "", "", err
	}
	receipt, err := s.VerifierReceiptPath(claim)
	if err != nil {
		return "", "", err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,effect_owner=?,effect_claim=?,effect_kind=?,effect_lease_until=?,effect_receipt=?,updated_at=? WHERE id=? AND state=? AND effect_owner='' AND effect_claim='' AND effect_kind='' AND effect_receipt=''`, StateVerifying, owner, claim, EffectVerifier, leaseUntil.UnixNano(), receipt, stamp, id, StateSettled)
	if err != nil {
		return "", "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return "", "", nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateSettled, StateVerifying, stamp, "deterministic verifier claimed"); err != nil {
		return "", "", err
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return claim, receipt, nil
}

func (s *Store) FinishEffect(ctx context.Context, id, from, owner, claim, kind, state, infra, agent, verdict, code, detail string, now time.Time) (bool, error) {
	if claim == "" {
		return false, errors.New("effect claim is required")
	}
	if !terminalStates[state] {
		return false, fmt.Errorf("%s is not terminal", state)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	stamp := formatTime(now)
	if terminalStates[from] {
		if kind != EffectWorkspaceClose {
			return false, fmt.Errorf("run %s is terminal in %s", id, from)
		}
		if state != from {
			return false, fmt.Errorf("run %s terminal state %s cannot change to %s during workspace close", id, from, state)
		}
		current, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, id))
		if err != nil {
			return false, err
		}
		res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result=?,task_verdict=?,error_code=?,error_detail=?,provisioning_owner='',provisioning_lease_until=0,effect_owner='',effect_claim='',effect_kind='',effect_lease_until=0,effect_receipt='',acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND effect_owner=? AND effect_claim=? AND effect_kind=? AND effect_lease_until>?`, current.State, current.InfrastructureResult, current.AgentResult, current.TaskVerdict, current.ErrorCode, current.ErrorDetail, current.AcceptanceLane, current.AcceptanceReason, boolInt(current.Unread), stamp, id, from, owner, claim, kind, now.UnixNano())
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return false, nil
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, from, state, stamp, code, detail); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, state, verdict)
	if err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result=?,task_verdict=?,error_code=?,error_detail=?,provisioning_owner='',provisioning_lease_until=0,effect_owner='',effect_claim='',effect_kind='',effect_lease_until=0,effect_receipt='',acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND effect_owner=? AND effect_claim=? AND effect_kind=? AND effect_lease_until>?`, state, infra, agent, verdict, code, detail, lane, reason, unread, stamp, id, from, owner, claim, kind, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, from, state, stamp, code, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// FinishExpiredVerifier recovers only an expired verifier invocation. The
// caller may report a proven durable result or fail closed as interrupted.
func (s *Store) FinishExpiredVerifier(ctx context.Context, id, state, infra, agent, verdict, code, detail string, now time.Time) (bool, error) {
	if !terminalStates[state] {
		return false, fmt.Errorf("%s is not terminal", state)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	var effectKind string
	if err := tx.QueryRowContext(ctx, `SELECT effect_kind FROM runs WHERE id=?`, id).Scan(&effectKind); err != nil {
		return false, err
	}
	lane, reason, unread := config.AcceptanceMandatory, "legacy_verifier", 1
	if effectKind == EffectVerifier {
		lane, reason, unread, err = terminalAcceptanceTx(ctx, tx, id, state, verdict)
		if err != nil {
			return false, err
		}
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result=?,task_verdict=?,error_code=?,error_detail=?,effect_owner='',effect_claim='',effect_kind='',effect_lease_until=0,effect_receipt='',acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND ((effect_kind=? AND effect_lease_until<=?) OR (effect_owner='' AND effect_kind=''))`, state, infra, agent, verdict, code, detail, lane, reason, unread, stamp, id, StateVerifying, EffectVerifier, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, StateVerifying, state, stamp, code, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// InterruptExpiredVerifier fails closed when no durable completion marker
// exists for the prior external verifier invocation.
func (s *Store) InterruptExpiredVerifier(ctx context.Context, id, detail string, now time.Time) (bool, error) {
	return s.FinishExpiredVerifier(ctx, id, StateInterrupted, "completed", "completed", "unverified", "restart_during_verifier", detail, now)
}

func (s *Store) ReleaseEffect(ctx context.Context, id, from, owner, claim, kind string, now time.Time) (bool, error) {
	if claim == "" {
		return false, errors.New("effect claim is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET effect_owner='',effect_claim='',effect_kind='',effect_lease_until=0,effect_receipt='',updated_at=? WHERE id=? AND state=? AND effect_owner=? AND effect_claim=? AND effect_kind=?`, formatTime(now), id, from, owner, claim, kind)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func validateEffectLease(kind, owner string, now, leaseUntil time.Time) error {
	if owner == "" {
		return errors.New("effect owner is required")
	}
	if kind != EffectVerifier && kind != EffectWorkspaceClose {
		return fmt.Errorf("unsupported run effect %q", kind)
	}
	if !leaseUntil.After(now) {
		return errors.New("effect lease must expire after its claim time")
	}
	if leaseUntil.Sub(now) > MaxEffectLease {
		return fmt.Errorf("effect lease exceeds maximum %s", MaxEffectLease)
	}
	return nil
}

func (s *Store) SetReceipt(ctx context.Context, id, workspaceID, paneID, branch, path, mode, marker string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET workspace_id=?,pane_id=?,branch=?,worktree_path=?,execution_mode=?,completion_marker=?,updated_at=? WHERE id=?`, workspaceID, paneID, branch, path, mode, marker, formatTime(time.Now()), id)
	return err
}

func (s *Store) SetSourceContext(ctx context.Context, id, baseRevision, revision, inputContext string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET source_base_revision=?,source_revision=?,input_context=?,updated_at=? WHERE id=? AND state IN ('accepted','provisioning')`, baseRevision, revision, inputContext, formatTime(time.Now()), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("run %s could not persist source context before start", id)
	}
	return nil
}

// SetModelAttestation binds one run to the model receipt observed for its
// agent invocation. The read and the compare-and-set share one transaction so
// a second writer can never replace a receipt that is already durable. Before
// the run settles, a repeated identical receipt is an idempotent success; any
// other stored receipt or a missing run conflicts and changes nothing. Once a
// run is terminal its attestation is closed, so every attempt conflicts,
// including one repeating the receipt already stored.
func (s *Store) SetModelAttestation(ctx context.Context, id, receipt string) error {
	if receipt == "" {
		return errors.New("model attestation receipt is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, existing string
	err = tx.QueryRowContext(ctx, `SELECT state,model_attestation FROM runs WHERE id=?`, id).Scan(&state, &existing)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: run %s has no persisted row to attest", ErrStateConflict, id)
	}
	if err != nil {
		return err
	}
	// A terminal run is checked before the idempotent repeat so that no
	// attestation, not even one identical to the stored receipt, can be
	// reported as accepted after the run has settled.
	if terminalStates[state] {
		return fmt.Errorf("%w: run %s is terminal in %s and cannot be attested", ErrStateConflict, id, state)
	}
	if existing == receipt {
		return nil
	}
	if existing != "" {
		return fmt.Errorf("%w: run %s already attested a different model receipt", ErrStateConflict, id)
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET model_attestation=?,updated_at=? WHERE id=? AND state=? AND model_attestation=''`, receipt, formatTime(time.Now()), id, state)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: run %s did not accept a model attestation from %s", ErrStateConflict, id, state)
	}
	return tx.Commit()
}

// runReceiptKind selects one immutable bounded-review receipt column. It is a
// closed internal set: the column name is derived here and never taken from a
// caller-supplied string, so no receipt value can reach the statement text.
type runReceiptKind int

const (
	runReceiptInput runReceiptKind = iota
	runReceiptChange
)

// SetInputReceipt binds one run to the receipt bounding the inputs its review
// was allowed to read.
func (s *Store) SetInputReceipt(ctx context.Context, id, receipt string) error {
	return s.setImmutableRunReceipt(ctx, runReceiptInput, id, receipt)
}

// SetChangeReceipt binds one run to the receipt bounding the writes its review
// was allowed to make.
func (s *Store) SetChangeReceipt(ctx context.Context, id, receipt string) error {
	return s.setImmutableRunReceipt(ctx, runReceiptChange, id, receipt)
}

// setImmutableRunReceipt writes one bounded-review receipt once. The read and
// the compare-and-set share one transaction so a second writer can never
// replace a receipt that is already durable. Before the run settles, a repeated
// identical receipt is an idempotent success; any other stored receipt or a
// missing run conflicts and changes nothing. Once a run is terminal its
// receipts are closed, so every attempt conflicts, including one repeating the
// receipt already stored.
func (s *Store) setImmutableRunReceipt(ctx context.Context, kind runReceiptKind, id, receipt string) error {
	var column, noun string
	switch kind {
	case runReceiptInput:
		column, noun = "input_receipt", "bounded review input receipt"
	case runReceiptChange:
		column, noun = "change_receipt", "bounded review change receipt"
	default:
		return fmt.Errorf("unsupported run receipt kind %d", int(kind))
	}
	if receipt == "" {
		return fmt.Errorf("%s is required", noun)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, existing string
	err = tx.QueryRowContext(ctx, `SELECT state,`+column+` FROM runs WHERE id=?`, id).Scan(&state, &existing)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: run %s has no persisted row to record a %s", ErrStateConflict, id, noun)
	}
	if err != nil {
		return err
	}
	// A terminal run is checked before the idempotent repeat so that no receipt,
	// not even one identical to the stored value, can be reported as accepted
	// after the run has settled.
	if terminalStates[state] {
		return fmt.Errorf("%w: run %s is terminal in %s and cannot record a %s", ErrStateConflict, id, state, noun)
	}
	if existing == receipt {
		return nil
	}
	if existing != "" {
		return fmt.Errorf("%w: run %s already recorded a different %s", ErrStateConflict, id, noun)
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET `+column+`=?,updated_at=? WHERE id=? AND state=? AND `+column+`=''`, receipt, formatTime(time.Now()), id, state)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: run %s did not accept a %s from %s", ErrStateConflict, id, noun, state)
	}
	return tx.Commit()
}

func (s *Store) LastSuccessfulSource(ctx context.Context, jobID string) (SuccessfulSource, error) {
	var source SuccessfulSource
	err := s.db.QueryRowContext(ctx, `SELECT job_revision,source_revision FROM runs WHERE job_id=? AND state='succeeded' AND source_revision<>'' ORDER BY accepted_unix_nano DESC,id DESC LIMIT 1`, jobID).Scan(&source.JobRevision, &source.SourceRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return SuccessfulSource{}, nil
	}
	return source, err
}

func (s *Store) SaveProvisioningPlan(ctx context.Context, id, owner, branch string, now time.Time) (bool, error) {
	if owner == "" || branch == "" {
		return false, errors.New("provisioning owner and planned branch are required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET branch=?,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>?`, branch, formatTime(now), id, StateProvisioning, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) RecoverProvisioningWorkspace(ctx context.Context, id, branch, workspaceID, path string, now time.Time) (bool, error) {
	if branch == "" || workspaceID == "" {
		return false, errors.New("planned branch and recovered workspace identity are required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET workspace_id=?,worktree_path=?,updated_at=? WHERE id=? AND state=? AND branch=? AND (provisioning_owner='' OR provisioning_lease_until<=?) AND effect_owner=''`, workspaceID, path, formatTime(now), id, StateProvisioning, branch, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) SavePartialProvisioningReceipt(ctx context.Context, id, owner, workspaceID, paneID, branch string, now time.Time) (bool, error) {
	if owner == "" || workspaceID == "" || branch == "" {
		return false, errors.New("owner, workspace, and branch are required for a partial provisioning receipt")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET workspace_id=?,pane_id=?,branch=?,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>?`, workspaceID, paneID, branch, formatTime(now), id, StateProvisioning, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// SaveProvisioningReceipt persists a just-created workspace only while the
// caller still owns a live provisioning claim. Receipt persistence and the
// transition to starting are one transaction, and retain the same live owner
// and lease until start confirmation is committed.
func (s *Store) SaveProvisioningReceipt(ctx context.Context, id, owner, workspaceID, paneID, branch, path, mode, marker string, now time.Time) (bool, error) {
	if owner == "" {
		return false, errors.New("provisioning owner is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,workspace_id=?,pane_id=?,branch=?,worktree_path=?,execution_mode=?,completion_marker=?,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>?`, StateStarting, workspaceID, paneID, branch, path, mode, marker, stamp, id, StateProvisioning, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateProvisioning, StateStarting, stamp, "workspace provisioned and receipt persisted"); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, id)
	return scanRun(row)
}

func (s *Store) ListRuns(ctx context.Context, jobID string, limit int) ([]Run, error) {
	query := `SELECT ` + runColumns + ` FROM runs`
	args := []any{}
	if jobID != "" {
		query += ` WHERE job_id=?`
		args = append(args, jobID)
	}
	query += ` ORDER BY accepted_unix_nano DESC,id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) ListRunsGroupedByAcceptance(ctx context.Context, jobID string, limit int) ([]Run, error) {
	terminalsQuery := `SELECT ` + runColumns + ` FROM runs WHERE state NOT IN ('accepted','provisioning','starting','running','settled','verifying') AND effect_kind<>'workspace_close'`
	activeQuery := `SELECT ` + runColumns + ` FROM runs WHERE (state IN ('accepted','provisioning','starting','running','settled','verifying') OR effect_kind='workspace_close')`
	args := []any{}
	if jobID != "" {
		terminalsQuery += ` AND job_id=?`
		activeQuery += ` AND job_id=?`
		args = append(args, jobID)
	}
	terminalsQuery += ` ORDER BY CASE
		WHEN acceptance_lane='sample' THEN 1
		WHEN acceptance_lane='auto' THEN 2
		ELSE 0
	END, accepted_unix_nano DESC,id DESC`
	if limit > 0 {
		terminalsQuery += ` LIMIT ?`
	}
	terminalArgs := append([]any{}, args...)
	if limit > 0 {
		terminalArgs = append(terminalArgs, limit)
	}
	activeQuery += ` ORDER BY accepted_unix_nano DESC,id DESC`

	var out []Run
	if limit != 0 {
		rows, err := s.db.QueryContext(ctx, terminalsQuery, terminalArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			run, err := scanRun(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, run)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	activeRows, err := s.db.QueryContext(ctx, activeQuery, args...)
	if err != nil {
		return nil, err
	}
	defer activeRows.Close()
	for activeRows.Next() {
		run, err := scanRun(activeRows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, activeRows.Err()
}

func (s *Store) NonTerminalRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM runs WHERE state IN ('accepted','provisioning','starting','running','settled','verifying') OR effect_kind='workspace_close' ORDER BY accepted_unix_nano,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) LastJobResult(ctx context.Context, jobID string) (string, error) {
	var result string
	err := s.db.QueryRowContext(ctx, `SELECT CASE WHEN o.outcome='accepted' THEN COALESCE(r.state,o.outcome) ELSE o.outcome END
		FROM occurrences o LEFT JOIN runs r ON r.id=o.run_id
		WHERE o.job_id=? ORDER BY o.created_unix_nano DESC,o.occurrence_key DESC LIMIT 1`, jobID).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return result, err
}

// DecideAdmission serializes the capacity decision across every process using
// this database. The callback runs while a SQLite write lock is held and must
// stay pure over the active rows: it must never probe filesystems, because the
// caller has already probed the candidate and supplies its device id and disk
// reserve here. The candidate is then atomically held, failed, or claimed as
// provisioning with a bounded owner lease, and a successful claim persists the
// probed device and reserve in the same transaction so later decisions can
// account per-filesystem reserves from durable state alone.
func (s *Store) DecideAdmission(ctx context.Context, id, owner, diskDevice string, diskReserveGiB float64, now, leaseUntil time.Time, check AdmissionCheck) (bool, error) {
	if err := validateProvisioningLease(owner, now, leaseUntil); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// The no-op UPDATE takes the database write lock, serializing the decision
	// across independent handles. A missing lock row means the invariant the
	// serialization depends on is broken, so admission fails closed.
	lock, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='admission_lock'`)
	if err != nil {
		return false, err
	}
	if n, _ := lock.RowsAffected(); n != 1 {
		return false, fmt.Errorf("admission lock row is missing; run %s admission fails closed", id)
	}

	var trigger, jobID, definition, candidateState, globalPaused string
	var enabled, paused int
	err = tx.QueryRowContext(ctx, `SELECT r.trigger,r.job_id,r.definition,r.state,j.enabled,j.paused,
		COALESCE((SELECT value FROM metadata WHERE key='global_paused'),'0')
		FROM runs r JOIN jobs j ON j.id=r.job_id WHERE r.id=?`, id).
		Scan(&trigger, &jobID, &definition, &candidateState, &enabled, &paused, &globalPaused)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if candidateState != StateAccepted {
		return false, nil
	}
	if enabled == 0 {
		if trigger == "canary" {
			return false, finishAdmissionTx(ctx, tx, id, StateBlocked, "not_started", "job_disabled", "current job authority is disabled", now)
		}
		return false, nil
	}
	if paused != 0 {
		if trigger == "canary" {
			return false, finishAdmissionTx(ctx, tx, id, StateBlocked, "not_started", "job_paused", "current job authority is paused", now)
		}
		return false, nil
	}
	if globalPaused == "1" {
		if trigger == "canary" {
			return false, finishAdmissionTx(ctx, tx, id, StateBlocked, "not_started", "global_paused", "automation execution is globally paused", now)
		}
		return false, nil
	}
	if trigger == "canary" {
		var snapshot struct {
			Overlap string `json:"overlap"`
		}
		if err := json.Unmarshal([]byte(definition), &snapshot); err != nil {
			return false, finishAdmissionTx(ctx, tx, id, StateFailed, "failed", "snapshot_invalid", err.Error(), now)
		}
		if snapshot.Overlap == "" {
			snapshot.Overlap = "forbid"
		}
		if snapshot.Overlap != "allow" {
			var conflicting int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE job_id=? AND id<>? AND state IN ('accepted','provisioning','starting','running','settled','verifying')`, jobID, id).Scan(&conflicting); err != nil {
				return false, err
			}
			if conflicting > 0 {
				return false, finishAdmissionTx(ctx, tx, id, StateBlocked, "not_started", "overlap_hold", "another run for this job is nonterminal", now)
			}
		}
	}

	rows, err := tx.QueryContext(ctx, `SELECT `+runColumns+` FROM runs WHERE state IN ('provisioning','starting','running','settled','verifying') ORDER BY accepted_unix_nano,id`)
	if err != nil {
		return false, err
	}
	var active []Run
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			rows.Close()
			return false, scanErr
		}
		active = append(active, run)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	decision, err := check(active)
	if err != nil {
		return false, err
	}
	if decision.Admit && (decision.FailureCode != "" || decision.HoldCode != "") {
		return false, errors.New("admission decision cannot both admit and refuse")
	}
	if decision.FailureCode != "" && decision.HoldCode != "" {
		return false, errors.New("admission decision cannot both fail and hold")
	}
	if decision.FailureCode != "" {
		return false, finishAdmissionTx(ctx, tx, id, StateFailed, "failed", decision.FailureCode, decision.FailureDetail, now)
	}
	if !decision.Admit {
		if trigger == "canary" {
			code, detail := decision.HoldCode, decision.HoldDetail
			if code == "" {
				code, detail = "admission_hold", "attended canary could not be claimed during this call"
			}
			return false, finishAdmissionTx(ctx, tx, id, StateBlocked, "not_started", code, detail, now)
		}
		return false, nil
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,provisioning_owner=?,provisioning_lease_until=?,disk_device=?,disk_reserve_gib=?,updated_at=? WHERE id=? AND state=?`, StateProvisioning, owner, leaseUntil.UnixNano(), diskDevice, diskReserveGiB, stamp, id, StateAccepted)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateAccepted, StateProvisioning, stamp, "capacity admitted; route validation pending"); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func finishAdmissionTx(ctx context.Context, tx *sql.Tx, id, state, infrastructure, code, detail string, now time.Time) error {
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, state, "unverified")
	if err != nil {
		return err
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result='not_started',task_verdict='unverified',error_code=?,error_detail=?,acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=?`, state, infrastructure, code, detail, lane, reason, unread, stamp, id, StateAccepted)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, StateAccepted, state, stamp, code, detail); err != nil {
		return err
	}
	return tx.Commit()
}

// RenewProvisioningClaim extends only the same live owner claim while it is
// provisioning or starting; an expired claim cannot be resurrected.
func (s *Store) RenewProvisioningClaim(ctx context.Context, id, owner string, now, leaseUntil time.Time) (bool, error) {
	if err := validateProvisioningLease(owner, now, leaseUntil); err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET provisioning_lease_until=?,updated_at=? WHERE id=? AND state IN (?,?) AND provisioning_owner=? AND provisioning_lease_until>?`, leaseUntil.UnixNano(), formatTime(now), id, StateProvisioning, StateStarting, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// ConfirmStartingClaim commits durable start confirmation and releases
// ownership only for the same still-live owner.
func (s *Store) ConfirmStartingClaim(ctx context.Context, id, owner, detail string, now time.Time) (bool, error) {
	if owner == "" {
		return false, errors.New("provisioning owner is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,provisioning_owner='',provisioning_lease_until=0,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>? AND effect_owner=''`, StateRunning, stamp, id, StateStarting, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateStarting, StateRunning, stamp, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// FinishStartingClaim terminalizes a start failure only for the same live
// owner, preventing a stale executor from changing a run after claim loss.
func (s *Store) FinishStartingClaim(ctx context.Context, id, owner, state, infra, agent, verdict, code, detail string, now time.Time) (bool, error) {
	if owner == "" {
		return false, errors.New("provisioning owner is required")
	}
	if !terminalStates[state] {
		return false, fmt.Errorf("%s is not terminal", state)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, state, verdict)
	if err != nil {
		return false, err
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result=?,task_verdict=?,error_code=?,error_detail=?,provisioning_owner='',provisioning_lease_until=0,acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>?`, state, infra, agent, verdict, code, detail, lane, reason, unread, stamp, id, StateStarting, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, StateStarting, state, stamp, code, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// FinishProvisioningClaim terminalizes only the caller's still-live claim.
func (s *Store) FinishProvisioningClaim(ctx context.Context, id, owner, state, infra, agent, verdict, code, detail string, now time.Time) (bool, error) {
	if owner == "" {
		return false, errors.New("provisioning owner is required")
	}
	if !terminalStates[state] {
		return false, fmt.Errorf("%s is not terminal", state)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, state, verdict)
	if err != nil {
		return false, err
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result=?,agent_result=?,task_verdict=?,error_code=?,error_detail=?,provisioning_owner='',provisioning_lease_until=0,acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND provisioning_owner=? AND provisioning_lease_until>?`, state, infra, agent, verdict, code, detail, lane, reason, unread, stamp, id, StateProvisioning, owner, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateProvisioning, state, stamp, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// InterruptExpiredProvisioning marks only an expired or migrated unowned
// provisioning claim. A concurrent owner renewal and this update are mutually
// exclusive at the row compare-and-set.
func (s *Store) InterruptExpiredProvisioning(ctx context.Context, id string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, StateInterrupted, "unverified")
	if err != nil {
		return false, err
	}
	stamp := formatTime(now)
	detail := "service restarted after the provisioning claim expired before a durable Herdr receipt"
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result='uncertain',agent_result='not_started',task_verdict='unverified',error_code='restart_during_provisioning',error_detail=?,provisioning_owner='',provisioning_lease_until=0,acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND workspace_id='' AND (provisioning_owner='' OR provisioning_lease_until<=?) AND effect_owner=''`, StateInterrupted, detail, lane, reason, unread, stamp, id, StateProvisioning, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateProvisioning, StateInterrupted, stamp, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// InterruptExpiredStarting terminalizes an unowned or expired start claim.
// This CAS is used before conservative workspace interruption so a live owner
// can never be raced by reconciliation.
func (s *Store) InterruptExpiredStarting(ctx context.Context, id, code, detail string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return false, err
	}
	lane, reason, unread, err := terminalAcceptanceTx(ctx, tx, id, StateInterrupted, "unverified")
	if err != nil {
		return false, err
	}
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,infrastructure_result='uncertain',agent_result='not_started',task_verdict='unverified',error_code=?,error_detail=?,provisioning_owner='',provisioning_lease_until=0,acceptance_lane=?,acceptance_reason=?,unread=?,updated_at=? WHERE id=? AND state=? AND (provisioning_owner='' OR provisioning_lease_until<=?) AND effect_owner=''`, StateInterrupted, code, detail, lane, reason, unread, stamp, id, StateStarting, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, StateStarting, StateInterrupted, stamp, code, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ConfirmExpiredCommandStart recovers only an expired starting command after
// its unique completion marker durably proves that the command ran.
func (s *Store) ConfirmExpiredCommandStart(ctx context.Context, id, detail string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stamp := formatTime(now)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET state=?,provisioning_owner='',provisioning_lease_until=0,updated_at=? WHERE id=? AND state=? AND (provisioning_owner='' OR provisioning_lease_until<=?) AND effect_owner=''`, StateRunning, stamp, id, StateStarting, now.UnixNano())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,detail) VALUES(?,?,?,?,?)`, id, StateStarting, StateRunning, stamp, detail); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func validateProvisioningLease(owner string, now, leaseUntil time.Time) error {
	if owner == "" {
		return errors.New("provisioning owner is required")
	}
	if !leaseUntil.After(now) {
		return errors.New("provisioning lease must expire after its claim time")
	}
	if leaseUntil.Sub(now) > MaxProvisioningLease {
		return fmt.Errorf("provisioning lease exceeds maximum %s", MaxProvisioningLease)
	}
	return nil
}

// RecordRunEvent persists typed operational evidence and atomically reopens
// the unread inbox, including for evidence attached to a terminal run.
func (s *Store) RecordRunEvent(ctx context.Context, id, code, detail string, now time.Time) error {
	if code == "" {
		return errors.New("event code is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockOccurrenceWrites(ctx, tx); err != nil {
		return err
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM runs WHERE id=?`, id).Scan(&state); err != nil {
		return err
	}
	var previousCode, previousDetail string
	previousErr := tx.QueryRowContext(ctx, `SELECT code,detail FROM events WHERE run_id=? ORDER BY id DESC LIMIT 1`, id).Scan(&previousCode, &previousDetail)
	if previousErr == nil && previousCode == code && previousDetail == detail {
		return nil
	}
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		return previousErr
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,from_state,to_state,at,code,detail) VALUES(?,?,?,?,?,?)`, id, state, state, formatTime(now), code, detail); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET unread=1,updated_at=? WHERE id=?`, formatTime(now), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return sql.ErrNoRows
	}
	return tx.Commit()
}

func (s *Store) Events(ctx context.Context, id string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,from_state,to_state,at,code,detail FROM events WHERE run_id=? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var event Event
		var at string
		if err := rows.Scan(&event.ID, &event.RunID, &event.FromState, &event.ToState, &at, &event.Code, &event.Detail); err != nil {
			return nil, err
		}
		event.At, err = parseTime(at)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) MarkRead(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET unread=0 WHERE id=?`, id)
	return err
}

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (Run, error) {
	var out Run
	var scheduled sql.NullString
	var accepted, updated string
	var provisioningLeaseUntil, effectLeaseUntil int64
	var unread int
	err := row.Scan(&out.ID, &out.JobID, &out.JobRevision, &out.Definition, &out.Trigger, &scheduled, &out.State, &out.InfrastructureResult, &out.AgentResult, &out.TaskVerdict, &accepted, &out.AcceptedUnixNano, &updated, &out.WorkspaceID, &out.PaneID, &out.Branch, &out.WorktreePath, &out.ExecutionMode, &out.CompletionMarker, &out.SourceBaseRevision, &out.SourceRevision, &out.InputContext, &out.ErrorCode, &out.ErrorDetail, &out.ProvisioningOwner, &provisioningLeaseUntil, &out.EffectOwner, &out.EffectClaim, &out.EffectKind, &effectLeaseUntil, &out.EffectReceipt, &out.DiskDevice, &out.DiskReserveGiB, &out.AcceptanceLane, &out.AcceptanceReason, &unread, &out.ModelAttestation, &out.InputReceipt, &out.ChangeReceipt)
	if err != nil {
		return out, err
	}
	out.AcceptedAt, err = parseTime(accepted)
	if err != nil {
		return out, err
	}
	out.UpdatedAt, err = parseTime(updated)
	if err != nil {
		return out, err
	}
	if scheduled.Valid {
		t, err := parseTime(scheduled.String)
		if err != nil {
			return out, err
		}
		out.ScheduledFor = &t
	}
	if provisioningLeaseUntil != 0 {
		out.ProvisioningLeaseUntil = time.Unix(0, provisioningLeaseUntil).UTC()
	}
	if effectLeaseUntil != 0 {
		out.EffectLeaseUntil = time.Unix(0, effectLeaseUntil).UTC()
	}
	out.Unread = unread != 0
	return out, nil
}

func newRunID(jobID string, now time.Time) (string, error) {
	var suffix [5]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-%s", jobID, now.UTC().Format("20060102T150405Z"), hex.EncodeToString(suffix[:])), nil
}

func newEffectClaim() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func validEffectClaim(claim string) bool {
	if len(claim) != 32 {
		return false
	}
	for _, char := range claim {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
func formatTime(t time.Time) string         { return t.UTC().Format(time.RFC3339Nano) }
func parseTime(v string) (time.Time, error) { return time.Parse(time.RFC3339Nano, v) }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}
