# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
versions with a `vMAJOR.MINOR.PATCH` prefix.

## [0.3.0] - 2026-08-25

macOS prerelease (preview): adds opt-in model attestation, bounded review
inputs and observed write scope, and explicit acceptance lanes for terminal
review. Existing v0.1.x and v0.2.0 configs remain valid, the new blocks are
all opt-in, and the database migration is additive.

### Added

- Explicit acceptance lanes for terminal review: `acceptance.mode` accepts
  `mandatory`, `auto`, or `sample`, where `auto` and `sample` require a
  verifier and `sample_percent` (1-100) is valid only for `sample`. Sampling is
  deterministic from a SHA-256 bucket of the immutable run ID, so concurrent
  processes classify the same run identically. Each terminal run records a
  durable lane and machine reason alongside the verifier verdict; auto-reviewed
  runs start read while sampled and mandatory runs start unread. Classification
  is decided inside the serialized terminal transition, so it is written once
  and survives restart and recovery without reclassifying an already-settled
  run, and grants no further authority. Omitting the `acceptance` block keeps
  snapshots byte-stable and defaults terminal review to `mandatory` without
  changing the saved revision.
- Opt-in `execution.require_model_attestation` for `claude-code` jobs. The
  attested launch asks the harness for a machine-readable result and, after the
  command completes and before the verifier runs, binds that result to the
  configured model: a first-party provider, a canonical model equal to the
  configured full model name, and nonzero token usage. Anything absent,
  malformed, or mismatched fails the run as `model_attestation_failed` rather
  than downgrading to a weaker verdict. The contract requires a full
  `claude-`-prefixed model name, rejecting aliases and `harness-default`, and is
  invalid on Pi routes. Omitting the field leaves existing job snapshots and
  launches byte-for-byte unchanged.
- Opt-in bounded inputs and observed write scope. `execution.inputs` stages up
  to 32 declared snapshots (16 MiB per file, 64 MiB per run) into the reserved
  `.herdr-bots/inputs/` directory before the run, with `{date}`, `{year}`, and
  `{month}` sources resolved in the job's timezone against the scheduled
  occurrence; destinations are never overwritten and no write scope may name
  that directory. `execution.allowed_write_paths` (requires
  `repo-write-no-network`) declares exact paths and directory prefixes, and
  never `.git`. Both produce durable deterministic receipts: what was staged,
  with sizes and sha256 digests, and what the worktree changed, including
  ignored files and a content fingerprint per changed path, under an
  `observed_within_scope` verdict. Staged inputs are reproved before the change
  set is read, the boundary is observed after the agent settles and rechecked
  after the verifier runs in the same worktree, and an out-of-scope change fails
  the run as `bounded_review_failed`. The verdict is post-run evidence under
  shell-free permission profiles, not OS containment. Snapshots are retained in
  the evidence worktree; the scheduler never deletes them.

### Fixed

- Verifier process-group quiescence: the supervisor now proves the verifier's
  process group has actually gone before the run settles, instead of assuming
  termination once the command returns. Before signaling, the supervisor
  observes the group's members and their ownership: a group with zero observed
  members is skipped and treated as the success it is, and a signal is sent at
  most once, only when same-user members are observed. `EPERM` and `ESRCH` are
  resolved by re-observing membership rather than assumed, and the run fails
  closed if members it owns — or any same-user member — remain. Absence is
  proved before the run settles.

## [0.2.0] - 2026-08-25

macOS prerelease (preview): adds an opt-in unread-terminal-run attention guard
for jobs. Existing v0.1.x configs remain valid, and the database migration
is additive.

### Added

- Optional per-job `attention.max_unread_terminal_runs` guard (values 1-1000;
  omitting the block preserves v0.1.x behavior exactly). When a job's count of
  unread runs in terminal states reaches the limit, the scheduler pauses the job
  before admitting another run — atomically, inside the same authority-fenced
  admission transaction used by scheduled, event, and manual admission. A tripped
  guard creates no run, occurrence, workspace, or agent.
- Durable pause reason `unread_terminal_runs` (distinct from a manual pause's
  `manual` reason) with a persisted pause timestamp; nonterminal runs never count,
  and marking runs read never auto-resumes the job — only an explicit
  `herdr-bots resume JOB` clears the pause. `herdr-bots list` shows the durable
  pause reason without marking any run read.

## [0.1.1] - 2026-08-24

### Fixed

- Real Herdr emits structured error envelopes on stderr; parse them as well as
  stdout so transient agent-wait timeouts are recognized and retried.

## [0.1.0] - 2026-08-24

First public preview of Herdr Bots: a durable, local, macOS scheduler for
coding-agent jobs with pinned native-harness routes, isolated worktree runs,
and an evidence-backed read inbox.

### Added

- Durable SQLite control plane: occurrences and immutable job snapshots are
  recorded before any external effect; `(job_id, occurrence_key)` uniqueness
  prevents duplicate runs across ticks, restarts, and processes.
- Clock schedules (`cron`, `once`) and typed local event intake
  (`enqueue JOB --event-id ID`) with idempotent event identity and no event
  payloads.
- Native-harness routing with pinned provider/model: Pi as an interactive
  Herdr agent, Claude Code as `claude -p --safe-mode` in a Herdr pane. Route
  probes verify readiness and the exact observed model before workspace
  creation. No fallback at any level.
- Mandatory fresh-worktree execution with per-run pinned source revisions,
  `run_if_changed` gating, and untrusted commit context supplied to agents.
- Serialized capacity admission: concurrency caps and per-device disk
  reserves persisted with durable provisioning leases; conservative
  fail-closed handling of legacy claims and failed probes.
- Restart reconciliation that preserves live owned claims, interrupts
  expired ones, never retries agents automatically, and never treats an idle
  pane as completion.
- Deterministic verifiers with size limits, hard timeouts, and process-group
  reaping, recorded separately from infrastructure and agent results.
- Read-oriented Herdr pane inbox and CLI (`list`, `runs`, `show`, `run
  --canary`, `cancel`, `pause`, `resume`, `doctor`, `service render`).
- Owner-private source launcher (`./herdr-bots`) that compiles the CLI into
  a revision-keyed cache for plugin-managed checkouts.
- Config authority admission: only regular, non-symlink, mode-`0600` files
  whose opened inode matches the path are accepted.
- Public release artefacts: README, SECURITY.md, CONTRIBUTING.md, NOTICE,
  release gate (`assays/release-gate.sh`), and read-only macOS CI running
  `make check`. Linux is not claimed as a build or test target because the
  engine depends on macOS-only atomic filesystem primitives.

### Attribution

Derived implementation patterns from the MIT-licensed
`DnzzL/herdr-automations` project at commit `08640f3` by Thomas Legrand;
this implementation is separately maintained by Terry Li. See `NOTICE`.

[0.3.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.3.0
[0.2.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.2.0
[0.1.1]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.1
[0.1.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.0
