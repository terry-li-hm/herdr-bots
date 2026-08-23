# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
versions with a `vMAJOR.MINOR.PATCH` prefix.

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

[0.1.1]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.1
[0.1.0]: https://github.com/terry-li-hm/herdr-bots/releases/tag/v0.1.0
