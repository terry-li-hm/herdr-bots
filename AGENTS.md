# Herdr Bots repository conventions

Herdr Bots is a local, current-user control plane for scheduled coding-agent
work. Claude models route through Claude Code. Non-Claude routes are
configured and reported by Pi. This document records repository conventions
for coding agents working here.

## Architecture

```text
bots.yaml
      |
      v
config validation -> scheduler -> SQLite occurrence/run state
                           |              |
                           v              v
                     route probe     unread run inbox
                           |
                           v
                    Herdr worktree + native harness
                    Pi agent | Claude headless command
                           |
                           v
                    optional deterministic verifier

launchd supervises the scheduler process. Herdr does not.
```

## Commands

| Command | Purpose |
|---|---|
| `daemon` | Evaluate schedules, reconcile restarts, and dispatch due runs |
| `list` | Show configured jobs and their latest run |
| `runs [JOB]` | Show the unread run inbox |
| `show RUN` | Show one run and mark it read |
| `pane` | Open the read-oriented Herdr inbox |
| `run JOB --canary` | Execute one attended canary |
| `enqueue JOB --event-id ID` | Accept one typed local event occurrence |
| `cancel RUN` | Close a received Herdr workspace and mark the run cancelled |
| `pause` / `resume` | Change a job or global execution pause |
| `doctor` | Validate configuration, repositories, and exact harness routes |
| `service render` | Print the launchd plist without installing it |

## Conventions

1. Persist a run before creating any workspace or agent.
2. Never substitute a provider, model, harness, or permission profile.
3. Keep event payloads out of version one. The saved prompt is the authority.
4. Do not answer unattended approval prompts. Record the run as blocked.
5. Never retry an agent automatically.
6. Fresh worktrees are mandatory in version one. Root runs are rejected.
7. Change-gated jobs persist an exact source commit before provisioning, pin
   the worktree to that commit, and skip unchanged occurrences without
   creating a workspace.
8. Terminal states are immutable.
9. Installing launchd, linking the plugin, enabling a job, pushing,
   publishing, or creating a remote requires its own authority outside this
   repository.
10. A new bounded job retains its staged snapshots. Nothing in the scheduler
    deletes a staged input or the worktree holding it; the snapshots are the
    evidence a receipt is checked against, and reclaiming them belongs to
    whoever owns the worktree's lifecycle.
11. An attested Claude job names a canonical full model, never an alias such
    as `opus` and never `harness-default`. Attestation compares the harness's
    reported canonical model against that exact name.
12. A bounded boundary is re-observed after the verifier finishes, because the
    verifier runs unrestricted in the same worktree. The second observation
    must recompute the first receipt's bytes; any changed fingerprint fails
    the run.

## Extension pattern

When adding a lifecycle feature:

1. Add the immutable field to the configuration snapshot.
2. Add or migrate durable state before introducing an external effect.
3. Add a compare-and-set transition and a typed failure code.
4. Add a fake-client test proving duplicate effects cannot occur.
5. Update `bots.example.yaml` and `README.md`.

## Gotchas

- Herdr plugin startup hooks are one-shot and unsupervised. Never run the
  daemon from a `[[startup]]` hook; supervise it with launchd instead.
- `catch_up_grace_minutes: 0` still allows normal polling skew of 60 seconds.
- Pi is treated as a harness, not a generic model API. Provider and model are
  checked against Pi before workspace creation.
- Claude runs as `claude -p --safe-mode` in the Herdr pane. Interactive
  Claude stops at a trust prompt for every fresh worktree, while print mode
  keeps the native subscription route and `--tools` availability boundary.
- The no-network profiles enforce their boundary by omitting shell and web
  tools. Adding Bash or another shell invalidates that claim.
- `observed_within_scope` is a post-run observation of the worktree, not
  containment. It is worth recording only because the profiles launched here
  stay shell-free and web-free, so a run reaches the workspace through file
  edits git can be asked to enumerate. A run that could execute arbitrary code
  could write where no receipt would ever mention it. Never document or name
  the verdict as a sandbox, and never grant a bounded job a shell or web tool
  to make the observation more convenient.
- A run interrupted during provisioning or unconfirmed start is not replayed
  because its external effects may be uncertain. Reconciliation preserves a
  live owner lease through receipt-bearing `starting`, interrupts only
  expired/unowned claims, and never treats an idle Pi pane as task completion
  before prompt acceptance.
- Disk admission reserves each active run's full declared peak again even
  though some space may already be allocated. This deliberate conservatism
  prevents several builds from spending the same observed free-space
  headroom. Only the candidate repository is probed, before the serialized
  decision; active reserves are counted from the device id and reserve
  persisted with each durable claim. Reserves are charged per filesystem
  device; a legacy active row persisted before per-device claims is charged
  its snapshot/default reserve globally, and an unparseable legacy snapshot
  fails closed.

## Testing

```bash
make check
git diff --check
```

The engine tests use fake Herdr and harness clients. A live canary is a
separate attended action and must use a disposable repository.

## Commit style

Use conventional commit prefixes. Stage named files.
