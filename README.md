# Herdr Bots

Herdr Bots runs scheduled coding-agent jobs on your Mac. A bot is a durable,
versioned job charter; each of its runs is short-lived, executes in a fresh
Herdr worktree on a pinned route, and lands in a read-oriented evidence
inbox. Claude runs stay native to Claude Code; non-Claude routes configured and
reported by Pi stay native to Pi. launchd supervises the scheduler. Nothing
runs, installs, or schedules merely because this code exists on disk.

**Status: v0.1.1 preview.** The scheduler lifecycle and its safety contract
are complete and tested, but the project is young and the surface will
change. macOS is the only supported build, test, plugin, and managed-service
target. The engine uses macOS-only atomic filesystem primitives and does not
currently compile on Linux.

## Prerequisites

- macOS (the managed service renders a launchd plist; the verifier supervisor
  uses stock macOS `/usr/bin/perl`)
- [Herdr](https://github.com/herdrdev/herdr) 0.8.0 or newer, for workspaces,
  panes, and agent launching
- Go matching the version in [`go.mod`](go.mod) (first use compiles from
  source)
- Git (the launcher derives its build revision from the checkout, and runs
  pin source revisions)

## Install

### Herdr plugin (pane surface)

```bash
herdr plugin install terry-li-hm/herdr-bots --ref v0.1.1
```

The plugin install provides the pane surface: the read-oriented run inbox.
Its manifest pane command is `./herdr-bots pane`, backed by the root source
launcher. On first use the launcher compiles `cmd/herdr-bots` into an
owner-private cache — under `${HERDR_PLUGIN_STATE_DIR}/bin` when Herdr
provides that variable, otherwise under
`${XDG_CACHE_HOME:-$HOME/.cache}/herdr-bots/bin` — and then executes the
cached binary, so `go` and `git` must be available to the pane. The first
source build may fetch Go modules from the module proxy unless they are
already in the local module cache. The launcher never installs or links
anything, never downloads anything itself, and never deletes old cache
entries; cleaning the cache is yours to do.

Repeated use at the same clean source revision reuses the cached executable
without rebuilding. The revision comes from `git describe --tags --always`.
For a dirty checkout, the key includes the NUL-delimited porcelain status,
the tracked binary diff, and both exact names and content hashes of every
nonignored untracked file. Every Git operation is checked. The launcher
fingerprints before and after compilation and refuses a build if source moved.
Cache directories and entries must be non-symlinks owned by the effective user
with mode exactly `0700`; existing violations are refused, not repaired.
Publication cannot clobber an entry, so concurrent first builds validate and
reuse the one atomic winner.

The plugin deliberately registers no startup hook: Herdr startup hooks are
one-shot and cannot supervise a daemon. The scheduler runs under launchd (see
[Process supervision](#process-supervision)) or manually.

### Command-line CLI and service

The shell commands (`list`, `runs`, `doctor`, `service render`, and the
rest) and the launchd service are not provided by the plugin install;
install the CLI binary separately:

```bash
go install github.com/terry-li-hm/herdr-bots/cmd/herdr-bots@v0.1.1
```

Put the directory `go install` writes to on `PATH`. Find it with `go env
GOBIN`; when that is empty — the default — binaries go to
`$(go env GOPATH)/bin`, and `$(go env GOPATH)` itself defaults to `$HOME/go`:

```bash
go env GOBIN                       # empty means: binaries land in $(go env GOPATH)/bin
go env GOPATH                      # where `bin` lives when GOBIN is empty
bindir=$(go env GOBIN); [ -n "$bindir" ] || bindir="$(go env GOPATH)/bin"
echo "$bindir"                      # put this directory on PATH
```

`go install` may fetch Go modules on first use, just like the first source
build.

## Configure

The default config path is `~/.config/herdr-bots/bots.yaml`. The job
definition is executable authority — verifier commands are arbitrary saved
local commands — so the file must be a regular, non-symlink file owned by the
effective user with mode exactly `0600`. It is opened with no-follow and
close-on-exec controls, validated through the opened descriptor, then checked
against the current non-symlink path before parsing. **macOS ACLs are not inspected and must not grant others access.**

`bots.example.yaml` exists only in a source checkout of this repository. From
a checkout, copy it securely:

```bash
mkdir -p ~/.config/herdr-bots
(umask 077 && cp /path/to/checkout/bots.example.yaml ~/.config/herdr-bots/bots.yaml)
chmod 600 ~/.config/herdr-bots/bots.yaml
```

Without a checkout, create `~/.config/herdr-bots/bots.yaml` yourself with the
same mode-safe recipe and paste the example below as a starting point.

Every example job ships disabled. Review it, point `repository` at an
absolute path you control, and select provider/model values only from what
your native harness reports (`pi --list-models PROVIDER`, or your Claude Code
subscription's models). Then check everything before enabling:

```bash
herdr-bots doctor
```

```yaml
version: 1
capacity:
  max_concurrent_runs: 2
  min_free_disk_gib: 3
jobs:
  - id: docs-drift
    revision: 1
    enabled: false
    schedule:
      kind: cron
      expression: "0 9 * * 1"
      timezone: Asia/Hong_Kong
      catch_up_grace_minutes: 120
    execution:
      repository: /absolute/path/to/repository
      workspace: worktree
      base_ref: main
      harness: pi
      provider: openai-codex
      model: gpt-5.6-sol
      thinking: high
      permission_profile: read-only-no-network
    run_if_changed: true
    prompt: Report documentation drift. Do not edit files.
    timeout_minutes: 45
    overlap: forbid
    verifier:
      command: ["git", "diff", "--check"]
    limits:
      max_runs_per_day: 1
      disk_reserve_gib: 1.25
```

Unknown fields are rejected. `model` is required; use `harness-default`
explicitly rather than relying on an absent value. `revision` is the
monotonic saved-authority generation: increase it whenever any job field
changes. A lower revision is rejected, and reusing one revision for a
different snapshot hash fails closed rather than overwriting current
authority.

The default state database is `~/.local/state/herdr-bots/state.sqlite3`.

## Use

```text
herdr-bots list
herdr-bots runs [JOB]
herdr-bots show RUN
herdr-bots pane
herdr-bots run JOB --canary
herdr-bots enqueue JOB --event-id ID
herdr-bots pause JOB|--all
herdr-bots resume JOB|--all
herdr-bots cancel RUN
herdr-bots doctor
herdr-bots service render
```

`list` reports the latest run state or a schedule outcome such as
`skipped_unchanged`. `show` marks a run read; asterisks in `runs` and `pane`
mark unread results. The attended `run` command waits on durable state rather
than process-local executor presence. The Herdr pane is read-oriented: Enter
focuses the exact run workspace and marks it read; it does not mutate
schedules or clean worktrees.

Event jobs (`schedule.kind: event`) have no clock occurrence. Only the local
typed command accepts one:

```bash
herdr-bots enqueue JOB --event-id ID
```

The event ID is 1 to 128 lowercase ASCII letters or digits separated by
single hyphens. Repeating the same ID returns the original accepted run and
creates nothing else. No event payload exists: the command accepts no prompt
override, context file, JSON, shell command, or environment context. Only the
saved job prompt, route, repository, permission profile, limits, and verifier
authorize the later run. Manual `run` refuses an event job unless the
operator supplies `--canary`.

## Pause and revoke

- `herdr-bots pause JOB` holds one job's future occurrences; `pause --all`
  holds everything without losing accepted runs.
- `herdr-bots cancel RUN` closes a received Herdr workspace and marks the run
  cancelled.
- Unloading the launchd service (below) stops all scheduling; state is
  preserved.
- `herdr plugin uninstall terry.herdr-bots` removes the plugin pane surface
  only; it does not stop the launchd service or delete any local data (see
  [Uninstall caveats](#uninstall-caveats)).

## Process supervision

`service render` prints a launchd plist using the current binary, config,
state, environment, and log paths. It writes and loads nothing:

```bash
herdr-bots service render > /tmp/com.terry.herdr-bots.plist
plutil -lint /tmp/com.terry.herdr-bots.plist
```

Installing or loading the plist changes a persistent system service and is a
separate explicit action you own:

```bash
launchctl bootout gui/$(id -u)/com.terry.herdr-bots 2>/dev/null  # optional, if a previous version is loaded
cp /tmp/com.terry.herdr-bots.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.terry.herdr-bots.plist
```

To stop later: `launchctl bootout gui/$(id -u)/com.terry.herdr-bots`.

## Build and development

```bash
make check     # verify, format/vet/race/build, shell syntax, launcher assay, release gate
make build     # bin/herdr-bots with the current git revision embedded
make fmt       # rewrite formatting
./bin/herdr-bots --version
```

`make build` embeds the current Git description in `--version`; an
uncommitted tracked change adds the `-dirty` suffix. If linker metadata is
absent, `--version` falls back to Go build information: first the module
version, then the VCS revision (including its dirty flag). See [CONTRIBUTING.md](CONTRIBUTING.md)
for the extension pattern and [docs/release.md](docs/release.md) for the
release evidence matrix.

## Uninstall caveats

```bash
herdr plugin uninstall terry.herdr-bots
```

Plugin uninstall removes only the plugin pane surface. It does not stop or
unload the launchd service, and it does not delete the config file, the
SQLite state database, the launcher's build cache, or any Herdr panes,
transcripts, or worktrees created by past runs. To stop scheduling, run the
`launchctl bootout` from [Process supervision](#process-supervision) and
remove the rendered plist. To remove data, review and delete
`~/.config/herdr-bots/`, `~/.local/state/herdr-bots/`, the cache directory
above, and the worktrees listed in `herdr-bots runs`/`show` output yourself.

## Safety contract

- The state database records an occurrence and an immutable job snapshot
  before any external effect.
- `(job_id, occurrence_key)` is unique, so repeated ticks or event deliveries
  cannot duplicate a run.
- Route probes verify native-harness readiness and the exact observed
  provider/model before Herdr creates a workspace. No provider, model,
  harness, or permission fallback exists.
- Fresh worktrees are mandatory; root execution is rejected.
- No-task-network permission profiles omit task-controlled shell and web
  tools. A blocked approval is a terminal result, never a prompt the
  scheduler answers.
- Infrastructure result, agent result, and deterministic verifier verdict are
  stored separately. The saved verifier command executes as an
  unrestricted local process in the run workspace — outside the agent
  permission profile — and its output-size, hard-timeout, and process-group
  limits bound resource use, not authority: they are not a sandbox. A
  verifier command can use the shell or the network if the config grants it.
- Dispatch holds accepted runs when the concurrency cap or reserved disk
  headroom is exhausted. Per-run disk reserves are charged per filesystem
  device and persisted with each durable claim; active repositories are never
  probed from another run's admission.
- No automatic agent retry. No automatic worktree deletion. No push, merge,
  message, or publication authority anywhere in the scheduler.
- On restart: accepted runs dispatch once; live owned claims survive
  reconciliation; expired claims are interrupted, never blindly retried; an
  idle agent pane is never mistaken for completion; a settled run resumes
  only its verifier.

## Limitations

Clock schedules (cron, once) and typed local event intake only. No remote
APIs, webhooks, event payloads, persistent conversation memory, workflow
graphs, cloud execution, multi-repository jobs, systemd support, or model
fallback — all deliberate v0.1.x exclusions. `min_herdr_version` is asserted
in the manifest but not re-checked by the CLI at runtime. Report security
issues through [SECURITY.md](SECURITY.md).

## Attribution

Selected implementation patterns derive from the MIT-licensed
`DnzzL/herdr-automations` project at commit `08640f3` by Thomas Legrand. This
repository is a separately maintained implementation by Terry Li. See
[NOTICE](NOTICE) and [LICENSE](LICENSE).
