# Security policy

## Scope

Herdr Bots is a local, current-user scheduler for coding-agent jobs on macOS.
It runs on your machine, as your user, against repositories and harnesses you
configure. There is no server, no telemetry, and no network listener.

## Reporting a vulnerability

Report vulnerabilities privately to the repository owner through GitHub
security advisories (Report a vulnerability) for
`terry-li-hm/herdr-bots`. Do not open a public issue for a vulnerability.
Include reproduction steps, affected versions, and impact. You will receive an
acknowledgement; fixes ship through the normal release process described in
[`docs/release.md`](docs/release.md).

## Authority model

- **The config file is executable authority.** Job definitions name
  repositories, harnesses, models, permission profiles, and verifier
  commands; verifier commands are arbitrary saved local commands executed by
  the scheduler as your user in the run workspace. The verifier executes as
  an unrestricted local process outside the agent permission profile: its
  output-size, wall-clock, and process-group limits are resource bounds —
  not a sandbox — and a saved verifier command can use the shell or the
  network if the config grants it. The loader opens with no-follow and
  close-on-exec controls, then accepts only a regular, non-symlink file owned
  by the effective user with mode exactly `0600` whose opened inode still
  matches the current non-symlink path. It refuses group/world-readable files,
  symlinks, path replacement, and wrong ownership. **macOS ACLs are not inspected and must not grant others access.** Anyone who can write that file
  controls what runs.
- **Native harness credentials are delegated, never stored.** Herdr Bots
  never reads, stores, or transmits provider credentials or tokens. Route
  probes invoke the native harness CLIs (`pi`, `claude`), which use their own
  authenticated sessions. Provider API transport, when a run needs one,
  happens inside the native harness under its own credential handling.
- **No-task-network profiles omit task-controlled shell and web tools.** The
  `*-no-network` permission profiles are enforced by the native harness tool
  allowlists (for example `read,grep,find,ls[,edit,write]` for Pi and
  `Read,Glob,Grep[,Edit,Write]` for Claude Code). The scheduler adds no
  network access of its own; note that provider API transport to the model
  still occurs through the native harness itself.

## Data handling

- Prompts, job snapshots, source context, run snapshots, lifecycle events,
  and results are stored in a local SQLite database under
  `~/.local/state/herdr-bots/` with an owner-private parent directory.
- Prompts are staged in owner-private temporary files and removed after
  start.
- Herdr retains its own panes, transcripts, and worktrees for each run; Herdr
  Bots reads bounded run state from them via the `herdr` CLI and does not
  copy them elsewhere.
- No telemetry, crash reporting, or outbound reporting exists in this code.

## Operational boundaries

- Manual pause (global or per job), run cancellation, launchd service
  unload, and worktree cleanup are always available and never automated away.
- The scheduler has no automatic retry, no automatic deletion, and no push,
  merge, message, or publication authority. Terminal states are immutable.
- Installing or loading the launchd service, linking the plugin, and
  enabling the first job are separate explicit human actions.

## Non-goals

Herdr Bots does not sandbox arbitrary code, does not manage secrets beyond
delegating to native harnesses, and does not protect a machine from a
compromised config file or a compromised repository. Treat the config file,
the state database, and run worktrees as sensitive local data.
