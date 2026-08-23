# Contributing to Herdr Bots

Thank you for considering a contribution. Herdr Bots is a safety-first local
scheduler; changes that weaken the lifecycle or authority contract will not
be accepted even when they are convenient.

## Prerequisites

- macOS for the full plugin surface (launchd service, Herdr integration).
  Linux works for builds and tests as portability evidence only.
- Go matching the version in `go.mod`.
- Git.
- Herdr 0.8.0+ for any change touching workspaces, panes, or agents.

## Development loop

```bash
make check   # module verify, gofmt check, vet, race tests, build, shell syntax, release gate
make build
make test    # race-enabled tests only
make fmt     # rewrite formatting
```

`make check` must pass before a pull request is worth reviewing. Engine tests
use fake Herdr and harness clients; no live Herdr, harness, or network is
needed.

## Conventions

1. Persist a run before creating any workspace or agent.
2. Never substitute a provider, model, harness, or permission profile.
3. The saved prompt is the authority; event IDs and callers carry no payload.
4. Do not answer unattended approval prompts. Record the run as blocked.
5. Never retry an agent automatically.
6. Fresh worktrees are mandatory in version one. Root runs are rejected.
7. Change-gated jobs persist an exact source commit before provisioning, pin
   the worktree to that commit, and skip unchanged occurrences without
   creating a workspace.
8. Terminal states are immutable.

## Adding a lifecycle feature

1. Add the immutable field to the configuration snapshot.
2. Add or migrate durable state before introducing an external effect.
3. Add a compare-and-set transition and a typed failure code.
4. Add a fake-client test proving duplicate effects cannot occur.
5. Update `bots.example.yaml` and `README.md`.
6. Add a release-gate or focused test if the change touches identity,
   artefacts, or authority admission.

## Commit style and pull requests

Use conventional commit prefixes (`feat:`, `fix:`, `test:`, `docs:`,
`chore:`). Stage named files rather than blanket adds. Keep pull requests
small, describe the lifecycle effect your change introduces, and call out
anything that touches the safety contract.

## Security

Do not open issues for vulnerabilities; see [SECURITY.md](SECURITY.md).
