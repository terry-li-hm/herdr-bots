# Release evidence for Herdr Bots

This document records what a release candidate must prove before the v0.3.0
tag exists, and which actions stay behind separate explicit gates. It is
evidence, not authority: no step here publishes anything. v0.3.0 is a macOS
prerelease (preview) of Herdr Bots.

## Evidence matrix

| # | Requirement | Evidence | Owner of the check |
|---|---|---|---|
| 1 | Full static and test gate | `make check` passes (module verify, gofmt check without rewriting, vet, race-enabled tests, full build, shell syntax, behavioral launcher assay, release gate) | `make check` |
| 2 | No whitespace damage | `git diff --check` is clean | verifier command |
| 3 | No secrets in the release tree | `gitleaks dir . --redact=100 --no-banner --no-color` passes | verifier command |
| 4 | Required public artefacts | README, LICENSE (dual copyright), NOTICE, SECURITY, CONTRIBUTING, CHANGELOG (0.3.0 with preserved 0.2.0, 0.1.1, and 0.1.0 history), AGENTS, bots.example.yaml, herdr-plugin.toml, root launcher, docs/release.md, CI workflow all present | `assays/release-gate.sh` |
| 5 | One coherent public identity | module `github.com/terry-li-hm/herdr-bots`, CLI `cmd/herdr-bots`, plugin id `terry.herdr-bots`, service label `com.terry.herdr-bots`, current version `0.3.0` agree everywhere | `assays/release-gate.sh` |
| 6 | Source-installable plugin | Manifest pane command is `["./herdr-bots", "pane"]`; launcher builds `cmd/herdr-bots` into the private cache and execs it | `assays/release-gate.sh` + `assays/launcher-assay.sh` |
| 7 | Launcher cache behavior | The executable assay creates its own `mktemp` root and disposable Git fixtures with a fake compiler (so it neither uses the network nor claims a production-toolchain integration test). It covers clean inode reuse, paths with spaces, identical untracked content under distinct names, checked Git failure, source mutation during build, cache symlink/mode rejection, and concurrent first-build convergence. Wrong-owner rejection runs only as root and is explicitly reported as unsupported otherwise; no ownership result is fabricated. | `assays/launcher-assay.sh` |
| 8 | Service render validity | `./herdr-bots service render` output passes `plutil -lint` with label `com.terry.herdr-bots`; nothing is installed or loaded | verifier command |
| 9 | Examples ship disabled | Every job in `bots.example.yaml` declares `enabled: false`; example route values are labelled as examples | `assays/release-gate.sh` |
| 10 | No private/internal identity strings | Tracked tree greps clean for forbidden identity strings; public owner strings (`Terry Li`, `terry-li-hm`, `Thomas Legrand`) remain valid | `assays/release-gate.sh` |
| 11 | CI evidence | `.github/workflows/ci.yml` runs `make check` only on macOS with read-only repository permissions, the `go.mod` Go version, and the verified current `actions/checkout@v7` and `actions/setup-go@v7` majors. The first hosted Ubuntu run failed to compile the macOS-only engine (`unix.RenameatxNp` and `unix.RENAME_EXCL`), so no Linux build or test evidence is claimed. | workflow definition + hosted CI result |
| 12 | Changelog dating | The CHANGELOG 0.3.0 entry carries the actual release date `2026-08-25`, not `Unreleased`, in the same commit that will be tagged; the dated 0.2.0, 0.1.1, and 0.1.0 sections and links remain historical | `assays/release-gate.sh` + release operator |
| 13 | Lineage hygiene | The exact release candidate commit descends from the published v0.2.0 tag, and the incremental `v0.2.0..HEAD` range was reviewed before tagging; no history rewrite occurs | release operator |
| 14 | Config admission | Go tests cover exact mode, symlink and non-regular rejection; wrong-owner rejection runs only where the test process can create a genuinely different owner. The implementation and public policy pin no-follow/close-on-exec descriptor admission, regular-file/effective-UID/exact-0600 checks, and final path/inode equality. macOS ACLs are not inspected and must not grant others access. | `go test` + code/policy review |
| 15 | Herdr error-channel patch | Focused tests prove failed Herdr commands parse structured API errors from stdout or stderr, preserve plain stderr fallback, and retry an agent wait after a timeout envelope on stderr | `go test -race ./internal/herdr` |
| 16 | Unread-terminal-run attention guard | Go tests in `internal/config`, `internal/store`, `internal/engine`, and `cmd/herdr-bots` prove the opt-in `attention.max_unread_terminal_runs` guard: config validation of the 1-1000 limit (and exact rejection outside it), durable pause reason `unread_terminal_runs` with persisted pause timestamp (distinct from `manual`), authority-fenced scheduled, event, and manual admission pausing the job before any run/occurrence/workspace/agent is created, counting only unread terminal-state runs, explicit `resume` as the only unblock (marking runs read never auto-resumes), and `list` showing the durable pause reason without marking runs read | `go test ./internal/config ./internal/store ./internal/engine ./cmd/herdr-bots` |
| 17 | Acceptance lanes | Go tests prove config validation of `acceptance.mode` (`mandatory`, `auto`, `sample`), the verifier requirement for `auto` and `sample`, `sample_percent` accepted only for `sample` and only in 1-100, and byte-stable snapshots plus a `mandatory` default when the block is absent; engine and store tests prove the deterministic SHA-256 run-ID bucket classifies a run identically across processes, that the lane and machine reason are written once inside the serialized terminal transition and survive restart and recovery without reclassification, and that auto-reviewed runs start read while sampled and mandatory runs start unread | `go test ./internal/config ./internal/store ./internal/engine ./cmd/herdr-bots` |
| 18 | Bounded review inputs and write scope | Go tests prove input staging limits (at most 32 entries, 16 MiB per file, 64 MiB per run), `{date}`/`{year}`/`{month}` resolution in the job timezone against the scheduled occurrence, refusal of a source that grows during the copy, non-overwriting destinations under the reserved `.herdr-bots/inputs/` directory that no write scope may name, `allowed_write_paths` requiring `repo-write-no-network` and never granting `.git`, and durable receipts recording staged sizes and sha256 digests plus every changed path (ignored files included) with a content fingerprint under `observed_within_scope`; the boundary is observed after the agent settles and rechecked after the verifier, with an identical re-observation accepted and any changed fingerprint failing the run as `bounded_review_failed`. The verdict is post-run evidence under shell-free profiles, not OS containment | `go test ./internal/config ./internal/engine ./cmd/herdr-bots` |
| 19 | Model attestation | Go tests prove opt-in `execution.require_model_attestation` is valid only for `claude-code` jobs with a full `claude-`-prefixed model name (aliases and `harness-default` rejected, Pi routes rejected), that the machine-readable harness result is bound after the command completes and before the verifier runs, requiring a first-party provider, a canonical model equal to the configured name, and nonzero token usage, and that anything absent, malformed, or mismatched fails the run as `model_attestation_failed` rather than downgrading to a weaker verdict; omitting the field leaves snapshots and launches byte-for-byte unchanged | `go test ./internal/config ./internal/engine ./internal/herdr` |
| 20 | Verifier process-group quiescence | Go tests prove the supervisor confirms the verifier's process group is gone before the run settles, observing same-user group membership before signaling, skipping a group with zero observed members as success rather than an error, signaling at most once and only when members are observed, re-observing membership after `EPERM` or `ESRCH` instead of assuming an outcome, and failing closed while owned or same-user members remain, so absence is proved before settlement | `go test -race ./internal/engine` |

## Separate explicit gates (not performed by this repository's build)

Each of the following is an intentional human action that nothing in `make
check`, the release gate, or CI performs or authorizes:

1. Creating the public `terry-li-hm/herdr-bots` repository.
2. Pushing any history or export to it.
3. Creating and pushing the `v0.3.0` tag.
4. Publishing the GitHub release.
5. Changing repository visibility.
6. Announcing the release anywhere.
7. Installing, linking, or enabling the plugin on any machine (including the
   author's), or loading the launchd service.

## Release checklist

1. Confirm every row of the evidence matrix on the exact commit intended for
   release.
2. Confirm the CHANGELOG 0.3.0 entry has the actual release date
   `2026-08-25`, is not `Unreleased`, and preserves the 0.2.0, 0.1.1, and
   0.1.0 sections and links.
3. Confirm the exact candidate commit descends from the published v0.2.0
   tag and that the incremental `v0.2.0..HEAD` range was reviewed (no
   history rewrite; the existing public repository and lineage are kept).
4. Only then, and as separate decisions, walk the explicit gates above.
