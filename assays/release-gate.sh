#!/bin/sh
# Deterministic release gate for the Herdr Bots public release candidate.
#
# Fails when a required public artefact is missing, the root launcher or
# manifest pane command is wrong, the module/CLI/plugin/service identities
# disagree, an example job is enabled, or forbidden private/internal identity
# strings remain in the tracked release tree. Intended public owner strings
# (Terry Li, terry-li-hm, Thomas Legrand) stay valid.
#
# This gate is static evidence only. It checks that make check separately runs
# the behavioral launcher assay; this script itself never builds, installs,
# links, publishes, or mutates anything.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)

fail() {
    printf 'release-gate: FAIL %s\n' "$*" >&2
    exit 1
}
note() {
    printf 'release-gate: ok %s\n' "$*"
}

# 1. Required public artefacts.
for artefact in \
    .github/workflows/ci.yml \
    .gitignore \
    AGENTS.md \
    CHANGELOG.md \
    CONTRIBUTING.md \
    LICENSE \
    NOTICE \
    README.md \
    SECURITY.md \
    assays/launcher-assay.sh \
    assays/release-gate.sh \
    bots.example.yaml \
    cmd/herdr-bots/main.go \
    docs/release.md \
    go.mod \
    herdr-bots \
    herdr-plugin.toml
do
    [ -f "$root/$artefact" ] || fail "missing required artefact: $artefact"
done
[ -x "$root/herdr-bots" ] || fail 'root launcher herdr-bots exists but is not executable'
[ -x "$root/assays/launcher-assay.sh" ] || fail 'launcher assay exists but is not executable'
[ ! -e "$root/CLAUDE.md" ] || fail 'private CLAUDE.md must not remain in the release tree'
legacy_dir=cmd/vive"sca"-automations
[ ! -e "$root/$legacy_dir" ] || fail "legacy cmd directory $legacy_dir must not remain"
note 'required artefacts present'

# 2. Launcher and manifest pane command.
grep -q 'command = \["./herdr-bots", "pane"\]' "$root/herdr-plugin.toml" || \
    fail 'manifest pane command must be ["./herdr-bots", "pane"]'
grep -q 'go build -trimpath .*-o "\$build_tmp" ./cmd/herdr-bots' "$root/herdr-bots" || \
    fail 'root launcher must build ./cmd/herdr-bots'
grep -q '^\t\./assays/launcher-assay.sh$' "$root/Makefile" || \
    fail 'make check must run the behavioral launcher assay'
note 'launcher, behavioral assay, and manifest command agree'

# 3. One coherent public identity across module, CLI, plugin, and service.
grep -q '^module github.com/terry-li-hm/herdr-bots$' "$root/go.mod" || \
    fail 'go.mod module must be github.com/terry-li-hm/herdr-bots'
grep -q '^id = "terry.herdr-bots"$' "$root/herdr-plugin.toml" || \
    fail 'plugin id must be terry.herdr-bots'
grep -q '^version = "0.1.0"$' "$root/herdr-plugin.toml" || \
    fail 'plugin manifest version must be 0.1.0'
grep -q 'const Label = "com.terry.herdr-bots"' "$root/internal/service/launchd.go" || \
    fail 'launchd service label must be com.terry.herdr-bots'
grep -q -- '-o bin/herdr-bots ./cmd/herdr-bots' "$root/Makefile" || \
    fail 'Makefile build target must produce bin/herdr-bots'
grep -q '^## \[0.1.0\]' "$root/CHANGELOG.md" || \
    fail 'CHANGELOG.md must carry the 0.1.0 entry'
note 'module, CLI, plugin, and service identities agree'

# 4. Examples ship disabled.
enabled_true=$(grep -c 'enabled: *true' "$root/bots.example.yaml" || true)
[ "$enabled_true" -eq 0 ] || fail 'bots.example.yaml must not contain an enabled job'
enabled_any=$(grep -c 'enabled:' "$root/bots.example.yaml" || true)
enabled_false=$(grep -c 'enabled: *false' "$root/bots.example.yaml" || true)
[ "$enabled_any" -eq "$enabled_false" ] || \
    fail 'every enabled: line in bots.example.yaml must be explicitly false'
job_count=$(grep -c '^  - id:' "$root/bots.example.yaml" || true)
[ "$job_count" -eq "$enabled_any" ] || \
    fail 'every example job must declare enabled: false'
note "example jobs disabled ($job_count/$job_count)"

# 4b. Corrected publication contracts.
grep -q 'https://github.com/herdrdev/herdr' "$root/README.md" || \
    fail 'README must point the Herdr prerequisite at github.com/herdrdev/herdr'
grep -q 'go install github.com/terry-li-hm/herdr-bots/cmd/herdr-bots@v0.1.0' "$root/README.md" || \
    fail 'README must document the pinned go install path for the shell CLI'
grep -q 'herdr plugin uninstall terry.herdr-bots' "$root/README.md" || \
    fail 'README must use the exact current uninstall command'
grep -q 'may fetch Go modules' "$root/README.md" || \
    fail 'README must disclose that first builds may fetch Go modules'
if grep -qiE 'approved non-claude' "$root/README.md" "$root/AGENTS.md"; then
    fail 'non-Claude routes must be described as configured/reported by Pi, not approved'
fi
grep -q 'reported by Pi' "$root/README.md" || \
    fail 'README must state non-Claude routes are configured/reported by Pi'
grep -q 'reported by Pi' "$root/AGENTS.md" || \
    fail 'AGENTS.md must state non-Claude routes are configured/reported by Pi'
grep -q 'actual release date' "$root/docs/release.md" || \
    fail 'docs/release.md must require setting the actual release date before tag/release'
grep -q '^## \[0.1.0\]' "$root/CHANGELOG.md" || \
    fail 'CHANGELOG must carry the 0.1.0 section'
grep -qx 'Copyright (c) 2026 Thomas Legrand' "$root/LICENSE" || \
    fail 'LICENSE must keep the original MIT copyright as a separate unannotated line'
grep -qx 'Copyright (c) 2026 Terry Li' "$root/LICENSE" || \
    fail 'LICENSE must keep the implementation copyright as a separate unannotated line'
for marker in 'describe --tags --always' 'status --porcelain=v1 -z --untracked-files=all' \
    'diff --binary --no-ext-diff HEAD' 'ls-files --others --exclude-standard -z' \
    'hash-object'; do
    grep -q -- "$marker" "$root/herdr-bots" || \
        fail "launcher dirty-cache hardening is missing $marker"
done
# The dirty cache key must preserve provenance: <base-revision>-dirty-<hash>.
grep -q 'revision="$revision-dirty-$changes"' "$root/herdr-bots" || \
    fail 'launcher dirty key must compose <base-revision>-dirty-<content-hash>'
if grep -q 'revision="dirty-' "$root/herdr-bots"; then
    fail 'launcher must not compose a dirty key without its base revision'
fi
# Launcher cache-boundary hardening: strict fail-closed admission of the
# cache directory and cache entries, explicit 0700 chmod of built artefacts.
for marker in 'path_mode()' 'require_cache_dir()' 'require_entry()' \
    'cache directory is a symlink, refusing' \
    'cache entry is a symlink, refusing' \
    'cache entry mode is not 0700, refusing' \
    'cache directory mode is not 0700, refusing' \
    'cache entry is not owned by effective uid' \
    'cache directory is not owned by effective uid' \
    'chmod 700 "$cache_dir"' 'chmod 700 "$build_tmp"' \
    'source changed during build' 'ln "$build_tmp" "$binary"'; do
    grep -qF -- "$marker" "$root/herdr-bots" || \
        fail "launcher cache-boundary hardening is missing: $marker"
done
entry_admissions=$(grep -cF 'require_entry "$binary"' "$root/herdr-bots" || true)
[ "$entry_admissions" -ge 2 ] || \
    fail 'launcher must validate the final entry both at admission and before exec'
if grep -qF 'if [ ! -x "$binary" ]' "$root/herdr-bots"; then
    fail 'launcher must not admit entries by executability alone (symlinks would pass)'
fi
# Verifier authority disclosure: the saved verifier is an unrestricted local
# process outside the agent permission profile, and its limits are not a
# sandbox. Both public documents must state this.
for doc in README.md SECURITY.md; do
    grep -qF 'unrestricted local process' "$root/$doc" || \
        fail "$doc must disclose the verifier runs as an unrestricted local process"
    grep -qF 'not a sandbox' "$root/$doc" || \
        fail "$doc must state that verifier limits are not a sandbox"
done
grep -qF 'syscall.O_NOFOLLOW|syscall.O_CLOEXEC' "$root/internal/config/config.go" || \
    fail 'config loader must open with O_NOFOLLOW and CLOEXEC'
grep -qF 'stat.Uid != uint32(os.Geteuid())' "$root/internal/config/config.go" || \
    fail 'config loader must enforce effective-uid ownership'
for doc in README.md SECURITY.md; do
    grep -qF 'macOS ACLs are not inspected' "$root/$doc" || \
        fail "$doc must disclose the config ACL inspection limitation"
done
note 'corrected publication contracts hold'

# 5. No forbidden private/internal identity strings in the release tree.
#    Allowlisted public strings (Terry Li, terry-li-hm, Thomas Legrand,
#    terry.herdr-bots) are intentional and not listed here. The scan covers
#    tracked and untracked (not ignored) files so a pre-commit working tree
#    is gated as strictly as a clean export.
if ! command -v git >/dev/null 2>&1 || ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    fail 'release gate must run inside a git checkout of the release tree'
fi
leaks=$(git -C "$root" ls-files --cached --others --exclude-standard | while IFS= read -r f; do
    [ "$f" = "assays/release-gate.sh" ] && continue
    [ -f "$root/$f" ] && grep -HniE \
        -e 'vive'"sca" \
        -e 'cumora' \
        -e 'chromatin' \
        -e 'epigenome' \
        -e '/Users/terry' \
        -e 'docs/solutions' \
        -e 'oscillators' \
        -e 'pilot' \
        -e 'plugin remove' \
        "$root/$f"
done || true)
if [ -n "$leaks" ]; then
    printf 'release-gate: FAIL forbidden private/internal identity strings:\n%s\n' "$leaks" >&2
    exit 1
fi
note 'release tree free of private/internal identity strings'
printf 'release-gate: passed\n'
