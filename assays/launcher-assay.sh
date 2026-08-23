#!/bin/sh
# Behavioral assay for the source launcher. Everything mutable lives under an
# assay-owned mktemp root; the checkout under test is only read.
set -eu
umask 077

source_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/herdr-bots-launcher.XXXXXX") || exit 1
trap 'rm -rf -- "$test_root"' EXIT HUP INT TERM
real_git=$(command -v git) || { echo 'launcher-assay: git unavailable' >&2; exit 1; }
real_path=$PATH
fakebin=$test_root/fake-bin
mkdir -p "$fakebin"

fail() {
    printf 'launcher-assay: FAIL %s\n' "$*" >&2
    exit 1
}
note() {
    printf 'launcher-assay: ok %s\n' "$*"
}

cat >"$fakebin/git" <<'EOF'
#!/bin/sh
if [ "${FAIL_GIT_STATUS:-}" = 1 ]; then
    for argument do
        [ "$argument" != status ] || { echo 'injected git status failure' >&2; exit 73; }
    done
fi
exec "$REAL_GIT" "$@"
EOF
cat >"$fakebin/go" <<'EOF'
#!/bin/sh
[ "${1:-}" = build ] || exit 64
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = -o ]; then
        shift
        output=${1:-}
        break
    fi
    shift
done
[ -n "$output" ] || exit 65
printf '%s\n' "$$" >>"$BUILD_COUNT"
[ -z "${MUTATE_FILE:-}" ] || printf 'mutation from build\n' >>"$MUTATE_FILE"
[ -z "${BUILD_DELAY:-}" ] || sleep "$BUILD_DELAY"
{
    printf '%s\n' '#!/bin/sh'
    printf "printf 'fake-build-%s\\n'\n" "$$"
} >"$output"
EOF
chmod 755 "$fakebin/git" "$fakebin/go"

make_fixture() {
    fixture=$1
    mkdir -p "$fixture"
    cp "$source_root/herdr-bots" "$fixture/herdr-bots"
    chmod 755 "$fixture/herdr-bots"
    printf 'tracked source\n' >"$fixture/source.txt"
    "$real_git" -C "$fixture" init -q
    "$real_git" -C "$fixture" config user.name 'Launcher Assay'
    "$real_git" -C "$fixture" config user.email 'launcher-assay.invalid@example.invalid'
    "$real_git" -C "$fixture" add herdr-bots source.txt
    "$real_git" -C "$fixture" commit -q -m fixture
}

run_launcher() {
    run_repo=$1
    run_state=$2
    shift 2
    REAL_GIT=$real_git PATH="$fakebin:$real_path" HERDR_PLUGIN_STATE_DIR="$run_state" "$@" "$run_repo/herdr-bots" --version
}

line_count() {
    if [ -f "$1" ]; then wc -l <"$1" | tr -d ' '; else printf '0\n'; fi
}

# Clean first build and exact cache reuse.
repo=$test_root/clean
state=$test_root/clean-state
count=$test_root/clean-builds
make_fixture "$repo"
BUILD_COUNT=$count run_launcher "$repo" "$state" env >"$test_root/clean.1"
entry=$(find "$state/bin" -type f -name 'herdr-bots-*' -print)
[ -n "$entry" ] || fail 'clean build did not publish an entry'
inode_before=$(ls -di "$entry" | awk '{print $1}')
BUILD_COUNT=$count run_launcher "$repo" "$state" env >"$test_root/clean.2"
inode_after=$(ls -di "$entry" | awk '{print $1}')
[ "$inode_before" = "$inode_after" ] || fail 'clean cache entry was replaced'
[ "$(line_count "$count")" = 1 ] || fail 'clean cache was rebuilt instead of reused'
cmp -s "$test_root/clean.1" "$test_root/clean.2" || fail 'clean reuse executed a different binary'
note 'clean first build is reused'

# Checkout and source paths containing spaces.
repo="$test_root/path with spaces"
state="$test_root/state with spaces"
count="$test_root/space builds"
make_fixture "$repo"
printf 'untracked content\n' >"$repo/untracked name with spaces.txt"
BUILD_COUNT=$count run_launcher "$repo" "$state" env >"$test_root/spaces.out" || fail 'paths containing spaces were rejected'
[ "$(line_count "$count")" = 1 ] || fail 'space-path build did not run once'
note 'checkout and source paths with spaces are preserved'

# Identical bytes under distinct untracked filenames must produce distinct
# keys; the filename stream is therefore part of the fingerprint.
repo=$test_root/untracked-names
state=$test_root/untracked-state
count=$test_root/untracked-builds
make_fixture "$repo"
printf 'same bytes\n' >"$repo/first name.txt"
BUILD_COUNT=$count run_launcher "$repo" "$state" env >/dev/null
rm "$repo/first name.txt"
printf 'same bytes\n' >"$repo/second name.txt"
BUILD_COUNT=$count run_launcher "$repo" "$state" env >/dev/null
[ "$(line_count "$count")" = 2 ] || fail 'distinct untracked filenames reused one cache key'
[ "$(find "$state/bin" -type f -name 'herdr-bots-*' | wc -l | tr -d ' ')" = 2 ] || fail 'distinct untracked names did not publish distinct entries'
note 'distinct untracked filenames with identical content have distinct keys'

# A checked git operation must abort the launcher.
repo=$test_root/git-failure
state=$test_root/git-failure-state
count=$test_root/git-failure-builds
make_fixture "$repo"
if BUILD_COUNT=$count FAIL_GIT_STATUS=1 run_launcher "$repo" "$state" env >"$test_root/git-failure.out" 2>"$test_root/git-failure.err"; then
    fail 'injected git status failure was ignored'
fi
unset FAIL_GIT_STATUS
[ "$(line_count "$count")" = 0 ] || fail 'build ran after git fingerprint failure'
grep -q 'git status failed' "$test_root/git-failure.err" || fail 'git failure was not reported honestly'
note 'injected git failure aborts before build'

# Mutating tracked source from the fake compiler must be detected before
# publication.
repo=$test_root/source-mutation
state=$test_root/source-mutation-state
count=$test_root/source-mutation-builds
make_fixture "$repo"
if BUILD_COUNT=$count MUTATE_FILE="$repo/source.txt" run_launcher "$repo" "$state" env >"$test_root/mutation.out" 2>"$test_root/mutation.err"; then
    fail 'source mutation during build was accepted'
fi
unset MUTATE_FILE
grep -q 'source changed during build' "$test_root/mutation.err" || fail "source mutation refusal was not reported: $(cat "$test_root/mutation.err")"
[ "$(find "$state/bin" -type f -name 'herdr-bots-*' | wc -l | tr -d ' ')" = 0 ] || fail 'mutated build was published'
note 'source mutation during build is refused'

# Cache directory symlinks and wrong modes fail closed.
repo=$test_root/cache-boundary
make_fixture "$repo"
mkdir -p "$test_root/elsewhere" "$test_root/symlink-state"
chmod 700 "$test_root/elsewhere" "$test_root/symlink-state"
ln -s "$test_root/elsewhere" "$test_root/symlink-state/bin"
if BUILD_COUNT=$test_root/symlink-builds run_launcher "$repo" "$test_root/symlink-state" env >/dev/null 2>&1; then
    fail 'symlink cache directory was accepted'
fi
mkdir -p "$test_root/mode-state/bin"
chmod 755 "$test_root/mode-state/bin"
if BUILD_COUNT=$test_root/mode-builds run_launcher "$repo" "$test_root/mode-state" env >/dev/null 2>&1; then
    fail 'wrong-mode cache directory was accepted'
fi

# Existing entry symlinks and wrong modes also fail closed.
state=$test_root/entry-state
count=$test_root/entry-builds
BUILD_COUNT=$count run_launcher "$repo" "$state" env >/dev/null
entry=$(find "$state/bin" -type f -name 'herdr-bots-*' -print)
rm "$entry"
ln -s "$test_root/elsewhere/not-a-binary" "$entry"
if BUILD_COUNT=$count run_launcher "$repo" "$state" env >/dev/null 2>&1; then
    fail 'symlink cache entry was accepted'
fi
rm "$entry"
printf '%s\n' '#!/bin/sh' 'exit 0' >"$entry"
chmod 755 "$entry"
if BUILD_COUNT=$count run_launcher "$repo" "$state" env >/dev/null 2>&1; then
    fail 'wrong-mode cache entry was accepted'
fi
note 'cache symlinks and wrong modes are rejected'

# Ownership rejection is real only where this assay can create a mismatched
# owner. Never simulate it with a same-owner marker.
if [ "$(id -u)" = 0 ]; then
    state=$test_root/owner-state
    mkdir -p "$state/bin"
    chmod 700 "$state/bin"
    chown 65534 "$state/bin" || fail 'root assay could not create wrong-owner cache fixture'
    if BUILD_COUNT=$test_root/owner-builds run_launcher "$repo" "$state" env >/dev/null 2>&1; then
        fail 'wrong-owner cache directory was accepted'
    fi
    note 'wrong-owner cache directory is rejected'
else
    printf 'launcher-assay: SKIP ownership rejection unsupported without root; no ownership result fabricated\n'
fi

# Two first users may both compile, but no builder may clobber the winner and
# both invocations must converge on that one executable.
repo=$test_root/concurrent
state=$test_root/concurrent-state
count=$test_root/concurrent-builds
make_fixture "$repo"
(BUILD_COUNT=$count BUILD_DELAY=1 run_launcher "$repo" "$state" env >"$test_root/concurrent.1" 2>"$test_root/concurrent.1.err") &
pid1=$!
(BUILD_COUNT=$count BUILD_DELAY=1 run_launcher "$repo" "$state" env >"$test_root/concurrent.2" 2>"$test_root/concurrent.2.err") &
pid2=$!
wait "$pid1" || fail "first concurrent launcher failed: $(cat "$test_root/concurrent.1.err")"
wait "$pid2" || fail "second concurrent launcher failed: $(cat "$test_root/concurrent.2.err")"
cmp -s "$test_root/concurrent.1" "$test_root/concurrent.2" || fail 'concurrent builders did not execute the same winner'
[ "$(find "$state/bin" -type f -name 'herdr-bots-*' | wc -l | tr -d ' ')" = 1 ] || fail 'concurrent publication produced more than one final entry'
builds=$(line_count "$count")
[ "$builds" -ge 1 ] && [ "$builds" -le 2 ] || fail "unexpected concurrent build count: $builds"
note 'concurrent first builds atomically converge on one winner'

printf 'launcher-assay: passed (mktemp root %s removed on exit)\n' "$test_root"
