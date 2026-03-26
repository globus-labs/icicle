#!/usr/bin/env bash
#
# fswatch-icicle.sh — Runs icicle fswatch on a directory, generates a
# workload exercising all event types, then shuts down.
#
# Usage:
#   bash scripts/icicle-workloads/fswatch-icicle.sh $(mktemp -d)
#   bash scripts/icicle-workloads/fswatch-icicle.sh $(mktemp -d) --changelog-mode
#   bash scripts/icicle-workloads/fswatch-icicle.sh $(mktemp -d) --reduction-rules
#   bash scripts/icicle-workloads/fswatch-icicle.sh $(mktemp -d) --json
#   bash scripts/icicle-workloads/fswatch-icicle.sh $(mktemp -d) --kafka-msk
#
# Note: macOS FSEvents coalesces rapid writes at the kernel level (e.g.
# 10 overwrites in a tight loop may produce only 2 fswatch events).
# With --reduction-rules, batch_reduced varies between runs because
# reduction only fires when two events (e.g. CREATE+REMOVE) land in the
# same icicle read() batch, which depends on this coalescing timing.
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PYTHON="$ROOT_DIR/.venv/bin/python"

WATCH_DIR="${1:?ERROR: watch_dir is required}"
shift
mkdir -p "$WATCH_DIR"

# Parse optional flags that expand into icicle CLI args.
EXTRA_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --json)
            JSON_OUT="$WATCH_DIR/fswatch-out.json"
            EXTRA_ARGS+=(--json "$JSON_OUT")
            ;;
        --kafka-msk)
            EXTRA_ARGS+=(--kafka fswatch-out --kafka-msk)
            ;;
        *)
            EXTRA_ARGS+=("$1")
            ;;
    esac
    shift
done

# Start icicle fswatch, forwarding remaining args.
$PYTHON -m src.icicle fswatch "$WATCH_DIR" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} &
PID=$!
trap 'kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null' EXIT
sleep 1

log() { echo "  [$1]" >&2; }

# === Workload: exercises every fswatch event type ===

log "Create file"
echo "hello" > "$WATCH_DIR/file.txt"
sleep 0.3

log "Append to file"
echo "world" >> "$WATCH_DIR/file.txt"
sleep 0.3

log "Overwrite file"
echo "replaced" > "$WATCH_DIR/file.txt"
sleep 0.3

log "Touch file"
touch "$WATCH_DIR/file.txt"
sleep 0.3

log "chmod 755"
chmod 755 "$WATCH_DIR/file.txt"
sleep 0.3

log "chmod 644"
chmod 644 "$WATCH_DIR/file.txt"
sleep 0.3

log "Rename file -> renamed.txt"
mv "$WATCH_DIR/file.txt" "$WATCH_DIR/renamed.txt"
sleep 0.3

log "Create symlink"
ln -s "$WATCH_DIR/renamed.txt" "$WATCH_DIR/link.txt"
sleep 0.3

log "Create hard link"
ln "$WATCH_DIR/renamed.txt" "$WATCH_DIR/hardlink.txt"
sleep 0.3

log "Write via symlink"
echo "via symlink" >> "$WATCH_DIR/link.txt"
sleep 0.3

log "Create directory"
mkdir "$WATCH_DIR/subdir"
sleep 0.3

log "Create nested file"
echo "nested" > "$WATCH_DIR/subdir/nested.txt"
sleep 0.3

log "Create deep tree (3 levels, 3 files)"
mkdir -p "$WATCH_DIR/deep/a/b/c"
echo "deep1" > "$WATCH_DIR/deep/a/f1.txt"
echo "deep2" > "$WATCH_DIR/deep/a/b/f2.txt"
echo "deep3" > "$WATCH_DIR/deep/a/b/c/f3.txt"
sleep 0.5

log "Rename deep/a -> deep/z"
mv "$WATCH_DIR/deep/a" "$WATCH_DIR/deep/z"
sleep 1

log "Move file across dirs"
mv "$WATCH_DIR/subdir/nested.txt" "$WATCH_DIR/deep/z/moved.txt"
sleep 0.3

log "Copy file"
cp "$WATCH_DIR/renamed.txt" "$WATCH_DIR/copied.txt"
sleep 0.3

log "Truncate file"
: > "$WATCH_DIR/copied.txt"
sleep 0.3

log "Write 4KB binary"
dd if=/dev/urandom of="$WATCH_DIR/binary.bin" bs=1024 count=4 2>/dev/null
sleep 0.3

log "Set xattr"
if [[ "$(uname)" == "Darwin" ]]; then
    xattr -w com.test.attr "value" "$WATCH_DIR/renamed.txt"
else
    setfattr -n user.test.attr -v "value" "$WATCH_DIR/renamed.txt"
fi
sleep 0.3

log "Remove xattr"
if [[ "$(uname)" == "Darwin" ]]; then
    xattr -d com.test.attr "$WATCH_DIR/renamed.txt"
else
    setfattr -x user.test.attr "$WATCH_DIR/renamed.txt"
fi
sleep 0.3

log "Delete symlink"
rm "$WATCH_DIR/link.txt"
sleep 0.3

log "Delete hard link"
rm "$WATCH_DIR/hardlink.txt"
sleep 0.3

log "Delete file"
rm "$WATCH_DIR/renamed.txt"
sleep 0.3

log "Delete deep/ recursively"
rm -rf "$WATCH_DIR/deep"
sleep 0.3

log "Rapid overwrite x10"
for i in $(seq 1 10); do
    echo "overwrite$i" > "$WATCH_DIR/coalesce.txt"
done
sleep 0.5

log "Create/delete cancellation x3"
for i in $(seq 1 3); do
    echo "ephemeral$i" > "$WATCH_DIR/ephemeral_$i.txt"
    rm "$WATCH_DIR/ephemeral_$i.txt"
done
sleep 0.5

log "Create final file"
echo "done" > "$WATCH_DIR/final.txt"
sleep 2
