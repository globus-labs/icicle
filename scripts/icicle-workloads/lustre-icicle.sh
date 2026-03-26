#!/usr/bin/env bash
#
# lustre-icicle.sh — Runs icicle lustre on a Lustre filesystem, generates a
# workload exercising all changelog event types, then shuts down.
#
# Usage:
#   bash scripts/icicle-workloads/lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX)
#   bash scripts/icicle-workloads/lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --changelog-mode
#   bash scripts/icicle-workloads/lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --reduction-rules --ignore-events OPEN
#   bash scripts/icicle-workloads/lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --kafka-msk
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PYTHON="$ROOT_DIR/.venv/bin/python"

WATCH_DIR="${1:?ERROR: watch_dir is required (e.g. /mnt/fs0/workdir)}"
shift

# Lustre defaults.
LUSTRE_MOUNT="/mnt/fs0"
MDT="fs0-MDT0000"
FSNAME="fs0"

# Parse optional flags that expand into icicle CLI args.
EXTRA_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --json)
            JSON_OUT="$WATCH_DIR/lustre-out.json"
            EXTRA_ARGS+=(--json "$JSON_OUT")
            ;;
        --kafka-msk)
            EXTRA_ARGS+=(--kafka lustre-out --kafka-msk)
            ;;
        --mdt)
            shift; MDT="$1"
            ;;
        --mount)
            shift; LUSTRE_MOUNT="$1"
            ;;
        --fsname)
            shift; FSNAME="$1"
            ;;
        *)
            EXTRA_ARGS+=("$1")
            ;;
    esac
    shift
done

# Check Lustre is available.
if ! mountpoint -q "$LUSTRE_MOUNT"; then
    echo "ERROR: $LUSTRE_MOUNT is not mounted." >&2
    exit 1
fi

# Create watch dir on the target MDT if it doesn't exist.
if [ ! -d "$WATCH_DIR" ]; then
    MDT_INDEX=$(printf '%d' "0x${MDT##*-MDT}")
    sudo lfs mkdir -i "$MDT_INDEX" "$WATCH_DIR"
    sudo chown "$(id -u):$(id -g)" "$WATCH_DIR"
fi

# Start from current changelog position.
STARTREC=$(sudo lfs changelog "$MDT" | tail -1 | awk '{print $1}')
STARTREC=$((STARTREC + 1))

# Start icicle lustre monitor, forwarding remaining args.
$PYTHON -m src.icicle lustre \
    --mdt "$MDT" --mount "$LUSTRE_MOUNT" --fsname "$FSNAME" \
    --startrec "$STARTREC" --poll-interval 0.5 -v \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} &
PID=$!
trap 'kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null' EXIT
sleep 2

log() { echo "  [$1]" >&2; }

# === Workload: exercises every Lustre changelog event type ===

log "Create file"
echo "hello" > "$WATCH_DIR/file.txt"
sleep 0.5

log "Append to file"
echo "world" >> "$WATCH_DIR/file.txt"
sleep 0.5

log "Overwrite file"
echo "replaced" > "$WATCH_DIR/file.txt"
sleep 0.5

log "Touch file"
touch "$WATCH_DIR/file.txt"
sleep 0.5

log "chmod 755"
chmod 755 "$WATCH_DIR/file.txt"
sleep 0.3

log "chmod 644"
chmod 644 "$WATCH_DIR/file.txt"
sleep 0.3

log "Rename file -> renamed.txt"
mv "$WATCH_DIR/file.txt" "$WATCH_DIR/renamed.txt"
sleep 0.5

log "Create hard link"
ln "$WATCH_DIR/renamed.txt" "$WATCH_DIR/hardlink.txt"
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
sleep 0.5

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
setfattr -n user.test.attr -v "value" "$WATCH_DIR/renamed.txt" 2>/dev/null || true
sleep 0.3

log "Remove xattr"
setfattr -x user.test.attr "$WATCH_DIR/renamed.txt" 2>/dev/null || true
sleep 0.3

log "Delete hard link"
rm "$WATCH_DIR/hardlink.txt"
sleep 0.3

log "Delete file"
rm "$WATCH_DIR/renamed.txt"
sleep 0.3

log "Delete deep/ recursively"
rm -rf "$WATCH_DIR/deep"
sleep 0.5

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
