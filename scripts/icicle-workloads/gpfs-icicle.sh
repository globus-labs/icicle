#!/usr/bin/env bash
#
# gpfs-icicle.sh — Exercises GPFS inotify event types via Kafka (mmwatch),
# optionally through the icicle monitor pipeline.
#
# Usage:
#   bash scripts/icicle-workloads/gpfs-icicle.sh                          # raw Kafka consumer
#   bash scripts/icicle-workloads/gpfs-icicle.sh --icicle                 # icicle → stdout
#   bash scripts/icicle-workloads/gpfs-icicle.sh --dir /ibm/gpfs/fs1/test # custom GPFS directory
#   bash scripts/icicle-workloads/gpfs-icicle.sh --topic my-topic         # custom mmwatch topic
#   bash scripts/icicle-workloads/gpfs-icicle.sh --num-consumers 4        # parallel consumers
#   bash scripts/icicle-workloads/gpfs-icicle.sh --queue-type shm         # shared-memory queue
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV="$ROOT_DIR/.venv"
PYTHON="$VENV/bin/python"

# --- Defaults ---
GPFS_MOUNT="${GPFS_MOUNT:-/ibm/fs1/fset1}"
TOPIC="${TOPIC:-fset1-topic}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
USE_ICICLE=false
CUSTOM_DIR=false
NUM_CONSUMERS=1
QUEUE_TYPE="stdlib"

while [ $# -gt 0 ]; do
    case "$1" in
        --icicle)        USE_ICICLE=true ;;
        --dir)
            [ $# -ge 2 ] || { echo "ERROR: --dir requires a value" >&2; exit 1; }
            shift; WATCH_DIR="$1"; CUSTOM_DIR=true ;;
        --topic)
            [ $# -ge 2 ] || { echo "ERROR: --topic requires a value" >&2; exit 1; }
            shift; TOPIC="$1" ;;
        --bootstrap)
            [ $# -ge 2 ] || { echo "ERROR: --bootstrap requires a value" >&2; exit 1; }
            shift; BOOTSTRAP="$1" ;;
        --mount)
            [ $# -ge 2 ] || { echo "ERROR: --mount requires a value" >&2; exit 1; }
            shift; GPFS_MOUNT="$1" ;;
        --num-consumers)
            [ $# -ge 2 ] || { echo "ERROR: --num-consumers requires a value" >&2; exit 1; }
            shift; NUM_CONSUMERS="$1" ;;
        --queue-type)
            [ $# -ge 2 ] || { echo "ERROR: --queue-type requires a value" >&2; exit 1; }
            shift; QUEUE_TYPE="$1" ;;
        *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
    shift
done

# Set WATCH_DIR after arg parsing so --mount is respected
if ! $CUSTOM_DIR; then
    WATCH_DIR="$GPFS_MOUNT/icicle_demo_$$_$(date +%s)"
fi

# --- Prerequisites ---
if [ ! -d "$GPFS_MOUNT" ]; then
    echo "ERROR: GPFS path $GPFS_MOUNT not found. Set GPFS_MOUNT or use --mount." >&2
    exit 1
fi

echo "Verifying mmwatch Kafka topic '$TOPIC' is reachable..." >&2
if ! $PYTHON -c "
from confluent_kafka.admin import AdminClient
a = AdminClient({'bootstrap.servers': '$BOOTSTRAP'})
m = a.list_topics(timeout=5)
assert '$TOPIC' in m.topics, f'Topic $TOPIC not found. Available: {list(m.topics.keys())}'
print('  Topic $TOPIC found with', len(m.topics['$TOPIC'].partitions), 'partition(s)')
" 2>&1 | head -5 >&2; then
    echo "ERROR: Cannot reach Kafka topic '$TOPIC' at $BOOTSTRAP" >&2
    exit 1
fi

# --- Cleanup trap ---
cleanup() {
    if [ -n "${PID:-}" ]; then
        echo "Stopping monitor (pid $PID)..." >&2
        kill "$PID" 2>/dev/null || true
        wait "$PID" 2>/dev/null || true
    fi
    if ! $CUSTOM_DIR; then
        rm -rf "$WATCH_DIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- Create watch directory ---
if ! $CUSTOM_DIR; then
    rm -rf "$WATCH_DIR" 2>/dev/null || true
fi
mkdir -p "$WATCH_DIR"

# --- Start the monitor or raw Kafka consumer ---
if $USE_ICICLE; then
    MONITOR_ARGS=(gpfs --topic "$TOPIC" --bootstrap "$BOOTSTRAP"
                  --num-consumers "$NUM_CONSUMERS" --queue-type "$QUEUE_TYPE"
                  --poll-timeout 10 -v)
    echo "=== gpfs-icicle (icicle → stdout) ===" >&2
    echo "GPFS mount:  $GPFS_MOUNT" >&2
    echo "Watch dir:   $WATCH_DIR" >&2
    echo "Kafka topic: $TOPIC ($BOOTSTRAP)" >&2
    echo "Consumers:   $NUM_CONSUMERS (queue: $QUEUE_TYPE)" >&2
    echo "---" >&2
    $PYTHON -m src.icicle "${MONITOR_ARGS[@]}" &
    PID=$!
else
    echo "=== gpfs-icicle (raw Kafka consumer) ===" >&2
    echo "GPFS mount:  $GPFS_MOUNT" >&2
    echo "Watch dir:   $WATCH_DIR" >&2
    echo "Kafka topic: $TOPIC ($BOOTSTRAP)" >&2
    echo "Output: raw JSON events from mmwatch" >&2
    echo "---" >&2
    $PYTHON -c "
import json, sys, signal
from confluent_kafka import Consumer, TopicPartition

running = True
def stop(sig, frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

c = Consumer({
    'bootstrap.servers': '$BOOTSTRAP',
    'group.id': 'gpfs-icicle-raw-$$',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
})
c.subscribe(['$TOPIC'])
count = 0
while running:
    msg = c.poll(timeout=1.0)
    if msg is None or msg.error():
        continue
    try:
        data = json.loads(msg.value().decode())
        print(json.dumps(data))
        count += 1
    except Exception:
        pass
c.close()
print(f'  ({count} messages consumed)', file=sys.stderr)
" &
    PID=$!
fi

# Let the monitor/consumer attach before generating events.
sleep 2

log() { echo "  [$1]" >&2; }

# =====================================================================
# Workload: exercises GPFS inotify event types.
# Events flow: filesystem → mmwatch → Kafka → icicle (if --icicle).
# =====================================================================

# 1. Create a new file.
#    Expected: IN_CREATE, IN_MODIFY, IN_CLOSE_WRITE
log "Create file"
echo "hello" > "$WATCH_DIR/file.txt"
sleep 0.5

# 2. Append to existing file.
#    Expected: IN_MODIFY, IN_CLOSE_WRITE
log "Append to file"
echo "world" >> "$WATCH_DIR/file.txt"
sleep 0.5

# 3. Overwrite file contents.
#    Expected: IN_MODIFY, IN_CLOSE_WRITE
log "Overwrite file"
echo "replaced" > "$WATCH_DIR/file.txt"
sleep 0.5

# 4. Touch — updates timestamps.
#    Expected: IN_ATTRIB
log "Touch file"
touch "$WATCH_DIR/file.txt"
sleep 0.5

# 5. Permission change via chmod.
#    Expected: IN_ATTRIB
log "chmod 755"
chmod 755 "$WATCH_DIR/file.txt"
sleep 0.3

log "chmod 644"
chmod 644 "$WATCH_DIR/file.txt"
sleep 0.3

# 6. Rename a file.
#    Expected: IN_MOVED_FROM (old) + IN_MOVED_TO (new)
#    In icicle mode: IN_MOVED_FROM is dropped; IN_MOVED_TO handles
#    the rename via inode-based state lookup.
log "Rename file -> renamed.txt"
mv "$WATCH_DIR/file.txt" "$WATCH_DIR/renamed.txt"
sleep 0.5

# 7. Create a hard link.
#    Expected: IN_CREATE (link), IN_ATTRIB (target)
log "Create hard link"
ln "$WATCH_DIR/renamed.txt" "$WATCH_DIR/hardlink.txt"
sleep 0.3

# 8. Create a directory.
#    Expected: IN_CREATE IN_ISDIR
log "Create directory"
mkdir "$WATCH_DIR/subdir"
sleep 0.3

# 9. Create a file inside a subdirectory.
#    Expected: IN_CREATE, IN_MODIFY, IN_CLOSE_WRITE (file) + parent IN_MODIFY
log "Create nested file"
echo "nested" > "$WATCH_DIR/subdir/nested.txt"
sleep 0.5

# 10. Create a multi-level directory tree with files.
#     Tests recursive path tracking in the state manager.
log "Create deep tree (3 levels, 3 files)"
mkdir -p "$WATCH_DIR/deep/a/b/c"
echo "deep1" > "$WATCH_DIR/deep/a/f1.txt"
echo "deep2" > "$WATCH_DIR/deep/a/b/f2.txt"
echo "deep3" > "$WATCH_DIR/deep/a/b/c/f3.txt"
sleep 1

# 11. Rename a top-level directory.
#     Key behavior: GPFS emits IN_MOVED_FROM + IN_MOVED_TO for the directory.
#     The icicle GPFSStateManager uses inode lookup + recursive child update.
log "Rename deep/a -> deep/z"
mv "$WATCH_DIR/deep/a" "$WATCH_DIR/deep/z"
sleep 1

# 12. Move a file across directories.
#     Expected: IN_MOVED_FROM + IN_MOVED_TO
log "Move file across dirs"
mv "$WATCH_DIR/subdir/nested.txt" "$WATCH_DIR/deep/z/moved.txt"
sleep 0.5

# 13. Copy a file — creates a new file with same content.
#     Expected: IN_CREATE + IN_MODIFY + IN_CLOSE_WRITE (new file)
log "Copy file"
cp "$WATCH_DIR/renamed.txt" "$WATCH_DIR/copied.txt"
sleep 0.5

# 14. Truncate a file to zero bytes.
#     Expected: IN_MODIFY, IN_CLOSE_WRITE
log "Truncate file"
: > "$WATCH_DIR/copied.txt"
sleep 0.3

# 15. Write binary data (4KB).
log "Write 4KB binary"
dd if=/dev/urandom of="$WATCH_DIR/binary.bin" bs=1024 count=4 2>/dev/null
sleep 0.5

# 16-18. Deletions: hard link, file, recursive directory.
log "Delete hard link"
rm "$WATCH_DIR/hardlink.txt"
sleep 0.3

log "Delete file"
rm "$WATCH_DIR/renamed.txt"
sleep 0.3

log "Delete deep/ recursively"
rm -rf "$WATCH_DIR/deep"
sleep 0.5

# 19. Rapid overwrites — update coalescing test.
log "Rapid overwrite x10"
for i in $(seq 1 10); do
    echo "overwrite$i" > "$WATCH_DIR/coalesce.txt"
done
sleep 0.5

# 20. Create/delete cancellation — batch reduction test.
#     With GPFS rules: REMOVED cancels CREATED → no output.
log "Create/delete cancellation x3"
for i in $(seq 1 3); do
    echo "ephemeral$i" > "$WATCH_DIR/ephemeral_$i.txt"
    rm "$WATCH_DIR/ephemeral_$i.txt"
done
sleep 1

# 21. Final persistent file — should always appear in output.
log "Create final file"
echo "done" > "$WATCH_DIR/final.txt"
sleep 2

# =====================================================================
# Shutdown: kill the monitor, then show results.
# =====================================================================
echo "---" >&2
kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true

echo "=== done ===" >&2
