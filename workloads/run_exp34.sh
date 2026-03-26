#!/bin/bash
set -euo pipefail

REPEAT="${REPEAT:-1}"
SLEEP_SECS="${SLEEP_SECS:-1}"
PARTITIONS_LIST=(${PARTITIONS_LIST:-1 2})

OUTPUT_DEST="${OUTPUT_DEST:-kafka}"
OUTPUT_HANDLE="${OUTPUT_HANDLE:-gpfs-mon-out}"
TOPIC_PREFIX="${TOPIC_PREFIX:-fset1-topic}"
GPFS_IDLE_GRACE_SECONDS="${GPFS_IDLE_GRACE_SECONDS:-0}"

run_monitor() {
  local exp_label="$1"
  local changelog_mode="$2"
  local partitions="$3"
  shift 3
  local extra_args=("$@")

  local topic="${TOPIC_PREFIX}-${partitions}p"

  for i in $(seq 1 "$REPEAT"); do
    echo "** ${exp_label} | p=${partitions} | Run ${i}/${REPEAT} **"
    PYTHONPATH=. .venv/bin/python -m monitor.main \
      --fs_type ibm \
      --changelog_mode "${changelog_mode}" \
      --output_destination "${OUTPUT_DEST}" \
      --output_handle "${OUTPUT_HANDLE}" \
      --gpfs_topic "${topic}" \
      --gpfs_partition "${partitions}" \
      --gpfs_idle_grace_seconds "${GPFS_IDLE_GRACE_SECONDS}" \
      "${extra_args[@]}"
    sleep "${SLEEP_SECS}"
  done
}

for p in "${PARTITIONS_LIST[@]}"; do
  # EXP 3: changelog_mode=false (no reduction)
  run_monitor "Exp3 (changelog_mode=false)" false "$p"

  # EXP 4: changelog_mode=false + ignore open/close + reduction rules
  run_monitor "Exp4 (ignore open/close + reduction)" false "$p" \
    --gpfs_ignore_events IN_OPEN,IN_CLOSE_NOWRITE \
    --enable_reduction_rules true
done

echo "** ALL EXPERIMENTS COMPLETED **"
