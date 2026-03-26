#!/bin/bash
set -euo pipefail

REPEAT="${REPEAT:-1}"                 # repetitions per (experiment, partitions)
SLEEP_SECS="${SLEEP_SECS:-1}"         # pause between runs
PARTITIONS_LIST=(${PARTITIONS_LIST:-1 2})
# PARTITIONS_LIST=(${PARTITIONS_LIST:-16})

OUTPUT_DEST="${OUTPUT_DEST:-kafka}"
OUTPUT_HANDLE="${OUTPUT_HANDLE:-gpfs-mon-out}"
TOPIC_PREFIX="${TOPIC_PREFIX:-fset1-topic}"

run_monitor() {
  local exp_label="$1"
  local changelog_mode="$2"
  local partitions="$3"
  shift 3
  local extra_args=("$@")

  local topic="${TOPIC_PREFIX}-${partitions}p"

  for i in $(seq 1 "$REPEAT"); do
    echo "** ${exp_label} | p=${partitions} | Run ${i}/${REPEAT} **"
    PYTHONPATH=. python3 -m monitor.main \
      --fs_type ibm \
      --changelog_mode "${changelog_mode}" \
      --output_destination "${OUTPUT_DEST}" \
      --output_handle "${OUTPUT_HANDLE}" \
      --gpfs_topic "${topic}" \
      --gpfs_partition "${partitions}" \
      "${extra_args[@]}"
    sleep "${SLEEP_SECS}"
  done
}

for p in "${PARTITIONS_LIST[@]}"; do
  # EXP 1: changelog_mode=true
  run_monitor "Exp1 (changelog_mode=true)"  true  "$p"

  # EXP 3: changelog_mode=false (no reduction)
  run_monitor "Exp3 (changelog_mode=false)" false "$p"

  # EXP 4: changelog_mode=false + ignore open/close + reduction rules
  run_monitor "Exp4 (ignore open/close + reduction)" false "$p" \
    --gpfs_ignore_events IN_OPEN,IN_CLOSE_NOWRITE \
    --enable_reduction_rules true
done

echo "** ALL EXPERIMENTS COMPLETED **"
