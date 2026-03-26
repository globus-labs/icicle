#!/bin/bash
set -euo pipefail

REPEAT="${REPEAT:-1}"                 # repetitions per (experiment, partitions)
SLEEP_SECS="${SLEEP_SECS:-1}"         # pause between runs
PARTITIONS_LIST=(${PARTITIONS_LIST:-1 2})

OUTPUT_DEST="${OUTPUT_DEST:-kafka}"
OUTPUT_HANDLE="${OUTPUT_HANDLE:-gpfs-mon-out}"

KEY="../lustre-alpha-1-key.pem"
HOST1="54.145.217.223"    # client 1
HOST2="100.28.109.235"    # client 2

TOPIC_PREFIX_HOST1="${TOPIC_PREFIX_HOST1:-fset1-topic}"
TOPIC_PREFIX_HOST2="${TOPIC_PREFIX_HOST2:-fset2-topic}"

run_pair() {
  local exp_label="$1"
  local changelog_mode="$2"
  local partitions="$3"
  shift 3
  local extra_args=("$@")

  local topic1="${TOPIC_PREFIX_HOST1}-${partitions}p"
  local topic2="${TOPIC_PREFIX_HOST2}-${partitions}p"

  for i in $(seq 1 "$REPEAT"); do
    echo "** ${exp_label} | p=${partitions} | Run ${i}/${REPEAT} **"

    # Host 1 -> fset1-topic-{1p,2p}
    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST1" "
      echo \"[HOST1 $HOST1] starting ${exp_label} topic=${topic1} p=${partitions} run ${i}/${REPEAT}\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type ibm \
        --changelog_mode ${changelog_mode} \
        --output_destination ${OUTPUT_DEST} \
        --output_handle ${OUTPUT_HANDLE} \
        --gpfs_topic ${topic1} \
        --gpfs_partition ${partitions} \
        ${extra_args[*]}
    " &
    PID1=$!

    # Host 2 -> fset2-topic-{1p,2p}
    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST2" "
      echo \"[HOST2 $HOST2] starting ${exp_label} topic=${topic2} p=${partitions} run ${i}/${REPEAT}\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type ibm \
        --changelog_mode ${changelog_mode} \
        --output_destination ${OUTPUT_DEST} \
        --output_handle ${OUTPUT_HANDLE} \
        --gpfs_topic ${topic2} \
        --gpfs_partition ${partitions} \
        ${extra_args[*]}
    " &
    PID2=$!

    wait "$PID1"
    wait "$PID2"
    sleep "$SLEEP_SECS"
  done
}

for p in "${PARTITIONS_LIST[@]}"; do
  # EXP 1: changelog_mode=true
  run_pair "Exp1 (changelog_mode=true)" true  "$p"

  # EXP 3: changelog_mode=false (no reduction)
  run_pair "Exp3 (changelog_mode=false)" false "$p"

  # EXP 4: changelog_mode=false + ignore open/close + reduction rules
  run_pair "Exp4 (ignore open/close + reduction)" false "$p" \
    --gpfs_ignore_events IN_OPEN,IN_CLOSE_NOWRITE \
    --enable_reduction_rules true
done

echo "** ALL EXPERIMENTS COMPLETED **"
