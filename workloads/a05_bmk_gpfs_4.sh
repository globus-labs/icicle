#!/bin/bash
set -euo pipefail

REPEAT="${REPEAT:-1}"                 # repetitions per (experiment, partitions)
SLEEP_SECS="${SLEEP_SECS:-1}"         # pause between runs
PARTITIONS_LIST=(${PARTITIONS_LIST:-1 2})

OUTPUT_DEST="${OUTPUT_DEST:-kafka}"
OUTPUT_HANDLE="${OUTPUT_HANDLE:-gpfs-mon-out}"

KEY="../lustre-alpha-1-key.pem"

# -------- Clients (edit IPs) --------
HOST1="54.145.217.223"      # client 1
HOST2="100.28.109.235"      # client 2
HOST3="34.229.238.205"      # client 3
HOST4="54.81.131.21"        # client 4

TOPIC_PREFIX_HOST1="fset1-topic"
TOPIC_PREFIX_HOST2="fset2-topic"
TOPIC_PREFIX_HOST3="fset3-topic"
TOPIC_PREFIX_HOST4="fset4-topic"

run_quad() {
  local exp_label="$1"
  local changelog_mode="$2"
  local partitions="$3"
  shift 3
  local extra_args=("$@")

  local topic1="${TOPIC_PREFIX_HOST1}-${partitions}p"
  local topic2="${TOPIC_PREFIX_HOST2}-${partitions}p"
  local topic3="${TOPIC_PREFIX_HOST3}-${partitions}p"
  local topic4="${TOPIC_PREFIX_HOST4}-${partitions}p"

  for i in $(seq 1 "$REPEAT"); do
    echo "** ${exp_label} | p=${partitions} | Run ${i}/${REPEAT} **"

    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST1" "
      echo \"[HOST1] ${exp_label} topic=${topic1} run=${i}\" >&2
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

    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST2" "
      echo \"[HOST2] ${exp_label} topic=${topic2} run=${i}\" >&2
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

    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST3" "
      echo \"[HOST3] ${exp_label} topic=${topic3} run=${i}\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type ibm \
        --changelog_mode ${changelog_mode} \
        --output_destination ${OUTPUT_DEST} \
        --output_handle ${OUTPUT_HANDLE} \
        --gpfs_topic ${topic3} \
        --gpfs_partition ${partitions} \
        ${extra_args[*]}
    " &
    PID3=$!

    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST4" "
      echo \"[HOST4] ${exp_label} topic=${topic4} run=${i}\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type ibm \
        --changelog_mode ${changelog_mode} \
        --output_destination ${OUTPUT_DEST} \
        --output_handle ${OUTPUT_HANDLE} \
        --gpfs_topic ${topic4} \
        --gpfs_partition ${partitions} \
        ${extra_args[*]}
    " &
    PID4=$!

    wait "$PID1"
    wait "$PID2"
    wait "$PID3"
    wait "$PID4"
    sleep "$SLEEP_SECS"
  done
}

for p in "${PARTITIONS_LIST[@]}"; do
  run_quad "Exp1 (changelog_mode=true)" true  "$p"
  run_quad "Exp3 (changelog_mode=false)" false "$p"
  run_quad "Exp4 (ignore open/close + reduction)" false "$p" \
    --gpfs_ignore_events IN_OPEN,IN_CLOSE_NOWRITE \
    --enable_reduction_rules true
done

echo "** ALL EXPERIMENTS COMPLETED **"
