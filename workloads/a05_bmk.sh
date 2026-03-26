#!/bin/bash
set -euo pipefail

REPEAT=1  # number of repetitions for each experiment

KEY="../lustre-alpha-1-key.pem"
HOST1="54.145.217.223"    # exacloud-MDT0000 / cl2
HOST2="100.28.109.235"    # exacloud-MDT0001 / cl3

run_pair() {
  local exp_name="$1"
  local changelog_mode="$2"
  local fid_method="$3"
  shift 3
  local extra_args=("$@")

  for i in $(seq 1 "$REPEAT"); do
    echo "** $exp_name Run $i **"

    # Host 1
    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST1" "
      echo \"[HOST1 $HOST1] starting $exp_name run $i\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type lfs \
        --changelog_mode $changelog_mode \
        --output_destination kafka \
        --output_handle lustre-mon-out \
        --lustre_mdt exacloud-MDT0000 \
        --lustre_cid cl2 \
        --lustre_fid_resolution_method $fid_method \
        ${extra_args[*]}
    " &
    PID1=$!

    # Host 2
    ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST2" "
      echo \"[HOST2 $HOST2] starting $exp_name run $i\" >&2
      cd /home/ubuntu/icicle &&
      export PYTHONPATH=. &&
      /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
        --fs_type lfs \
        --changelog_mode $changelog_mode \
        --output_destination kafka \
        --output_handle lustre-mon-out \
        --lustre_mdt exacloud-MDT0001 \
        --lustre_cid cl3 \
        --lustre_fid_resolution_method $fid_method \
        ${extra_args[*]}
    " &
    PID2=$!

    wait "$PID1"
    wait "$PID2"
    sleep 1
  done
}

#######################################
# EXPERIMENT 1: changelog_mode=true, icicle
#######################################
run_pair "Exp1 (true, icicle)" true icicle

########################################
# EXPERIMENT 2: changelog_mode=false, fsmonitor
########################################
run_pair "Exp2 (false, fsmonitor)" false fsmonitor

########################################
# EXPERIMENT 3: changelog_mode=false, icicle
########################################
run_pair "Exp3 (false, icicle)" false icicle

########################################
# EXPERIMENT 4: icicle + ignore 10OPEN + reduction enabled
########################################
run_pair "Exp4 (false, icicle+ignore+reduce)" false icicle \
  --lustre_ignore_events 10OPEN \
  --enable_reduction_rules true

echo "** ALL EXPERIMENTS COMPLETED **"
