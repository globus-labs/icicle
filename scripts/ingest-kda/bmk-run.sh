#!/usr/bin/env bash
set -euo pipefail

wait_for_kda_ready() {
  while true; do
    status=$(./kda-describe.sh)
    echo "$(date '+%Y-%m-%d %H:%M:%S') status: $status"
    [[ "$status" == "READY" ]] && break
    sleep 20
  done
}

run_kda_job() {
  # run_kda_job "<dataset_args>" "<pipeline_name>" ["<extra_bmk_args>"] ["<kpu>"]
  local dataset_args="$1"
  local pipeline_name="$2"
  local extra_bmk_args="${3:-}"
  # local kpu="${4:-256}"
  local kpu="${4:-128}"

  ./bmk-update-json.sh ${dataset_args} --pipeline_name="${pipeline_name}" ${extra_bmk_args}

  ./bmk-update-kda.sh post "${kpu}"
  ./kda-update.sh
  wait_for_kda_ready
  ./kda-start.sh
  sleep 3
  wait_for_kda_ready
}

# ----------------------
# Dataset argument blocks
# ----------------------
itap_args=' --s3_input=s3a://icicle-fs-small/fs-small-1m --agg_file=itap.agg2.csv --cmode=dd --run=1 '

nersc_args=' --s3_input=s3a://icicle-fs-medium/fs-medium-1m --agg_file=nersc.agg2.csv --prefix_min=0 --prefix_max=5 --cmode=dd --run=1 '

hpss_args=' --s3_input=s3a://icicle-fs-large/fs-large-1m --agg_file=hpss.agg2.csv --cmode=dd --run=1 '

# ======================
# ITAP
# ======================
run_kda_job "${itap_args}" "PRI"
run_kda_job "${itap_args}" "CNT" "--execution_mode=batch"
run_kda_job "${itap_args}" "SND" '--pipeline_args=usr|grp|dir'

run_kda_job "${itap_args}" "PRI"
run_kda_job "${itap_args}" "CNT" "--execution_mode=batch"
run_kda_job "${itap_args}" "SND" '--pipeline_args=usr|grp|dir'

# # ======================
# # NERSC
# # ======================
# run_kda_job "${nersc_args}" "SND" '--pipeline_args=usr|grp'
# run_kda_job "${nersc_args}" "SND" '--pipeline_args=dir'
# run_kda_job "${nersc_args}" "CNT" "--execution_mode=batch"
# run_kda_job "${nersc_args}" "PRI"

# # ======================
# # HPSS (6 commands, explicit)
# # ======================
# run_kda_job "${hpss_args}" "SND" '--pipeline_args=usr|grp'

# run_kda_job "${hpss_args}" "SND" '--pipeline_args=dir --prefix_min=1 --prefix_max=3'

# run_kda_job "${hpss_args}" "SND" '--pipeline_args=dir --prefix_min=3 --prefix_max=5'

# run_kda_job "${hpss_args}" "CNT" '--pipeline_args=usr|grp --execution_mode=batch'

# run_kda_job "${hpss_args}" "CNT" '--pipeline_args=dir --execution_mode=batch'

# run_kda_job "${hpss_args}" "PRI"
