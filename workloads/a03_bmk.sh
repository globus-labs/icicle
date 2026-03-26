#!/bin/bash
set -euo pipefail

REPEAT=1   # number of repetitions for each experiment

# ** EXPERIMENT 1: changelog_mode=true, icicle **
for i in $(seq 1 $REPEAT); do
  echo "** Exp1 Run $i **"
  PYTHONPATH=. python3 -m monitor.main \
    --fs_type lfs \
    --changelog_mode true \
    --output_destination kafka \
    --output_handle lustre-mon-out \
    --lustre_cid cl2 \
    --lustre_fid_resolution_method icicle
  sleep 1
done

# ** EXPERIMENT 2: changelog_mode=false, fsmonitor **
for i in $(seq 1 $REPEAT); do
  echo "** Exp2 Run $i **"
  PYTHONPATH=. python3 -m monitor.main \
    --fs_type lfs \
    --changelog_mode false \
    --output_destination kafka \
    --output_handle lustre-mon-out \
    --lustre_cid cl2 \
    --lustre_fid_resolution_method fsmonitor
  sleep 1
done

# ** EXPERIMENT 3: changelog_mode=false, icicle **
for i in $(seq 1 $REPEAT); do
  echo "** Exp3 Run $i **"
  PYTHONPATH=. python3 -m monitor.main \
    --fs_type lfs \
    --changelog_mode false \
    --output_destination kafka \
    --output_handle lustre-mon-out \
    --lustre_cid cl2 \
    --lustre_fid_resolution_method icicle
  sleep 1
done

# ** EXPERIMENT 4: icicle + ignore 10OPEN + reduction enabled **
for i in $(seq 1 $REPEAT); do
  echo "** Exp4 Run $i **"
  PYTHONPATH=. python3 -m monitor.main \
    --fs_type lfs \
    --changelog_mode false \
    --output_destination kafka \
    --output_handle lustre-mon-out \
    --lustre_cid cl2 \
    --lustre_fid_resolution_method icicle \
    --lustre_ignore_events 10OPEN \
    --enable_reduction_rules true
  sleep 1
done

echo "** ALL EXPERIMENTS COMPLETED **"
