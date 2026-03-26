#!/usr/bin/env bash
# set -euo pipefail

# ——— Parameters ————————————————————————————————————————————————
COUNT="${1:-0}"           # 0 = infinite loop; >0 = fixed iterations
SLEEP_TIME="${2:-0.1}"    # seconds between each operation
OUTPUT="${3:-1}"          # 1 = log actions; 0 = silent

# ——— Workspace setup —————————————————————————————————————————————
base="$(mktemp -d /mnt/exacloud/mdt0/tmpXXXXXX)"
# base="$(mktemp -d /ibm/fs1/fset1/tmpXXXXXX)"
echo $$
cd "$base"
echo "Working in folder: $base (COUNT=${COUNT:-∞}, SLEEP=$SLEEP_TIME)"

trap 'echo "To delete everything: rm -rf \"$base\""' EXIT

# ——— Basic operations —————————————————————————————————————————————
create_file() {
  echo "hello performance" > "hello_${i}.txt"
  (( OUTPUT )) && echo "[create]   hello_${i}.txt"
}

modify_file() {
  echo "modified at $(date +%T)" >> "hello_${i}.txt"
  (( OUTPUT )) && echo "[modify]   hello_${i}.txt"
}

delete_file() {
  rm -f "hello_${i}.txt"
  (( OUTPUT )) && echo "[delete]   hello_${i}.txt"
}

delete_folder() {
  rm -rf "$base"
  (( OUTPUT )) && echo "[delete]   $base"
}

# ——— Random tiny sleep helper (0–3 ms) ————————————————————————————
jitter() {
  python3 -c "import time,random; time.sleep(random.random()/300)"
}

# ——— One iteration ——————————————————————————————————————————————
run_iteration() {
  create_file
  # sleep "$SLEEP_TIME"
  # jitter
  modify_file
  # sleep "$SLEEP_TIME"
  # jitter
  delete_file
  # sleep "$SLEEP_TIME"
  # jitter
}


# ——— Loop ————————————————————————————————————————————————
if (( COUNT > 0 )); then
  for ((i=1; i<=COUNT; i++)); do
    (( OUTPUT )) && echo "=== iteration $i ==="
    run_iteration
  done
else
  i=1
  while true; do
    (( OUTPUT )) && echo "=== iteration $i ==="
    run_iteration
    (( i++ ))
  done
fi

# delete_folder # should not delete, will mess up with fid
# sleep "$SLEEP_TIME"
