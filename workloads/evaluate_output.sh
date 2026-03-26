#!/usr/bin/env bash
set -euo pipefail

# ——— Parameters ————————————————————————————————————————————————
COUNT="${1:-0}"         # 0 = infinite; >0 = that many
SLEEP_TIME="${2:-0.05}" # seconds between each step
OUTPUT="${3:-1}"        # 1 = show actions; 0 = silent

# ——— Workspace setup —————————————————————————————————————————————
base="$(mktemp -d /mnt/exacloud/mdt0/tmpXXXXXX)"
# base="$(mktemp -d /ibm/fs1/fset1/tmpXXXXXX)"

cd "$base"
echo "Working in folder: $base (COUNT=${COUNT:-∞}, SLEEP=$SLEEP_TIME)"

# ——— Cleanup hint on exit ————————————————————————————————————————
echo "Evaluate Output: create/modify/rename/mkdir/move/delete loop"
trap 'echo "To delete everything: rm -rf \"$base\""' EXIT

# ——— Single‐responsibility operations —————————————————————————————————
create_file() {
  echo "hello world" > "hello_${i}.txt"
  if (( OUTPUT )); then
    echo "[create]   hello_${i}.txt"
  fi
}

modify_file() {
  echo "modified at $(date +%T)" >> "hello_${i}.txt"
  if (( OUTPUT )); then
    echo "[modify]   hello_${i}.txt"
  fi
}

rename_file() {
  mv "hello_${i}.txt" "hi_${i}.txt"
  if (( OUTPUT )); then
    echo "[rename]   hello_${i}.txt → hi_${i}.txt"
  fi
}

make_dir() {
  mkdir "okdir_${i}"
  if (( OUTPUT )); then
    echo "[mkdir]    okdir_${i}"
  fi
}

move_file() {
  mv "hi_${i}.txt" "okdir_${i}/"
  if (( OUTPUT )); then
    echo "[move]     hi_${i}.txt → okdir_${i}/"
  fi
}

delete_dir() {
  rm -rf "okdir_${i}"
  if (( OUTPUT )); then
    echo "[delete]   okdir_${i}"
  fi
}

# ——— One iteration of the evaluate sequence —————————————————————————————
run_iteration() {
  create_file
  sleep "$SLEEP_TIME"
  modify_file
  sleep "$SLEEP_TIME"
  rename_file
  sleep "$SLEEP_TIME"
  make_dir
  sleep "$SLEEP_TIME"
  move_file
  sleep "$SLEEP_TIME"
  delete_dir
  sleep "$SLEEP_TIME"
}

# ——— Loop through COUNT or forever ——————————————————————————————————
if (( COUNT > 0 )); then
  for ((i=1; i<=COUNT; i++)); do
    if (( OUTPUT )); then
      echo "=== iteration $i ==="
    fi
    run_iteration
  done

  # ——— After finishing all iterations ——————————————————————————————
  # sleep 0.5
  # echo
  # echo "Final contents of $base:"
  # ls -l "$base"

else
  i=1
  while true; do
    if (( OUTPUT )); then
      echo "=== iteration $i ==="
    fi
    run_iteration
    (( i++ ))
  done
fi
