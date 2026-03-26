#!/usr/bin/env bash
# set -euo pipefail

# ——— Known filesystem mount prefixes ————————————————————————————————
KNOWN_MOUNTS=("/mnt/fs0" "/ibm/fs1")

# ——— Usage ————————————————————————————————————————————————————————
usage() {
  cat <<'HELP'
Usage: evaluate_performance.sh [OPTIONS]

Evaluate filesystem performance: create/modify/delete loop.

Options:
  -p PATH      Base path to work in (default: auto-create temp dir under /mnt/fs0/mdt0/)
  -c COUNT     Number of iterations; 0 = infinite (default: 0)
  -s SLEEP     Seconds between each operation (default: 0)
  -j JOBS      Parallel workers (default: 1)
  -q           Quiet mode (suppress per-operation output)
  -f           Force: skip filesystem mount-point validation
  -h           Show this help message

Examples:
  # 1 iteration with 0.1s sleep between steps
  evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 1 -s 0.1
  # 100 iterations across 4 parallel workers, quiet mode
  evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 100 -j 4 -q
HELP
  exit 0
}

# ——— Defaults —————————————————————————————————————————————————————
BASE_PATH=""
COUNT=0
SLEEP_TIME=0
OUTPUT=1
JOBS=1
FORCE=0

# ——— Parse flags ——————————————————————————————————————————————————
while getopts ":p:c:s:j:qfh" opt; do
  case "$opt" in
    p) BASE_PATH="$OPTARG" ;;
    c) COUNT="$OPTARG" ;;
    s) SLEEP_TIME="$OPTARG" ;;
    j) JOBS="$OPTARG" ;;
    q) OUTPUT=0 ;;
    f) FORCE=1 ;;
    h) usage ;;
    :) echo "Error: -$OPTARG requires an argument" >&2; exit 1 ;;
    *) echo "Error: unknown option -$OPTARG" >&2; usage ;;
  esac
done

# ——— Validate mount point ————————————————————————————————————————
validate_mount() {
  local path="$1"
  if (( FORCE )); then return 0; fi
  for prefix in "${KNOWN_MOUNTS[@]}"; do
    if [[ "$path" == "$prefix"* ]]; then return 0; fi
  done
  echo "Error: '$path' is not under a known filesystem mount (${KNOWN_MOUNTS[*]})." >&2
  echo "       Use -f to override this check." >&2
  exit 1
}

# ——— Workspace setup —————————————————————————————————————————————
if [[ -n "$BASE_PATH" ]]; then
  validate_mount "$BASE_PATH"
  mkdir -p "$BASE_PATH"
  base="$BASE_PATH"
else
  base="$(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX)"
fi

echo "Working in folder: $base (COUNT=${COUNT:-∞}, SLEEP=$SLEEP_TIME, JOBS=$JOBS)"

# ——— Timing & signal handling ————————————————————————————————————
COMPLETED=0
START_TIME=$(date +%s%N)

print_summary() {
  local end_time elapsed_ns elapsed_s rate total_completed
  end_time=$(date +%s%N)
  elapsed_ns=$(( end_time - START_TIME ))
  elapsed_s=$(awk "BEGIN {printf \"%.2f\", $elapsed_ns / 1000000000}")

  # In parallel mode, sum counts from worker files
  if (( JOBS > 1 )); then
    total_completed=0
    for f in "$base"/.worker_count_*; do
      [[ -f "$f" ]] && total_completed=$(( total_completed + $(cat "$f") ))
    done
  else
    total_completed=$COMPLETED
  fi

  if (( total_completed > 0 )) && awk "BEGIN {exit ($elapsed_ns <= 0)}" 2>/dev/null; then
    rate=$(awk "BEGIN {printf \"%.1f\", $total_completed / ($elapsed_ns / 1000000000)}")
  else
    rate="0.0"
  fi
  echo ""
  echo "——— Summary ———————————————————————————————————————————"
  echo "  Iterations completed: $total_completed"
  echo "  Elapsed time:         ${elapsed_s}s"
  echo "  Throughput:           ${rate} iter/s"
  echo "  Operations:           $(( total_completed * 3 )) total (3 ops/iter)"
  echo "  Working directory:    $base"
  echo "  To delete everything: rm -rf \"$base\""
  echo "———————————————————————————————————————————————————————"
}

cleanup() {
  print_summary
  # Kill child workers if any
  if (( JOBS > 1 )); then
    kill "${WORKER_PIDS[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
  fi
  exit 0
}

trap cleanup INT TERM
trap print_summary EXIT

# ——— Basic operations —————————————————————————————————————————————
create_file() {
  echo "hello performance" > "hello_${1}.txt"
  (( OUTPUT )) && echo "[create]   hello_${1}.txt"
}

modify_file() {
  echo "modified at $(date +%T)" >> "hello_${1}.txt"
  (( OUTPUT )) && echo "[modify]   hello_${1}.txt"
}

delete_file() {
  rm -f "hello_${1}.txt"
  (( OUTPUT )) && echo "[delete]   hello_${1}.txt"
}

delete_folder() {
  rm -rf "$base"
  (( OUTPUT )) && echo "[delete]   $base"
}

# ——— Random tiny sleep helper (0-3 ms) ————————————————————————————
jitter() {
  python3 -c "import time,random; time.sleep(random.random()/300)"
}

# ——— One iteration ——————————————————————————————————————————————
run_iteration() {
  local id="$1"
  create_file "$id"
  sleep "$SLEEP_TIME"
  modify_file "$id"
  sleep "$SLEEP_TIME"
  delete_file "$id"
  sleep "$SLEEP_TIME"
}

# ——— Worker function (for parallel mode) —————————————————————————
run_worker() {
  local worker_id="$1"
  local worker_dir="$base/worker_${worker_id}"
  local count_file="$base/.worker_count_${worker_id}"
  local completed=0
  mkdir -p "$worker_dir"
  cd "$worker_dir" || exit

  if (( COUNT > 0 )); then
    for ((i=1; i<=COUNT; i++)); do
      (( OUTPUT )) && echo "[worker $worker_id] === iteration $i ==="
      run_iteration "${worker_id}_${i}"
      completed=$(( completed + 1 ))
      echo "$completed" > "$count_file"
    done
  else
    local i=1
    while true; do
      (( OUTPUT )) && echo "[worker $worker_id] === iteration $i ==="
      run_iteration "${worker_id}_${i}"
      completed=$(( completed + 1 ))
      echo "$completed" > "$count_file"
      i=$(( i + 1 ))
    done
  fi
}

# ——— Main ————————————————————————————————————————————————————————
if (( JOBS > 1 )); then
  WORKER_PIDS=()
  for ((w=1; w<=JOBS; w++)); do
    run_worker "$w" &
    WORKER_PIDS+=($!)
  done
  wait "${WORKER_PIDS[@]}" 2>/dev/null || true
else
  cd "$base" || exit
  if (( COUNT > 0 )); then
    for ((i=1; i<=COUNT; i++)); do
      (( OUTPUT )) && echo "=== iteration $i ==="
      run_iteration "$i"
      COMPLETED=$(( COMPLETED + 1 ))
    done
  else
    i=1
    while true; do
      (( OUTPUT )) && echo "=== iteration $i ==="
      run_iteration "$i"
      COMPLETED=$(( COMPLETED + 1 ))
      i=$(( i + 1 ))
    done
  fi
fi
