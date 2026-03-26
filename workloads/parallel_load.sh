#!/usr/bin/env bash
set -euo pipefail

max_parallel=128 # 4 * max partition
total=1000
per_job_count=100

jitter() {
  python3 -c "import time, random; time.sleep(random.random() * 0.03)"
}

for ((i=1; i<=total; i++)); do
  jitter
  ./evaluate_performance.sh "$per_job_count" 0 0 > /dev/null 2>&1 &

  # Throttle after N parallel jobs
  if (( i % max_parallel == 0 )); then
    wait
    jitter
  fi

done

wait
echo "All $total runs completed."
