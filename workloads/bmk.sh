#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")"

PY=python3  # or your venv python
echo "Using Python: $(which "$PY")"

for r in $(seq 1 10); do
    echo "===== Repetition $r ====="
    for p in 16p 12p 9p 8p 6p 4p 2p 1p; do
        ts="$(date +%s)"
        topic="fset1-topic-$p"
        log="2xlarge-$p-r${r}-${ts}.log"

        echo "[$(date)] Starting run $r for $topic -> $log"
        "$PY" -u icicle2/main.py "$topic" >"$log" 2>&1
        echo "[$(date)] Finished run $r for $topic"

        sleep 3
    done
done

echo "All benchmarks completed."
