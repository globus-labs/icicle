#!/usr/bin/env bash
set -euo pipefail

# ——— Parameters ————————————————————————————————————————————————
COUNT="${1:-0}"         # 0 = create forever; >0 = that many
SLEEP_TIME="${2:-0.05}" # seconds between creations
OUTPUT="${3:-1}"        # 1=show progress; 0=silent

# ——— Folder setup ———————————————————————————————————————————————
folder="$(mktemp -d /mnt/exacloud/tmpXXXXXX)"
echo "Working in folder: $folder (COUNT=${COUNT:-∞}, SLEEP=$SLEEP_TIME)"

# ——— Cleanup hint on exit ————————————————————————————————————————
trap 'echo "To delete everything: rm -rf \"$folder\""' EXIT

# ——— File-creation logic ———————————————————————————————————————
create_files() {
  if (( COUNT > 0 )); then
    for (( i=1; i<=COUNT; i++ )); do
      printf "File %d\n" "$i" > "${folder}/${i}.txt"
      if (( OUTPUT )); then
        echo "Created ${folder}/${i}.txt"
      fi
      sleep "$SLEEP_TIME"
    done
  else
    i=1
    while true; do
      printf "File %d\n" "$i" > "${folder}/${i}.txt"
      if (( OUTPUT )); then
        echo "Created ${folder}/${i}.txt"
      fi
      (( i++ ))
      sleep "$SLEEP_TIME"
    done
  fi
}

main() {
  create_files
}

main
