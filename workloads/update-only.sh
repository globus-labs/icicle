#!/usr/bin/env bash
set -euo pipefail

# ——— Parameters ————————————————————————————————————————————————
N="${1:-10}"         # how many files to create initially
M="${2:-0.01}"       # seconds between random ops
OUTPUT="${3:-1}"     # 1=echo each action, 0=silent

# ——— Folder setup ———————————————————————————————————————————————
folder="$(mktemp -d /mnt/exacloud/tmpXXXXXX)"
echo "Working in folder: $folder (N=$N, M=$M)"

# ——— Cleanup hint on exit ————————————————————————————————————————
trap 'echo "To delete everything: rm -rf \"$folder\""' EXIT

# ——— Create initial batch ———————————————————————————————————————
create_files() {
  for ((i=1; i<=N; i++)); do
    printf "File %d\n" "$i" > "${folder}/${i}.txt"
    if (( OUTPUT )); then
      echo "Created ${folder}/${i}.txt"
    fi
    sleep 0.05
  done
}

# ——— One random update operation ——————————————————————————————
random_operation() {
  idx=$(( RANDOM % N + 1 ))
  file="${folder}/${idx}.txt"

  case $(( RANDOM % 4 )) in
    0)  # truncate
        : > "$file"
        if (( OUTPUT )); then
          echo "Truncated $file"
        fi
        ;;
    1)  # chmod
        perms_list=(644 600 700 666)
        p=${perms_list[RANDOM % ${#perms_list[@]}]}
        chmod "$p" "$file"
        if (( OUTPUT )); then
          echo "Chmod $file → $p"
        fi
        ;;
    2)  # read (no-op)
        head -c 0 "$file" > /dev/null
        if (( OUTPUT )); then
          echo "Read $file"
        fi
        ;;
    3)  # append write
        echo "Update at $(date +%T)" >> "$file"
        if (( OUTPUT )); then
          echo "Appended to $file"
        fi
        ;;
  esac
}

# ——— Endless update loop ———————————————————————————————————————
update_loop() {
  echo "Initial creation done. Starting updates…"
  while true; do
    random_operation
    sleep "$M"
  done
}

# ——— Main —————————————————————————————————————————————————————————
main() {
  create_files
  update_loop
}

main
