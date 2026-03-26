#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFAULT_TEMPLATE="$SCRIPT_DIR/filebench.f.template"

usage() {
  cat <<'HELP'
Usage: run-filebench.sh -p DIR [OPTIONS]

Run a filebench workload against any filesystem path.

Options:
  -p DIR         Target directory for the workload (required)
  -t TEMPLATE    Path to .f.template file (default: filebench.f.template)
  -h             Show this help message

Examples:
  # Run against a Lustre MDT
  run-filebench.sh -p /mnt/fs0/mdt0
  # Run against a GPFS fileset
  run-filebench.sh -p /ibm/fs1/fset1
  # Use a custom template
  run-filebench.sh -p /mnt/exacloud/mdt3 -t custom.f.template
HELP
  exit 0
}

DIR=""
TEMPLATE="$DEFAULT_TEMPLATE"

while getopts ":p:t:h" opt; do
  case "$opt" in
    p) DIR="$OPTARG" ;;
    t) TEMPLATE="$OPTARG" ;;
    h) usage ;;
    :) echo "Error: -$OPTARG requires an argument" >&2; exit 1 ;;
    *) echo "Error: unknown option -$OPTARG" >&2; usage ;;
  esac
done

if [[ -z "$DIR" ]]; then
  echo "Error: -p DIR is required" >&2
  usage
fi

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Error: template not found: $TEMPLATE" >&2
  exit 1
fi

tmpfile=$(mktemp /tmp/filebench_XXXXXX.f)

# Save current ASLR setting and disable it (filebench requires ASLR off)
orig_aslr=$(cat /proc/sys/kernel/randomize_va_space)
echo "Current ASLR setting: $orig_aslr"
trap 'rm -f "$tmpfile"; echo "$orig_aslr" | sudo tee /proc/sys/kernel/randomize_va_space >/dev/null' EXIT
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space >/dev/null

sed "s|__WORKLOAD_DIR__|${DIR}|" "$TEMPLATE" > "$tmpfile"

echo "Filebench workload: $TEMPLATE"
echo "Target directory:   $DIR"
echo "Generated config:   $tmpfile"
echo ""

echo "ASLR setting before filebench: $(cat /proc/sys/kernel/randomize_va_space)"
sudo filebench -f "$tmpfile"
echo "$orig_aslr" | sudo tee /proc/sys/kernel/randomize_va_space >/dev/null
echo "ASLR setting restored to: $(cat /proc/sys/kernel/randomize_va_space)"
