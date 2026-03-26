#!/bin/bash
set -euo pipefail

KEY="../lustre-alpha-1-key.pem"
HOST1="54.145.217.223"    # exacloud-MDT0000 / cl2

echo "** Test Run: changelog_mode=false, resolution=icicle, no reduction **"

ssh -o StrictHostKeyChecking=no -i "$KEY" ubuntu@"$HOST1" "
  echo \"[HOST1 $HOST1] starting test run\" >&2
  cd /home/ubuntu/icicle &&
  export PYTHONPATH=. &&
  /home/ubuntu/icicle/.venv/bin/python -m monitor.main \
    --fs_type lfs \
    --changelog_mode false \
    --output_destination kafka \
    --output_handle lustre-mon-out \
    --lustre_mdt exacloud-MDT0000 \
    --lustre_cid cl2 \
    --lustre_fid_resolution_method icicle
"

echo "** Test Completed **"
