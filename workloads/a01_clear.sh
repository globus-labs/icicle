#!/bin/bash

# Clear changelog users for Lustre MDT
#
# Usage:
#   ./a01_clear.sh <endrec> [mdt] [num_clients]
#
# Examples:
#   ./a01_clear.sh 20000000                       # uses MDT=exacloud-MDT0000, clients 1..20
#   ./a01_clear.sh 20000000 exacloud-MDT0001      # use MDT1, clients 1..20
#   ./a01_clear.sh 20000000 exacloud-MDT0001 5    # clear cl1..cl5
#
# This will run:
#   lfs changelog_clear <MDT> cl1 <endrec>
#   lfs changelog_clear <MDT> cl2 <endrec>
#   ...

DEFAULT_MDT="exacloud-MDT0000"
DEFAULT_CLIENTS=20

if [ $# -lt 1 ]; then
    echo "Usage: $0 <endrec> [mdt] [num_clients]"
    exit 1
fi

ENDREC=$1
MDT=${2:-$DEFAULT_MDT}
NUM_CLIENTS=${3:-$DEFAULT_CLIENTS}

echo ">>> Clearing changelog on $MDT for clients cl1..cl$NUM_CLIENTS up to rec $ENDREC"
echo

for i in $(seq 1 "$NUM_CLIENTS"); do
    USER="cl$i"
    echo "Clearing changelog for $USER..."
    lfs changelog_clear "$MDT" "$USER" "$ENDREC"
done

echo ">>> Done."
echo "Check with:"
echo "  lctl get_param mdd.$MDT.changelog_users"
