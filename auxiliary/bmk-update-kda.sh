#!/bin/bash

# Usage: ./bmk-update-kda.sh <env> <kpu>
# Example: ./bmk-update-kda.sh test 64
# Result: APP_NAME=initial-ingest-test, KPU=64

if [ $# -lt 2 ]; then
    echo "Usage: $0 <env> <kpu>"
    exit 1
fi

ENV="$1"
KPU="$2"

CONFIG_FILE="kda-config.sh"

# Sanity check
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

# In-place replace
sed -i \
    -e "s/^APP_NAME=.*/APP_NAME=initial-ingest-$ENV/" \
    -e "s/^KPU=.*/KPU=$KPU/" \
    "$CONFIG_FILE"

echo "  APP_NAME=initial-ingest-$ENV"
echo "  KPU=$KPU"
