#!/usr/bin/env bash

OUTPUT_FILE="$1"
SEARCH_PATH="$2"

true > "$OUTPUT_FILE"

echo "Collecting db.db file stats from: $SEARCH_PATH"
echo "Writing to: $OUTPUT_FILE"

/usr/bin/time -f 'elapsed %E' \
  fdfind --type f 'db.db' "$SEARCH_PATH" \
    --threads 16 \
    -x stat -c '%n,%s' {} >> "$OUTPUT_FILE"

line_count=$(wc -l < "$OUTPUT_FILE")

echo "Stats written to $OUTPUT_FILE"
echo "Total entries: $line_count"
