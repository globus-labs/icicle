#!/usr/bin/env bash
set -euo pipefail

# Source files that get copied into my_deps for packaging
SRC_FILES=(
    helpers.py
    csv_schema.py
    constants.py
    pipeline.py
    batch.py
    sketches.py
    aggregation.py
    prefix.py
    calc.py
    conf.py
    search.py
)

# Preflight: check that all source files exist
missing=()
for f in "${SRC_FILES[@]}"; do
    [[ -f "$f" ]] || missing+=("$f")
done
if (( ${#missing[@]} )); then
    echo "ERROR: missing source files: ${missing[*]}" >&2
    echo "  If search.py is missing, create it with SEARCH_CLIENT_ID and SEARCH_CLIENT_SECRET." >&2
    echo "  See docs/ingest/03-flink-setup.md for details." >&2
    exit 1
fi

# Clean
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete 2>/dev/null || true
rm -rf target

# Copy source files into my_deps
cp "${SRC_FILES[@]}" my_deps/

# Note: when files are unzipped, they are NOT under the my_deps/ folder
# so that we can "import ddsketch" in the code
cd my_deps
zip -r ../my_deps.zip ./*
cd ..

# Note: when files are unzipped, they are under the utils/ folder
# so that we can "from utils.kafkaconnector import ..." in the code
zip -r utils.zip utils

# Note: when files are unzipped, they are NOT under the resources/ folder
cd resources
zip -r ../resources.zip ./*
cd ..

mvn clean package
aws s3 cp target/ingest-1.0.0.zip s3://search-alpha-bucket-1

# Cleanup
for f in "${SRC_FILES[@]}"; do rm -f "my_deps/$f"; done
rm -f my_deps.zip utils.zip resources.zip dependency-reduced-pom.xml
