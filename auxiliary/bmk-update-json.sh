#!/bin/bash

FILE="../initial_ingest/application_properties.json"

# --- Required fields with no defaults ---
S3_INPUT=""
AGG_FILE=""
PIPELINE_NAME=""

# --- Optional defaults ---
EXECUTION_MODE="streaming"
CMODE="dd"
RUN="1"
PIPELINE_ARGS="usr|grp|dir"
PREFIX_MIN="1"
PREFIX_MAX="5"
MOCK_INGEST="true"

# --- Parse keyword arguments ---
for arg in "$@"; do
    case $arg in
        --s3_input=*)
            S3_INPUT="${arg#*=}"
            shift
            ;;
        --agg_file=*)
            AGG_FILE="${arg#*=}"
            shift
            ;;
        --execution_mode=*)
            EXECUTION_MODE="${arg#*=}"
            shift
            ;;
        --cmode=*)
            CMODE="${arg#*=}"
            shift
            ;;
        --run=*)
            RUN="${arg#*=}"
            shift
            ;;
        --pipeline_name=*)
            PIPELINE_NAME="${arg#*=}"
            shift
            ;;
        --pipeline_args=*)
            PIPELINE_ARGS="${arg#*=}"
            shift
            ;;
        --prefix_min=*)
            PREFIX_MIN="${arg#*=}"
            shift
            ;;
        --prefix_max=*)
            PREFIX_MAX="${arg#*=}"
            shift
            ;;
        --mock_ingest=*)
            MOCK_INGEST="${arg#*=}"
            shift
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

# --- Validate required variables ---
if [[ -z "$S3_INPUT" || -z "$AGG_FILE" || -z "$PIPELINE_NAME" ]]; then
    echo "ERROR: Missing required keyword arguments."
    echo ""
    echo "Usage:"
    echo "  $0 --s3_input=PATH --agg_file=FILE --pipeline_name=NAME [options]"
    echo ""
    echo "Optional overrides (defaults shown):"
    echo "  --execution_mode=$EXECUTION_MODE"
    echo "  --cmode=$CMODE"
    echo "  --run=$RUN"
    echo "  --pipeline_args=$PIPELINE_ARGS"
    echo "  --prefix_min=$PREFIX_MIN"
    echo "  --prefix_max=$PREFIX_MAX"
    echo "  --mock_ingest=$MOCK_INGEST"
    exit 1
fi

# --- Perform in-place jq update ---
jq \
  --arg s3     "$S3_INPUT" \
  --arg agg    "$AGG_FILE" \
  --arg mode   "$EXECUTION_MODE" \
  --arg cmode  "$CMODE" \
  --arg run    "$RUN" \
  --arg pname  "$PIPELINE_NAME" \
  --arg pargs  "$PIPELINE_ARGS" \
  --arg mi     "$MOCK_INGEST" \
  --arg pmin   "$PREFIX_MIN" \
  --arg pmax   "$PREFIX_MAX" \
'
(.[] | select(.PropertyGroupId=="IngestConfig") | .PropertyMap)
  |= (
        .s3_input       = $s3
      | .agg_file       = $agg
      | .execution_mode = $mode
      | .cmode          = $cmode
      | .run            = $run
      | .pipeline_name  = $pname
      | .pipeline_args  = $pargs
      | .mock_ingest    = $mi
      | .prefix_min     = $pmin
      | .prefix_max     = $pmax
     )
' "$FILE" > "$FILE.tmp" && mv "$FILE.tmp" "$FILE"

jq '.[] | select(.PropertyGroupId=="IngestConfig")' "$FILE"
