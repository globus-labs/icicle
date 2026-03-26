#!/bin/bash

source kda-config.sh

aws kinesisanalyticsv2 create-application-presigned-url \
  --application-name "$APP_NAME" \
  --url-type FLINK_DASHBOARD_URL \
  --no-cli-pager \
  | jq -r '.AuthorizedUrl'
