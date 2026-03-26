#!/bin/bash

source kda-config.sh

aws kinesisanalyticsv2 describe-application \
    --application-name $APP_NAME \
    --output json \
    | jq -r '.ApplicationDetail.ApplicationStatus'
