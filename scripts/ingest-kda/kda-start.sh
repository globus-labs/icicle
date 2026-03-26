#!/bin/bash

source kda-config.sh

aws kinesisanalyticsv2 start-application \
    --application-name $APP_NAME \
    --output json \
    | jq
