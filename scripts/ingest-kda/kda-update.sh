#!/bin/bash

source kda-config.sh


# Get conditional token
TOKEN=$(aws kinesisanalyticsv2 describe-application \
  --application-name "$APP_NAME" \
  --query 'ApplicationDetail.ConditionalToken' \
  --output text --no-cli-pager)

# Generate and send update in one go
result=$(jq -n \
  --arg bucketArn "$S3_BUCKET_ARN" \
  --arg fileKey "$S3_FILE_KEY" \
  --arg kpu "$KPU" \
  --slurpfile env "$ENV_FILE" \
  '{
    ApplicationCodeConfigurationUpdate: {
      CodeContentTypeUpdate: "ZIPFILE",
      CodeContentUpdate: {
        S3ContentLocationUpdate: {
          BucketARNUpdate: $bucketArn,
          FileKeyUpdate: $fileKey
        }
      }
    },
    FlinkApplicationConfigurationUpdate: {
      ParallelismConfigurationUpdate: {
        ConfigurationTypeUpdate: "CUSTOM",
        ParallelismUpdate: ($kpu | tonumber),
        ParallelismPerKPUUpdate: 1,
        AutoScalingEnabledUpdate: false
      }
    },
    EnvironmentPropertyUpdates: {
      PropertyGroups: $env[0]
    }
  }' | aws kinesisanalyticsv2 update-application \
    --application-name "$APP_NAME" \
    --conditional-token "$TOKEN" \
    --application-configuration-update file:///dev/stdin \
    --no-cli-pager)

echo "$result" | jq '
.ApplicationDetail as $d |
{
  ApplicationName: $d.ApplicationName,
  ApplicationVersionId: $d.ApplicationVersionId,
}'
