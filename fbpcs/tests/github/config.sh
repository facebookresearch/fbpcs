#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: Github e2e configs

## Shared
export E2E_CLUSTER_NAME="fbpcs-github-cicd"
export E2E_S3_BUCKET="fbpcs-github-e2e"
export E2E_GITHUB_S3_URL="https://$E2E_S3_BUCKET.s3.us-west-2.amazonaws.com"
export COORDINATOR="python3.8 -m fbpcs.private_computation_cli.private_computation_cli"

# Matched with yaml file
export CLOUD_CONFIG_FILE="fbpcs_e2e_aws.yml"
export DOCKER_CLOUD_CONFIG_FILE="/$CLOUD_CONFIG_FILE"
export DOCKER_INSTANCE_REPO="/instances"

## Lift
# Lift study configs
export LIFT_PUBLISHER_NAME="pl_publisher_github"
export LIFT_PARTNER_NAME="pl_partner_github"
export LIFT_NUM_MPC_CONTAIENRS=2
export LIFT_NUM_PID_CONTAINERS=2
export LIFT_CONCURRENCY=4
export LIFT_PUBLISHER_INPUT_FILE=$E2E_GITHUB_S3_URL/lift/inputs/publisher_e2e_input.csv
export LIFT_PARTNER_INPUT_FILE=$E2E_GITHUB_S3_URL/lift/inputs/partner_e2e_input.csv
export LIFT_OUTPUT_DIR=$E2E_GITHUB_S3_URL/lift/outputs

# Lift result comparison
export LIFT_OUTPUT_PATH=s3://$E2E_S3_BUCKET/lift/outputs
export LIFT_PUBLISHER_AGGREGATION_OUTPUT=$LIFT_OUTPUT_PATH/"$LIFT_PUBLISHER_NAME"_out_dir/shard_aggregation_stage/out.json
export LIFT_PARTNER_AGGREGATION_OUTPUT=$LIFT_OUTPUT_PATH/"$LIFT_PARTNER_NAME"_out_dir/shard_aggregation_stage/out.json

export LIFT_RESULT_PATH=s3://$E2E_S3_BUCKET/lift/results
export LIFT_PUBLISHER_EXPECTED_RESULT=$LIFT_RESULT_PATH/publisher_expected_result.json
export LIFT_PARTNER_EXPECTED_RESULT=$LIFT_RESULT_PATH/partner_expected_result.json

## Attribution
# Attribution study configs
export ATTRIBUTION_PUBLISHER_NAME="pa_publisher_github"
export ATTRIBUTION_PARTNER_NAME="pa_partner_github"

export ATTRIBUTION_NUM_FILES_PER_MPC_CONTAINER=1
export ATTRIBUTION_CONCURRENCY=1
export ATTRIBUTION_NUM_PID_CONTAINERS=1
export ATTRIBUTION_NUM_MPC_CONTAINERS=1
export ATTRIBUTION_RULE="last_touch_1d"
export ATTRIBUTION_TYPE="measurement"

export ATTRIBUTION_PUBLISHER_INPUT_FILE=$E2E_GITHUB_S3_URL/attribution/inputs/publisher_e2e_input.csv
export ATTRIBUTION_PARTNER_INPUT_FILE=$E2E_GITHUB_S3_URL/attribution/inputs/partner_e2e_input.csv

export ATTRIBUTION_OUTPUT_DIR=$E2E_GITHUB_S3_URL/attribution/outputs

# Attribution result comparison
export ATTRIBUTION_OUTPUT_PATH=s3://$E2E_S3_BUCKET/attribution/outputs
export ATTRIBUTION_PUBLISHER_AGGREGATION_OUTPUT=$ATTRIBUTION_OUTPUT_PATH/"$ATTRIBUTION_PUBLISHER_NAME"_out_dir/shard_aggregation_stage/out.json
export ATTRIBUTION_PARTNER_AGGREGATION_OUTPUT=$ATTRIBUTION_OUTPUT_PATH/"$ATTRIBUTION_PARTNER_NAME"_out_dir/shard_aggregation_stage/out.json

export ATTRIBUTION_RESULT_PATH=s3://$E2E_S3_BUCKET/attribution/results
export ATTRIBUTION_PUBLISHER_EXPECTED_RESULT=$ATTRIBUTION_RESULT_PATH/publisher_expected_result.json
export ATTRIBUTION_PARTNER_EXPECTED_RESULT=$ATTRIBUTION_RESULT_PATH/partner_expected_result.json
