#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage:Remove ECS tasks and aggregation outputs in S3 bucket
set -e

# Remove ECS tasks
RUNNING_TASKS=$(aws ecs list-tasks --cluster "fbpcs-github-cicd" --desired-status RUNNING --region us-west-2 | grep -E "task/" | sed -E "s/.*task\/(.*)\"/\1/" | sed -z 's/\n/ /g')
IFS=', ' read -r -a array <<< "$RUNNING_TASKS"
for task in "${array[@]}"
do
    aws ecs stop-task --cluster "fbpcs-github-cicd" --task "${task}" --region us-west-2 > /dev/null
    echo "Task:${task} is stopped"
done

# Remove all the outputs from previous run
aws s3 rm --recursive "s3://fbpcs-github-e2e/lift/outputs"
aws s3 rm --recursive "s3://fbpcs-github-e2e/attribution/outputs"
