#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage:Remove ECS tasks and aggregation outputs in S3 bucket
set -e

# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

# Remove ECS tasks
RUNNING_TASKS=$(aws ecs list-tasks --cluster "$E2E_CLUSTER_NAME" --desired-status RUNNING --region us-west-2 | grep -E "task/" | sed -E "s/.*task\/(.*)\"/\1/" | sed -z 's/\n/ /g')
IFS=', ' read -r -a array <<< "$RUNNING_TASKS"
for task in "${array[@]}"
do
    aws ecs stop-task --cluster "${E2E_CLUSTER_NAME}" --task "${task}" --region us-west-2 > /dev/null
    echo "Task:${task} is stopped"
done

# Remove all the outputs from previous run
aws s3 rm --recursive "$LIFT_OUTPUT_PATH"
aws s3 rm --recursive "$ATTRIBUTION_OUTPUT_PATH"
