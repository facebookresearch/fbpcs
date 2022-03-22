#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: this script is to promote binaries for mpc games, should be run on local (not dev servser), and make sure you run with prod aws account

set -e

old_tag=$1
new_tag=$2

if [ $# -ne 2 ]; then
    echo "Usage: ./promote_binaries.sh old_tag new_tag"
    echo "Example tags: rc, canary, latest, etc."
    exit
fi

# list of the binaries
binary_names=(
    'private_lift/lift'
    'private_lift/aggregator'
    'private_attribution/decoupled_attribution'
    'private_attribution/decoupled_aggregation'
    'pcf2_attribution'
    'pcf2_aggregation'
    'private_attribution/shard-aggregator'
    'pid/private-id-client'
    'pid/private-id-server'
    # 'pid/private-id-multi-key-client'
    # 'pid/private-id-multi-key-server'
    'data_processing/sharder'
    'data_processing/sharder_hashed_for_pid'
    'data_processing/pid_preparer'
    'data_processing/lift_id_combiner'
    'data_processing/attribution_id_combiner'
)

s3_path="s3://one-docker-repository-prod"

update_folder_name_in_s3() {
    local path=$1
    local current_folder=$2
    local new_folder=$3

    echo "start copy binary path $path from $current_folder to $new_folder"
    aws s3 --recursive cp "$path/$current_folder" "$path/$new_folder"
}


echo "######################## [S3] Copy Binary to New Folder ########################"

for binary in "${binary_names[@]}"; do
    path="$s3_path/$binary"
    if [ "$new_tag" = 'latest' ]; then
        # move latest to today
        today=$(date '+%Y-%m-%d')
        update_folder_name_in_s3 "$path" "$new_tag" "$today"
    fi
    update_folder_name_in_s3 "$path" "$old_tag" "$new_tag"
done
