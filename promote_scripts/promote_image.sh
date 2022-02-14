#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: this script is to promote images for mpc games, should be run on local (not dev servser), and make sure you run with prod aws account

set -e

# TODO T110861946: once private_lift_service is deprecated, remove the sfid argument
#   and use hard-coded "private_measurement/private_computation_service"
old_tag=$1
new_tag=$2

if [ $# -ne 2 ]; then
    echo "Usage: ./promote_image.sh old_tag new_tag"
    echo "Validate sfids: ad_measurement/private_lift_service, private_measurement/private_computation_service"
    echo "Example tags: rc, canary, latest, etc."
    exit
fi

# list of the docker images
repo_names=('pl-coordinator-env')
today=$(date '+%Y-%m-%d')

update_tag_for_ecr_image() {
    local repo_name=$1
    local current_tag=$2
    local new_tag=$3

    echo "start processing image: $repo_name, move $current_tag to $new_tag"

    # retag image: https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-retag.html
    MANIFEST=$(aws ecr batch-get-image --repository-name "$repo_name" --image-ids imageTag="$current_tag" --query 'images[].imageManifest' --output text)
    aws ecr put-image --repository-name "$repo_name" --image-tag "$new_tag" --image-manifest "$MANIFEST" >/dev/null
}

echo "######################## [ECR] Mark Image to New Tag ########################"

for repo_name in "${repo_names[@]}"; do
    if [ "$new_tag" = 'latest' ]; then
        # move latest to today
        update_tag_for_ecr_image "$repo_name" "$new_tag" "$today"
    fi
    update_tag_for_ecr_image "$repo_name" "$old_tag" "$new_tag"
done
