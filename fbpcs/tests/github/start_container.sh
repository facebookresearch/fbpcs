#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: Start conatiner and setup directory and files
set -e

container_name=$1
image=$2

# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

if [ "$(docker ps -q -f name="$container_name")" ]; then
        # cleanup
        echo "Stop and remove old container"
        docker stop "$container_name"
        docker rm  "$container_name"
fi
# run container
echo "Run container $container_name"
docker run -td --name "$container_name" "$image"

# setup
echo "Create base directory for study"
docker exec "$container_name" mkdir "$DOCKER_INSTANCE_REPO" || exit 1

echo "Copy study yaml file to container"
docker cp "$CLOUD_CONFIG_FILE" "$container_name":"$DOCKER_CLOUD_CONFIG_FILE" || exit 1

echo "Setup AWS CLI"
docker exec "$container_name" aws configure set default/region us-west-2 --profile default || exit 1
