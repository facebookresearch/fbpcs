#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: Check if all status are complete for attribution stages
set -e
container_name=$1

# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

docker_command="docker exec $container_name $ATTRIBUTION_COORDINATOR"

# check if all the status are COMPLETED, if yes, return true
function check_status_complete() {
    $docker_command get_instance "$1" --config="$DOCKER_CLOUD_CONFIG_FILE" || exit 1
    #filter out "status": "COMPLETED"
    non_complete_status=$($docker_command get_instance "$1" \
        --config="$DOCKER_CLOUD_CONFIG_FILE" 2>&1 | \
        grep -o -E "\"status\": \"[a-zA-Z]+\"" | \
        awk -F: '$2 != " \"COMPLETED\""')
    if [ -z "$non_complete_status" ]
    then
        echo "$1 status is all complete"
        return 0
    else
        echo "$1 has following status: $non_complete_status"
        return 1
    fi
}

until check_status_complete $"$ATTRIBUTION_PUBLIHSER_NAME" \
&& check_status_complete $"$ATTRIBUTION_PARTNER_NAME"
    do echo "Waiting status to complete ..."
    sleep 60
done

echo "Stage complets successfully"
