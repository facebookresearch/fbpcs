#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: Check if all status are complete for lift|attribution stages: id_match, compute and aggregate_shards
set -e

container_name=$1
game=$2

if [ "$game" == 'lift' ]||[ "$game" == 'attribution' ]
then
    upper_game=${game^^}
    publisher_name=${upper_game}_PUBLIHSER_NAME
    partner_name=${upper_game}_PARTNER_NAME
else
    echo "Invalid Game"
    exit 1
fi


# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

docker_command="docker exec $container_name $COORDINATOR"

# check if all the status are COMPLETED, if yes, return true
function check_status_complete() {
    $docker_command get_instance "$1" --config="$DOCKER_CLOUD_CONFIG_FILE" || exit 1
    #filter out "status": "COMPLETED"
    non_complete_status=$($docker_command get "$1" \
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
# timeout is added in github workflows
until check_status_complete "${!publisher_name}" && check_status_complete "${!partner_name}"
    do echo "Waiting status to complete ..."
    sleep 60
done

echo "Stage completes successfully"
