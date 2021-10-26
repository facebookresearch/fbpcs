#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage:Run attribution different stages: create_instance, id_match, prepare_compute_input, compute_attribution, aggregate_shards

set -e

container_name=$1
stage=$2

# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

docker_command="docker exec $container_name $COORDINATOR"

case "$stage" in
    create_instance )
        echo "Create Attribution Publisher instance"
        $docker_command create_instance "$ATTRIBUTION_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --input_path="$ATTRIBUTION_PUBLISHER_INPUT_FILE" \
            --output_dir="$ATTRIBUTION_OUTPUT_DIR" \
            --role=publisher \
            --game_type=attribution \
            --num_pid_containers="$ATTRIBUTION_NUM_PID_CONTAINERS" \
            --num_mpc_containers="$ATTRIBUTION_NUM_MPC_CONTAINERS" \
            --num_files_per_mpc_container="$ATTRIBUTION_NUM_FILES_PER_MPC_CONTAINER" \
            --concurrency="$ATTRIBUTION_CONCURRENCY" \
            --attribution_rule="$ATTRIBUTION_RULE" \
            --aggregation_type="$ATTRIBUTION_TYPE"
        echo "Create Attribution Partner instance"
        $docker_command create_instance "$ATTRIBUTION_PARTNER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --input_path="$ATTRIBUTION_PARTNER_INPUT_FILE" \
            --output_dir="$ATTRIBUTION_OUTPUT_DIR" \
            --role=partner \
            --game_type=attribution \
            --num_pid_containers="$ATTRIBUTION_NUM_PID_CONTAINERS" \
            --num_mpc_containers="$ATTRIBUTION_NUM_MPC_CONTAINERS" \
            --num_files_per_mpc_container="$ATTRIBUTION_NUM_FILES_PER_MPC_CONTAINER" \
            --concurrency="$ATTRIBUTION_CONCURRENCY" \
            --attribution_rule="$ATTRIBUTION_RULE" \
            --aggregation_type="$ATTRIBUTION_TYPE"
            ;;
    prepare_compute_input )
        echo "Attribution Publisher $stage starts"
        $docker_command run_next "$ATTRIBUTION_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE"
        echo "Attribution Partner $stage starts"
        $docker_command run_next "$ATTRIBUTION_PARTNER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE"
        ;;
    id_match|compute_metrics|aggregate_shards )
        echo "Attribution Publisher $stage starts"
        $docker_command run_next "$ATTRIBUTION_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE"
        #Temporary solution: need to call get_status before get_sever_ips, otherwise get_server_ips returns none
        $docker_command get_instance "$ATTRIBUTION_PUBLIHSER_NAME" --config="$DOCKER_CLOUD_CONFIG_FILE"
        echo "Get Publisher Ips"
        publisher_server_ips=$($docker_command get_server_ips "$ATTRIBUTION_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" | sed 's/\r//g')
        echo "Server IPs are ${publisher_server_ips}"
        echo "Attribution Partner $stage starts"
        $docker_command run_next "$ATTRIBUTION_PARTNER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --server_ips="${publisher_server_ips}"
        ;;

    * )
        echo "Not a valid Attribution stage"
esac
