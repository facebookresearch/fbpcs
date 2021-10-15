#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: Run Lift different stages: create_instance, id_match, compute, aggregate

set -e

container_name=$1
stage=$2

# shellcheck disable=SC1091,SC1090
source "$(dirname "${BASH_SOURCE[0]}")/config.sh" || exit 1

docker_command="docker exec $container_name $LIFT_COORDINATOR"

case "$stage" in
    create_instance )
        echo "Create Lift Publisher instance"
        $docker_command create_instance "$LIFT_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --role=publisher \
            --input_path="$LIFT_PUBLISHER_INPUT_FILE" \
            --output_dir="$LIFT_OUTPUT_DIR" \
            --num_pid_containers="$LIFT_NUM_PID_CONTAINERS" \
            --num_mpc_containers="$LIFT_NUM_MPC_CONTAIENRS" \
            --concurrency="$LIFT_CONCURRENCY"
        echo "Create Lift Partner instance"
        $docker_command create_instance "$LIFT_PARTNER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --role=partner \
            --input_path="$LIFT_PARTNER_INPUT_FILE" \
            --output_dir="$LIFT_OUTPUT_DIR" \
            --num_pid_containers="$LIFT_NUM_PID_CONTAINERS" \
            --num_mpc_containers="$LIFT_NUM_MPC_CONTAIENRS" \
            --concurrency="$LIFT_CONCURRENCY"
            ;;
    id_match|compute|aggregate )
        echo "Lift Publisher $stage starts"
        $docker_command "$stage" "$LIFT_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE"
        echo "Get Publisher Ips"
        # get_server_ips returns an extra carriage return character
        publisher_server_ips=$($docker_command get_server_ips "$LIFT_PUBLIHSER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" | sed 's/\r//g')
        echo "Server IPs are ${publisher_server_ips}"
        echo "Lift Partner $stage starts"
        $docker_command "$stage" "$LIFT_PARTNER_NAME" \
            --config="$DOCKER_CLOUD_CONFIG_FILE" \
            --server_ips="${publisher_server_ips}"
        ;;
    * )
        echo "Not a valid Lift stage"
esac
