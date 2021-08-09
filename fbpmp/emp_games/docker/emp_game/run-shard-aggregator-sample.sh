#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

mkdir -p sample-output
USERDIR=$(pwd)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Run from the root dir of emp_games so the sample paths exist
cd "$SCRIPT_DIR" || exit

docker run --rm \
    -v "$SCRIPT_DIR/../../attribution/shard_aggregator/test/ad_object_format:/input" \
    -v "$USERDIR/sample-output:/output" \
    --network=host emp_game:latest \
        shard_aggregator \
        --party=1 \
        --input_base_path=/input/publisher_attribution_correctness_clickonly_clicktouch_out.json \
        --output_path=/output/publisher_shard_aggregation_out.json \
        --num_shards=2 \
        --first_shard_index=0 \
        2>&1 publisher & # Fork to background so "Bob" can run below

docker run --rm \
    -v "$SCRIPT_DIR/../../attribution/shard_aggregator/test/ad_object_format:/input" \
    -v "$USERDIR/sample-output:/output" \
    --network=host emp_game:latest \
        shard_aggregator \
        --party=2 \
        --input_base_path=/input/partner_attribution_correctness_clickonly_clicktouch_out.json \
        --output_path=/output/partner_shard_aggregation_out.json \
        --num_shards=2 \
        --first_shard_index=0 \
        --server_ip=127.0.0.1
