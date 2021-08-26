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
    -v "$SCRIPT_DIR/../../fbpmp/emp_games/attribution/test/shard_test_input/publisher:/input" \
    -v "$USERDIR/sample-output:/output" \
    --network=host emp-games:latest \
        attribution_calculator \
        --party=1 \
        --attribution_rules=last_click_1d \
        --aggregators=measurement \
        --input_base_path=/input/publisher_correctness_clickonly_clicktouch_input.csv \
        --output_base_path=/output/publisher_correctness_clickonly_clicktouch_output.json \
        --num_files=2 \
        --file_start_index=0 \
        2>&1 publisher & # Fork to background so "partner" can run below

docker run --rm \
    -v "$SCRIPT_DIR/../../fbpmp/emp_games/attribution/test/shard_test_input/partner:/input" \
    -v "$USERDIR/sample-output:/output" \
    --network=host emp-games:latest \
        attribution_calculator \
        --party=2 \
        --server_ip=127.0.0.1 \
        --input_base_path=/input/partner_correctness_clickonly_clicktouch_input.csv \
        --output_base_path=/output/partner_correctness_clickonly_clicktouch_output.json \
        --num_files=2 \
        --file_start_index=0
