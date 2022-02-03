#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

mkdir -p test-output
USERDIR=$(pwd)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Run from the root dir of data_processing so the test paths exist
cd "$SCRIPT_DIR" || exit

docker run --rm \
    -v "$SCRIPT_DIR/../../fbpcs/data_processing/test/attribution_id_combiner:/input" \
    -v "$USERDIR/test-output:/output" \
    --network=host data-processing:latest \
        attribution_id_combiner \
        --spine_path=/input/spine_input.csv_0 \
        --data_path=/input/partner_input.csv_0 \
        --output_path=/output/attribution_data_processing_output.csv_0
