#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

mkdir -p sample_output
USERDIR=$(pwd)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Run from the root dir of fbpcf so the sample paths exist
cd "$SCRIPT_DIR" || exit

docker run --rm \
    -v "$SCRIPT_DIR/sample_input:/input" \
    -v "$USERDIR/sample_output:/output" \
    --network=host fbpcs/tee-experiment:latest \
        plain_text_lift_calculator \
        --input_directory=/input \
        --input_publisher_filename="publisher_1k_0" \
        --input_partner_filename="partner_1k_0" \
        --output_directory=/output \
        --output_filename="out.csv" \
