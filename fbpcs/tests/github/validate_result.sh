#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage: ./validate_result.sh lift|attribution, to validate aggregation output is matching with expected results

set -e
game=$1

# shellcheck source=fbcode/fbpcs/tests/github/config.sh
source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

TMP_DIR="/tmp"
PUBLISHER_EXPECTED_RESULT=$TMP_DIR/publisher_expected_result.json
PARTNER_EXPECTED_RESULT=$TMP_DIR/partner_expected_result.json
PUBLISHER_AGGREGATION_OUTPUT=$TMP_DIR/publisher_output.json
PARTNER_AGGREGATION_OUTPUT=$TMP_DIR/partner_output.json

echo "Remove publisher and partner files"
rm -f $TMP_DIR/publisher* $TMP_DIR/partner*

if [ "$game" == 'lift' ]
then
    aws s3 cp "$LIFT_PUBLISHER_AGGREGATION_OUTPUT" $PUBLISHER_AGGREGATION_OUTPUT
    aws s3 cp "$LIFT_PARTNER_AGGREGATION_OUTPUT" $PARTNER_AGGREGATION_OUTPUT
    aws s3 cp "$LIFT_PUBLISHER_EXPECTED_RESULT" $PUBLISHER_EXPECTED_RESULT
    aws s3 cp "$LIFT_PARTNER_EXPECTED_RESULT" $PARTNER_EXPECTED_RESULT
    echo "Lift files are copied to $TMP_DIR"

elif [ "$game" == "attribution" ]
then
    aws s3 cp $ATTRIBUTION_PUBLISHER_AGGREGATION_OUTPUT $PUBLISHER_AGGREGATION_OUTPUT
    aws s3 cp $ATTRIBUTION_PARTNER_AGGREGATION_OUTPUT $PARTNER_AGGREGATION_OUTPUT
    aws s3 cp $ATTRIBUTION_PUBLISHER_EXPECTED_RESULT $PUBLISHER_EXPECTED_RESULT
    aws s3 cp $ATTRIBUTION_PARTNER_EXPECTED_RESULT $PARTNER_EXPECTED_RESULT
    echo "Attribution files are copied to $TMP_DIR"

else
    echo "Invalid game: $game"
fi
# check if there is difference
function check_results_match() {
    diff_output=$(diff <(jq -S . "$1") <(jq -S . "$2"))
    if [ -z "$diff_output" ];
    then
        echo "$1 and $2 results are the same"
        return 0
    else
        echo "$1 and $2 results are different"
        return 1
    fi
}

if check_results_match $PUBLISHER_AGGREGATION_OUTPUT $PUBLISHER_EXPECTED_RESULT \
&& check_results_match $PARTNER_AGGREGATION_OUTPUT $PARTNER_EXPECTED_RESULT
then
    echo "$game e2e tests succeed"
else
    echo "$game e2e tests failed, results donot not match"
fi
