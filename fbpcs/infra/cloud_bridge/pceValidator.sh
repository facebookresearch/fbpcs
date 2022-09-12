#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# shellcheck disable=SC1091
# shellcheck disable=SC1090

log_pce_validator() {
    local text=$1
    echo "$(date +"%M:%S") -> $text" >> "$PCE_VALIDATOR_LOG_FILE"
}

validate_pce () {
    local region=$1
    local pce_id=$2
    log_pce_validator "validate_pce $region $pce_id"
    local pceValidatorOutput
    pceValidatorOutput=$(python3 -m pce.validator --region="$region" --pce-id="$pce_id" 2>&1)
    local pceValidatorExitCode=$?
    log_pce_validator "$pceValidatorOutput"
    log_pce_validator "validator exitcode: $pceValidatorExitCode"

    if [ $pceValidatorExitCode -ne 0 ]
    then
        echo "PCE validator found some issue..please analyze further to debug the issue"
        exit 1
    else
        log_pce_validator "PCE validation successful"
    fi
}

# main script starts here
validate_pce "$1" "$2"
