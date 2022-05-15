#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


"""
CLI for running validations on the input data for private computations


Usage:
    pc_pre_validation_cli
        --input-file-path=<input-file-path>
        --cloud-provider=<cloud-provider>
        --region=<region>
        [--access-key-id=<access-key-id>]
        [--access-key-data=<access-key-data>]
        [--start-timestamp=<start-timestamp>]
        [--end-timestamp=<end-timestamp>]
        [--binary-version=<binary-version>]
"""


from typing import cast, List, Optional as OptionalType

from docopt import docopt
from fbpcs.pc_pre_validation.binary_file_validator import BinaryFileValidator
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.input_data_validator import InputDataValidator
from fbpcs.pc_pre_validation.validator import Validator
from fbpcs.pc_pre_validation.validators_runner import run_validators
from fbpcs.private_computation.entity.cloud_provider import CloudProvider
from schema import Optional, Or, Schema, Use

INPUT_FILE_PATH = "--input-file-path"
CLOUD_PROVIDER = "--cloud-provider"
REGION = "--region"
ACCESS_KEY_ID = "--access-key-id"
ACCESS_KEY_DATA = "--access-key-data"
START_TIMESTAMP = "--start-timestamp"
END_TIMESTAMP = "--end-timestamp"
BINARY_VERSION = "--binary-version"


def main(argv: OptionalType[List[str]] = None) -> None:
    optional_string = Or(None, str)
    cloud_provider_from_string = Use(lambda arg: CloudProvider[arg])

    s = Schema(
        {
            INPUT_FILE_PATH: str,
            CLOUD_PROVIDER: cloud_provider_from_string,
            REGION: str,
            Optional(ACCESS_KEY_ID): optional_string,
            Optional(ACCESS_KEY_DATA): optional_string,
            Optional(START_TIMESTAMP): optional_string,
            Optional(END_TIMESTAMP): optional_string,
            Optional(BINARY_VERSION): optional_string,
        }
    )
    arguments = s.validate(docopt(__doc__, argv))
    assert arguments
    print("Parsed pc_pre_validation_cli arguments")

    validators = [
        cast(
            Validator,
            InputDataValidator(
                input_file_path=arguments[INPUT_FILE_PATH],
                cloud_provider=arguments[CLOUD_PROVIDER],
                region=arguments[REGION],
                start_timestamp=arguments[START_TIMESTAMP],
                end_timestamp=arguments[END_TIMESTAMP],
                access_key_id=arguments[ACCESS_KEY_ID],
                access_key_data=arguments[ACCESS_KEY_DATA],
            ),
        ),
        cast(
            Validator,
            BinaryFileValidator(
                region=arguments[REGION],
                access_key_id=arguments[ACCESS_KEY_ID],
                access_key_data=arguments[ACCESS_KEY_DATA],
                binary_version=arguments[BINARY_VERSION],
            ),
        ),
    ]

    (aggregated_result, aggregated_report) = run_validators(validators)
    overall_result_str = f"Overall Validation Result: {aggregated_result.value}"

    if aggregated_result == ValidationResult.FAILED:
        raise Exception(f"{aggregated_report}\n{overall_result_str}")
    elif aggregated_result == ValidationResult.SUCCESS:
        print(f"Success: {aggregated_report}\n{overall_result_str}")
    else:
        raise Exception(
            f"Unknown validation result: {aggregated_result}.\n"
            + f"Validation report: {aggregated_report}\n{overall_result_str}"
        )


if __name__ == "__main__":
    main()
