# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import re
from typing import Dict, List, Pattern

from fbpcs.pc_pre_validation.binary_path import BinaryInfo

INPUT_DATA_VALIDATOR_NAME = "Input Data Validator"
BINARY_FILE_VALIDATOR_NAME = "Binary File Validator"

INPUT_DATA_TMP_FILE_PATH = "/tmp"
# 3GB
INPUT_DATA_MAX_FILE_SIZE_IN_BYTES: int = 3 * 1024 * 1024 * 1024

ID_FIELD_PREFIX = "id_"
CONVERSION_VALUE_FIELD = "conversion_value"
CONVERSION_TIMESTAMP_FIELD = "conversion_timestamp"
CONVERSION_METADATA_FIELD = "conversion_metadata"
VALUE_FIELD = "value"
EVENT_TIMESTAMP_FIELD = "event_timestamp"

PA_FIELDS: List[str] = [
    CONVERSION_VALUE_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    CONVERSION_METADATA_FIELD,
]
PL_FIELDS: List[str] = [
    VALUE_FIELD,
    EVENT_TIMESTAMP_FIELD,
]
REQUIRED_FIELDS: List[str] = [
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
]
FORMATTED_FIELDS: List[str] = [
    ID_FIELD_PREFIX,
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
]
ALL_FIELDS: List[str] = [
    ID_FIELD_PREFIX,
    VALUE_FIELD,
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_METADATA_FIELD,
    CONVERSION_VALUE_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
]

INTEGER_REGEX: Pattern[str] = re.compile(r"^[0-9]+$")
TIMESTAMP_REGEX: Pattern[str] = re.compile(r"^[0-9]{10}$")
BASE64_REGEX: Pattern[str] = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")

VALIDATION_REGEXES: Dict[str, Pattern[str]] = {
    ID_FIELD_PREFIX: BASE64_REGEX,
    CONVERSION_VALUE_FIELD: INTEGER_REGEX,
    CONVERSION_TIMESTAMP_FIELD: TIMESTAMP_REGEX,
    CONVERSION_METADATA_FIELD: INTEGER_REGEX,
    VALUE_FIELD: INTEGER_REGEX,
    EVENT_TIMESTAMP_FIELD: TIMESTAMP_REGEX,
}

VALID_LINE_ENDING_REGEX: Pattern[str] = re.compile(r".*(\S|\S\n)$")

DEFAULT_BINARY_REPOSITORY = (
    "https://one-docker-repository-prod.s3.us-west-2.amazonaws.com/"
)
DEFAULT_BINARY_VERSION = "latest"
DEFAULT_EXE_FOLDER = "/root/onedocker/package/"
BINARY_INFOS: List[BinaryInfo] = [
    BinaryInfo("data_processing/attribution_id_combiner"),
    BinaryInfo("data_processing/lift_id_combiner"),
    BinaryInfo("data_processing/pid_preparer"),
    BinaryInfo("data_processing/sharder"),
    BinaryInfo("data_processing/sharder_hashed_for_pid"),
    BinaryInfo("pid/private-id-client"),
    BinaryInfo("pid/private-id-server"),
    BinaryInfo("private_attribution/decoupled_aggregation"),
    BinaryInfo("private_attribution/decoupled_attribution"),
    BinaryInfo("private_attribution/pcf2_aggregation"),
    BinaryInfo("private_attribution/pcf2_attribution"),
    BinaryInfo("private_attribution/shard-aggregator"),
    BinaryInfo("private_lift/lift"),
]
ONEDOCKER_REPOSITORY_PATH = "ONEDOCKER_REPOSITORY_PATH"
ONEDOCKER_EXE_PATH = "ONEDOCKER_EXE_PATH"
