# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import re
from typing import Dict, List, Pattern, Set

from fbpcs.pc_pre_validation.binary_path import BinaryInfo

INPUT_DATA_VALIDATOR_NAME = "Input Data Validator"
BINARY_FILE_VALIDATOR_NAME = "Binary File Validator"

INPUT_DATA_TMP_FILE_PATH = "/tmp"
# 3GB
INPUT_DATA_MAX_FILE_SIZE_IN_BYTES: int = 3 * 1024 * 1024 * 1024
INTEGER_MAX_VALUE: int = 2147483647
# Allow up to 15% of processed rows to have an out of range timestamp
TIMESTAMP_OUT_OF_RANGE_MAX_THRESHOLD = 0.15

# 15 minutes
STREAMING_DURATION_LIMIT_IN_SECONDS: int = 15 * 60

# use a LARGE container with 16 vCPUs
# This is the max number of concurrent processes that can run concurrently
MAX_PARALLELISM: int = 16

# 10 MB
# Smallest chunk size before another worker is added
MIN_CHUNK_SIZE: int = 10 * 1024 * 1024

ID_FIELD_PREFIX = "id_"
COHORT_ID_FIELD = "cohort_id"
CONVERSION_VALUE_FIELD = "conversion_value"
CONVERSION_TIMESTAMP_FIELD = "conversion_timestamp"
CONVERSION_METADATA_FIELD = "conversion_metadata"
VALUE_FIELD = "value"
EVENT_TIMESTAMP_FIELD = "event_timestamp"
TIMESTAMP = "timestamp"
OPPORTUNITY_TIMESTAMP = "opportunity_timestamp"
AD_ID = "ad_id"
IS_CLICK = "is_click"


PA_FIELDS: List[str] = [
    CONVERSION_VALUE_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    CONVERSION_METADATA_FIELD,
]
PA_PUBLISHER_FIELDS: List[str] = [AD_ID, TIMESTAMP, IS_CLICK]
PL_FIELDS: List[str] = [
    VALUE_FIELD,
    EVENT_TIMESTAMP_FIELD,
]
PL_PUBLISHER_FIELDS: List[str] = [
    ID_FIELD_PREFIX,
    OPPORTUNITY_TIMESTAMP,
]
PRIVATE_ID_DFCA_FIELDS: List[str] = ["partner_user_id"]
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
    TIMESTAMP,
    OPPORTUNITY_TIMESTAMP,
    AD_ID,
    IS_CLICK,
]
RANGE_FIELDS: Set[str] = {
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    VALUE_FIELD,
    CONVERSION_VALUE_FIELD,
}
TIMESTAMP_RANGE_FIELDS: Set[str] = {
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    OPPORTUNITY_TIMESTAMP,
}
VALUE_FIELDS: Set[str] = {
    CONVERSION_VALUE_FIELD,
    VALUE_FIELD,
}

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
    COHORT_ID_FIELD: INTEGER_REGEX,
    TIMESTAMP: TIMESTAMP_REGEX,
    OPPORTUNITY_TIMESTAMP: TIMESTAMP_REGEX,
    AD_ID: INTEGER_REGEX,
    IS_CLICK: INTEGER_REGEX,
}

VALID_LINE_ENDING_REGEX: Pattern[str] = re.compile(r".*(\S|\S\n)$")

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
    # TODO: Add UDP-ralted binaries when rolling out
]
ONEDOCKER_EXE_PATH = "ONEDOCKER_EXE_PATH"
OUT_OF_RANGE_COUNT = "out_of_range_count"
ERROR_MESSAGES = "error_messages"
