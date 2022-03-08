# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import re
from typing import Dict, List, Pattern

INPUT_DATA_TMP_FILE_PATH = "/tmp"

ID_FIELD = "id_"
CONVERSION_VALUE_FIELD = "conversion_value"
CONVERSION_TIMESTAMP_FIELD = "conversion_timestamp"
CONVERSION_METADATA_FIELD = "conversion_metadata"
VALUE_FIELD = "value"
EVENT_TIMESTAMP_FIELD = "event_timestamp"

PA_FIELDS: List[str] = [
    ID_FIELD,
    CONVERSION_VALUE_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    CONVERSION_METADATA_FIELD,
]
PL_FIELDS: List[str] = [
    ID_FIELD,
    VALUE_FIELD,
    EVENT_TIMESTAMP_FIELD,
]

INTEGER_REGEX: Pattern[str] = re.compile(r"^[0-9]+$")
TIMESTAMP_REGEX: Pattern[str] = re.compile(r"^[0-9]{10}$")
BASE64_REGEX: Pattern[str] = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")

VALIDATION_REGEXES: Dict[str, Pattern[str]] = {
    ID_FIELD: BASE64_REGEX,
    CONVERSION_VALUE_FIELD: INTEGER_REGEX,
    CONVERSION_TIMESTAMP_FIELD: TIMESTAMP_REGEX,
    CONVERSION_METADATA_FIELD: INTEGER_REGEX,
    VALUE_FIELD: INTEGER_REGEX,
    EVENT_TIMESTAMP_FIELD: TIMESTAMP_REGEX,
}

VALID_LINE_ENDING_REGEX: Pattern[str] = re.compile(r".*(\S|\S\n)$")
