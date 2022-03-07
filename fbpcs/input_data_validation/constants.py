# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import re
from typing import List, Pattern

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

VALID_LINE_ENDING_REGEX: Pattern[str] = re.compile(r".*(\S|\S\n)$")
