# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import re
from typing import Pattern

INPUT_DATA_TMP_FILE_PATH = "/tmp"
PA_FIELDS = ["id_", "conversion_value", "conversion_timestamp", "conversion_metadata"]
PL_FIELDS = ["id_", "value", "event_timestamp"]
VALID_LINE_ENDING_REGEX: Pattern[str] = re.compile(r".*(\S|\S\n)$")
