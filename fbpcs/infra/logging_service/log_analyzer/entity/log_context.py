# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class LogContext:
    line_num: int
    # elapsed_second looks like "63.156"
    elapsed_second: Optional[str] = None
    # epoch time looks like "1654147049.156"
    epoch_time: Optional[str] = None
    # timestamp is UTC time, e.g. "2022-05-31 20:59:25.169"
    utc_time: Optional[str] = None
