# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass
from typing import Any, Dict

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class CellObjectiveInstance:
    data: Dict[str, Dict[str, Dict[str, Any]]]
