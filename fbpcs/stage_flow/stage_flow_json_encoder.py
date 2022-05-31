#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import json
from typing import Any

from fbpcs.stage_flow.stage_flow import StageFlowMeta


class StageFlowJSONEncoder(json.JSONEncoder):
    # pyre-ignore[2] Overriding JSON encoder API
    def default(self, o: Any) -> str:
        if isinstance(o, StageFlowMeta):
            return o.__name__
        return json.JSONEncoder.default(self, o)
