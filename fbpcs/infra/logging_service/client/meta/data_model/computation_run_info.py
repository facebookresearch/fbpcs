# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.base_info import BaseInfo

# Log metadata for a private computation run
@dataclass_json
@dataclass
class ComputationRunInfo(BaseInfo):
    # start timestamp, e.g. "2022-04-15T19:00:46.099Z"
    start_ts: str
    # Game type is like "LIFT", "ATTRIBUTION".
    game_type: str
    # Launch type is per game type. E.g.
    # For LIFT: run_study.
    # For ATTRIBUTION: run_attribution.
    launch_type: str
