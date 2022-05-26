# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Dict, Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.computation_run_info import (
    ComputationRunInfo,
)

# Log metadata for a Private Lift study run
@dataclass_json
@dataclass
class LiftRunInfo(ComputationRunInfo):
    # For LIFT run, the data is {"cell ID": {"instance ID": "objective ID"}}
    # E.g. {"498314401878593": {"1398047007316153": "491572669100524", "241776738106428": "349729400417538"}}
    cell_instance_objectives: Optional[Dict[str, Dict[str, str]]] = None
