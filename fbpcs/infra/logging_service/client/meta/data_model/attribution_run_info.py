# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.computation_run_info import (
    ComputationRunInfo,
)

# Log metadata for a Private Attribution study run
@dataclass_json
@dataclass
class AttributionRunInfo(ComputationRunInfo):
    # For ATTRIBUTION run,
    dataset_id: Optional[str] = None
