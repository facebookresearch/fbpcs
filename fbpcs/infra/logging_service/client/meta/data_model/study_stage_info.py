# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import List, Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.base_info import BaseInfo
from fbpcs.infra.logging_service.client.meta.data_model.container_instance import (
    ContainerInstance,
)

# Log metadata for a stage in a study instance
@dataclass_json
@dataclass
class StudyStageInfo(BaseInfo):
    run_id: str
    instance_id: str
    info_ts: str
    stage_instance_type: str
    stage_name: Optional[str] = None
    stage_status: Optional[str] = None
    containers: Optional[List[ContainerInstance]] = None
