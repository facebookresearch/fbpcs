# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass, field
from typing import List

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.log_analyzer.entity.container_info import ContainerInfo
from fbpcs.infra.logging_service.log_analyzer.entity.log_context import LogContext


@dataclass_json
@dataclass
class FlowStage:
    context: LogContext
    stage_id: str
    stage_tags: List[str] = field(default_factory=list)
    container_count: int = 0
    # Count of failed containers, in this stage.
    # This is negative integer. E.g. -5 means 5 failed containers.
    failed_container_count: int = 0
    containers: List[ContainerInfo] = field(default_factory=list)
