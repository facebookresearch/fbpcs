# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass, field
from typing import List, Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.log_analyzer.entity.flow_stage import FlowStage
from fbpcs.infra.logging_service.log_analyzer.entity.log_context import LogContext


@dataclass_json
@dataclass
class InstanceFlow:
    context: LogContext
    instance_id: str
    objective_id: Optional[str] = None
    cell_id: Optional[str] = None
    existing_instance_status: Optional[str] = None
    instance_container_count: int = 0
    # Count of failed containers, in this instance flow.
    # This is negative integer. E.g. -5 means 5 failed containers.
    instance_failed_container_count: int = 0
    summary_stages: List[str] = field(default_factory=list)
    # Count of log lines at ERROR level, in this instance flow.
    # This is negative integer. E.g. -2 means 2 log lines at ERROR level.
    instance_error_line_count: int = 0
    instance_error_lines: List[str] = field(default_factory=list)
    stages: List[FlowStage] = field(default_factory=list)
