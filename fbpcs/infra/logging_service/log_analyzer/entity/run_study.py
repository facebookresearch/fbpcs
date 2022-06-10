# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.log_analyzer.entity.instance_flow import InstanceFlow


@dataclass_json
@dataclass
class RunStudy:
    total_line_num: int
    first_log: str = ""
    # epoch time looks like "1654147049.156"
    start_epoch_time: Optional[str] = None
    summary_instances: List[str] = field(default_factory=list)
    # Count of log lines at ERROR level, not belonging to any instance flow.
    # This is negative integer. E.g. -2 means 2 log lines at ERROR level.
    error_line_count: int = 0
    error_lines: List[str] = field(default_factory=list)
    instances: Dict[str, InstanceFlow] = field(default_factory=dict)
