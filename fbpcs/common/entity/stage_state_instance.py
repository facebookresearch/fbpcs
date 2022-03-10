#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import time
from dataclasses import field, dataclass
from enum import Enum
from typing import Optional, List

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.instance_base import InstanceBase


class StageStateInstanceStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class StageStateInstance(InstanceBase):
    instance_id: str
    stage_name: str
    status: StageStateInstanceStatus = StageStateInstanceStatus.CREATED
    containers: List[ContainerInstance] = field(default_factory=list)
    start_time: int = field(default_factory=lambda: int(time.time()))
    end_time: Optional[int] = None

    @property
    def server_ips(self) -> List[str]:
        return [
            checked_cast(str, container.ip_address) for container in self.containers
        ]

    @property
    def elapsed_time(self) -> int:
        if self.end_time is None:
            return int(time.time()) - self.start_time

        return self.end_time - self.start_time

    def get_instance_id(self) -> str:
        return self.instance_id
