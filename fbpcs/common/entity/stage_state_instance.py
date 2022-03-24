#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import time
from dataclasses import field, dataclass
from enum import Enum
from typing import Optional, List, Union

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.error.pcp import PcpError
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.pcs_container_instance import PCSContainerInstance


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
    containers: List[Union[PCSContainerInstance, ContainerInstance]] = field(
        default_factory=list
    )
    creation_ts: int = field(default_factory=lambda: int(time.time()))
    end_ts: Optional[int] = None

    @property
    def server_ips(self) -> List[str]:
        return [
            checked_cast(str, container.ip_address) for container in self.containers
        ]

    @property
    def elapsed_time(self) -> int:
        end_ts = self.end_ts or int(time.time())
        return end_ts - self.creation_ts

    def get_instance_id(self) -> str:
        return self.instance_id

    def update_status(
        self,
        onedocker_svc: OneDockerService,
    ) -> StageStateInstanceStatus:
        container_ids = [container.instance_id for container in self.containers]
        containers = list(filter(None, onedocker_svc.get_containers(container_ids)))
        if len(self.containers) != len(containers):
            raise PcpError(
                f"Instance {self.instance_id} has {len(containers)} containers after update, but expecting {len(self.containers)} containers!"
            )

        # replacing new containers to have it update to date
        self.containers = containers
        statuses = {container.status for container in self.containers}
        # updating stage state status based on containers status
        if ContainerInstanceStatus.FAILED in statuses:
            self.status = StageStateInstanceStatus.FAILED
        elif all(status is ContainerInstanceStatus.COMPLETED for status in statuses):
            self.status = StageStateInstanceStatus.COMPLETED
            self.end_ts = int(time.time())
        elif ContainerInstanceStatus.STARTED in statuses:
            self.status = StageStateInstanceStatus.STARTED
        else:
            self.status = StageStateInstanceStatus.UNKNOWN

        return self.status
