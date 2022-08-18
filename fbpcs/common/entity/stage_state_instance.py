#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Union

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
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
        self.containers = self._update_containers(onedocker_svc)
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

    def _update_containers(
        self, onedocker_svc: OneDockerService
    ) -> List[ContainerInstance]:
        return [
            self._update_container(onedocker_svc, container)
            for container in self.containers
        ]

    def _update_container(
        self, onedocker_svc: OneDockerService, container: ContainerInstance
    ) -> ContainerInstance:
        # Stop updating from OneDocker, when the container is already stopped.
        if container.status in (
            ContainerInstanceStatus.COMPLETED,
            ContainerInstanceStatus.FAILED,
        ):
            return container
        return onedocker_svc.get_container(container.instance_id) or container

    def stop_containers(self, onedocker_svc: OneDockerService) -> None:
        container_ids = [instance.instance_id for instance in self.containers]
        errors = onedocker_svc.stop_containers(container_ids)
        error_msg = [(id, error) for id, error in zip(container_ids, errors) if error]
        if error_msg:
            raise RuntimeError(
                f"We encountered errors when stopping containers: {error_msg}"
            )
