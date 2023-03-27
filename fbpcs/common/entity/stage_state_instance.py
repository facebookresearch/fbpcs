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
    server_uris: Optional[List[str]] = None

    @property
    def server_ips(self) -> List[str]:
        if self.status in [
            StageStateInstanceStatus.UNKNOWN,
            StageStateInstanceStatus.CREATED,
            # if a container fails during initialization, then it is possible for
            # that container to have no server ip. If we do not include FAILED here,
            # the checked cast will fail. We don't need to transmit server ips
            # when in a FAILED state, so this should be fine.
            StageStateInstanceStatus.FAILED,
        ]:
            return []

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
        self.containers = self._get_updated_containers(onedocker_svc)
        statuses = {container.status for container in self.containers}
        # updating stage state status based on containers status
        if ContainerInstanceStatus.FAILED in statuses:
            self.status = StageStateInstanceStatus.FAILED
        elif all(status is ContainerInstanceStatus.COMPLETED for status in statuses):
            self.status = StageStateInstanceStatus.COMPLETED
            self.end_ts = int(time.time())
        elif ContainerInstanceStatus.UNKNOWN in statuses:
            self.status = StageStateInstanceStatus.UNKNOWN
        elif ContainerInstanceStatus.STARTED in statuses:
            self.status = StageStateInstanceStatus.STARTED
        else:
            self.status = StageStateInstanceStatus.UNKNOWN

        return self.status

    def _get_updated_containers(
        self, onedocker_svc: OneDockerService
    ) -> List[ContainerInstance]:
        containers_to_update = self.get_containers_to_update(self.containers)
        updated_containers = onedocker_svc.get_containers(
            [self.containers[idx].instance_id for idx in containers_to_update]
        )
        new_updated_containers = self.containers.copy()
        for i, updated_container in zip(containers_to_update, updated_containers):
            if updated_container is not None:
                # replace existing container with the new updated container
                new_updated_containers[i] = updated_container

        return new_updated_containers

    @classmethod
    def get_containers_to_update(
        cls,
        existing_containers: List[ContainerInstance],
    ) -> List[int]:
        # only update containers that previously not stopped
        return [
            i
            for i, container in enumerate(existing_containers)
            if container.status
            not in (ContainerInstanceStatus.COMPLETED, ContainerInstanceStatus.FAILED)
        ]

    def stop_containers(self, onedocker_svc: OneDockerService) -> None:
        container_ids = [instance.instance_id for instance in self.containers]
        errors = onedocker_svc.stop_containers(container_ids)
        error_msg = [(id, error) for id, error in zip(container_ids, errors) if error]
        if error_msg:
            raise RuntimeError(
                f"We encountered errors when stopping containers: {error_msg}"
            )
