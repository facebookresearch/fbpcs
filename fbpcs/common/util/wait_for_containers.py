# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
from typing import List

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService


DEFAULT_WAIT_FOR_CONTAINER_POLL = 5


async def wait_for_containers_async(
    onedocker_svc: OneDockerService,
    containers: List[ContainerInstance],
    poll: int = DEFAULT_WAIT_FOR_CONTAINER_POLL,
) -> List[ContainerInstance]:
    updated_containers = containers.copy()
    end_states = {
        ContainerInstanceStatus.COMPLETED,
        ContainerInstanceStatus.FAILED,
    }
    for i, container in enumerate(updated_containers):
        instance_id = container.instance_id
        onedocker_svc.logger.info(f"Waiting for container {instance_id} to complete")
        status = container.status
        while status not in end_states:
            await asyncio.sleep(poll)
            container = onedocker_svc.get_containers([instance_id])[0]
            if not container:
                break
            status = container.status
            updated_containers[i] = container
        if status is not ContainerInstanceStatus.COMPLETED:
            onedocker_svc.logger.warning(
                f"Container {instance_id} failed with status {status}"
            )
            return updated_containers
    return updated_containers
