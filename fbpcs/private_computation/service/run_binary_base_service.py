#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import Optional, List

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.util.wait_for_containers import wait_for_containers_async

DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200


class RunBinaryBaseService:
    async def start_containers(
        self,
        cmd_args_list: List[str],
        onedocker_svc: OneDockerService,
        binary_version: str,
        binary_name: str,
        timeout: Optional[int] = None,
        wait_for_containers_to_finish: bool = False,
    ) -> List[ContainerInstance]:
        logger = logging.getLogger(__name__)

        timeout = timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC

        pending_containers = onedocker_svc.start_containers(
            package_name=binary_name,
            version=binary_version,
            cmd_args_list=cmd_args_list,
            timeout=timeout,
        )

        containers = await onedocker_svc.wait_for_pending_containers(
            [container.instance_id for container in pending_containers]
        )

        logger.info("Task started")
        if wait_for_containers_to_finish:
            # Busy wait until the container is finished
            containers = await wait_for_containers_async(onedocker_svc, containers)
            if not all(container.status is ContainerInstanceStatus.COMPLETED for container in containers):
                raise RuntimeError(
                    "One or more containers failed. See the logs above to find the exact container_id"
                )
        return containers
