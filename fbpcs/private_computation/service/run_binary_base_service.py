#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
from typing import Optional, List

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcs.private_computation.service.constants import DEFAULT_CONTAINER_TIMEOUT_IN_SEC


class RunBinaryBaseService:
    async def start_and_wait_for_containers(
        self,
        cmd_args_list: List[str],
        onedocker_svc: OneDockerService,
        binary_version: str,
        binary_name: str,
        timeout: Optional[int] = None,
    ):
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

        # Busy wait until all containers are finished
        any_failed = False
        for shard, container in enumerate(containers):
            container_id = container.instance_id
            # Busy wait until the container is finished
            status = ContainerInstanceStatus.UNKNOWN
            logger.info(f"Task[{shard}] started, waiting for completion")
            while status not in [
                ContainerInstanceStatus.FAILED,
                ContainerInstanceStatus.COMPLETED,
            ]:
                container = onedocker_svc.get_containers([container_id])[0]
                if not container:
                    break
                status = container.status
                # Sleep 5 seconds between calls to avoid an unintentional DDoS
                logger.debug(f"Latest status: {status}")
                await asyncio.sleep(5)

            if not container:
                continue
            logger.info(
                f"container_id({container.instance_id}) finished with status: {status}"
            )
            if status is not ContainerInstanceStatus.COMPLETED:
                logger.error(f"Container {container.instance_id} failed!")
                any_failed = True
        if any_failed:
            raise RuntimeError(
                "One or more containers failed. See the logs above to find the exact container_id"
            )
