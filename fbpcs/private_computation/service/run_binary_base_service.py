#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from typing import Dict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcs.experimental.cloud_logs.log_retriever import CloudProvider, LogRetriever

DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200
from fbpcs.private_computation.service.constants import DEFAULT_CONTAINER_TIMEOUT_IN_SEC

DEFAULT_WAIT_FOR_CONTAINER_POLL = 5


class RunBinaryBaseService:
    async def start_containers(
        self,
        cmd_args_list: List[str],
        onedocker_svc: OneDockerService,
        binary_version: str,
        binary_name: str,
        timeout: Optional[int] = None,
        wait_for_containers_to_finish: bool = False,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> List[ContainerInstance]:
        logger = logging.getLogger(__name__)

        timeout = timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC

        pending_containers = onedocker_svc.start_containers(
            package_name=binary_name,
            version=binary_version,
            cmd_args_list=cmd_args_list,
            timeout=timeout,
            env_vars=env_vars,
        )

        containers = await onedocker_svc.wait_for_pending_containers(
            [container.instance_id for container in pending_containers]
        )

        # Log the URL once... since the DataProcessingStage doesn't expose the
        # containers, we handle the logic directly in each stage like so.
        # It's kind of weird. T107574607 is tracking this.
        # Hope we're using AWS!
        log_retriever = LogRetriever(CloudProvider.AWS)
        for i, container in enumerate(containers):
            try:
                log_url = log_retriever.get_log_url(container.instance_id)
                logger.info(f"Container[{i}] URL -> {log_url}")
            except Exception:
                logger.warning(f"Could not look up URL for container[{i}]")

        logger.info("Task started")
        if wait_for_containers_to_finish:
            # Busy wait until the container is finished
            containers = await self.wait_for_containers_async(onedocker_svc, containers)
            if not all(
                container.status is ContainerInstanceStatus.COMPLETED
                for container in containers
            ):
                raise RuntimeError(
                    "One or more containers failed. See the logs above to find the exact container_id"
                )
        return containers

    @staticmethod
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
            onedocker_svc.logger.info(
                f"Waiting for container {instance_id} to complete"
            )
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
