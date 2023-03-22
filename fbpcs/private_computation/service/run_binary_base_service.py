#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from typing import Dict, List, Optional

from fbpcp.entity.certificate_request import CertificateRequest

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.entity.container_type import ContainerType
from fbpcp.error.pcp import ThrottlingError
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.service.retry_handler import RetryHandler
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
        wait_for_containers_to_start_up: bool = True,
        existing_containers: Optional[List[ContainerInstance]] = None,
        container_type: Optional[ContainerType] = None,
        certificate_request: Optional[CertificateRequest] = None,
        env_vars_list: Optional[List[Dict[str, str]]] = None,
        opa_workflow_path: Optional[str] = None,
    ) -> List[ContainerInstance]:
        logger = logging.getLogger(__name__)

        timeout = timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC

        containers_to_start = self.get_containers_to_start(
            len(cmd_args_list), existing_containers
        )

        if containers_to_start:
            logger.info(f"Spinning up {len(containers_to_start)} containers")
            logger.info(f"Containers to start: {containers_to_start}")

            new_pending_containers = onedocker_svc.start_containers(
                package_name=binary_name,
                version=binary_version,
                cmd_args_list=[cmd_args_list[i] for i in containers_to_start],
                timeout=timeout,
                env_vars=[env_vars_list[i] for i in containers_to_start]
                if env_vars_list
                else env_vars,
                container_type=container_type,
                certificate_request=certificate_request,
                opa_workflow_path=opa_workflow_path,
            )

            pending_containers = self.get_pending_containers(
                new_pending_containers, containers_to_start, existing_containers
            )
        else:
            logger.info(
                "No containers are in a failed state - skipping container start-up"
            )
            pending_containers = existing_containers or []

        if not wait_for_containers_to_start_up:
            logger.info("Skipped container warm up")
            return pending_containers

        with RetryHandler(
            ThrottlingError, logger=logger, backoff_seconds=30
        ) as retry_handler:
            containers = await retry_handler.execute(
                onedocker_svc.wait_for_pending_containers,
                [container.instance_id for container in pending_containers],
            )

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
    def _remove_finished_containers_from_container_ids(
        onedocker_svc: OneDockerService,
        containers: List[Optional[ContainerInstance]],
        container_ids: List[str],
        finished_containers: List[Optional[ContainerInstance]],
    ) -> List[str]:
        end_states = {
            ContainerInstanceStatus.COMPLETED,
            ContainerInstanceStatus.FAILED,
        }
        container_id_set = set(container_ids)

        filtered_containers = filter(
            lambda container: container.status in end_states if container else False,
            containers,
        )
        for container in filtered_containers:
            # pyre-ignore[16] This is not actually an Optional[ContainerInstance] here since we filter out None in the filter function on line 112.
            if container.status is not ContainerInstanceStatus.COMPLETED:
                onedocker_svc.logger.warning(
                    # pyre-ignore[16] This is not actually an Optional[ContainerInstance] here since we filter out None in the filter function on line 112.
                    f"Container {container.instance_id} failed with exit code {container.exit_code}."
                )
            finished_containers.append(container)
            container_id_set.remove(container.instance_id)
        return list(container_id_set)

    @staticmethod
    async def wait_for_containers_async(
        onedocker_svc: OneDockerService,
        containers: List[ContainerInstance],
        poll: int = DEFAULT_WAIT_FOR_CONTAINER_POLL,
    ) -> List[ContainerInstance]:
        """Wait for the requested containers to finish.

        Args:
            onedocker_svc: An instance of OneDockerService to query the container statuses
            containers: A list of ContainerInstances to wait to finish.
            poll: Number of seconds to wait between polls

        Returns:
            A list of ContainerInstances whose status is COMPLETED or FAILED
        """
        container_ids = [container.instance_id for container in containers]
        finished_containers = []

        updated_containers = onedocker_svc.get_containers(container_ids)
        pending_container_ids = (
            RunBinaryBaseService._remove_finished_containers_from_container_ids(
                onedocker_svc, updated_containers, container_ids, finished_containers
            )
        )
        while pending_container_ids:
            await asyncio.sleep(poll)
            updated_containers = onedocker_svc.get_containers(pending_container_ids)
            pending_container_ids = (
                RunBinaryBaseService._remove_finished_containers_from_container_ids(
                    onedocker_svc,
                    updated_containers,
                    pending_container_ids,
                    finished_containers,
                )
            )

        return finished_containers

    @classmethod
    def get_containers_to_start(
        cls,
        num_containers: int,
        existing_containers: Optional[List[ContainerInstance]] = None,
    ) -> List[int]:
        if not existing_containers:
            # if there are no existing containers, we need to spin containers up for
            # every command
            return list(range(num_containers))

        if num_containers != len(existing_containers):
            raise ValueError(
                f"Cannot retry stage - list of existing containers ({len(existing_containers)}) is not consistent with number of requested containers ({num_containers})"
            )

        # only start containers that previously failed
        return [
            i
            for i, container in enumerate(existing_containers)
            if container.status is ContainerInstanceStatus.FAILED
        ]

    @classmethod
    def get_pending_containers(
        cls,
        new_pending_containers: List[ContainerInstance],
        containers_to_start: List[int],
        existing_containers: Optional[List[ContainerInstance]] = None,
    ) -> List[ContainerInstance]:
        if not existing_containers:
            return new_pending_containers

        pending_containers = existing_containers.copy()
        for i, new_pending_container in zip(
            containers_to_start, new_pending_containers
        ):
            # replace existing container with the new pending container
            pending_containers[i] = new_pending_container

        return pending_containers
