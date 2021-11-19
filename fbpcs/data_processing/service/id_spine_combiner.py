#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.arg_builder import build_cmd_args
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.private_computation.service.constants import DEFAULT_SORT_STRATEGY
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class IdSpineCombinerService(RunBinaryBaseService):
    async def combine_on_container_async(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        onedocker_svc: OneDockerService,
        tmp_directory: str,
        binary_version: str,
        binary_name: str,
        sort_strategy: str = DEFAULT_SORT_STRATEGY,
        container_timeout: Optional[int] = None,
        # TODO T106159008: padding_size and run_name are only temporarily optional
        # because Lift does not use them. It should and will be required to use them.
        padding_size: Optional[int] = None,
        # run_name is the binary name used by the log cost to s3 feature
        run_name: Optional[str] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        cmd_args_list = []
        for shard in range(num_shards):
            # TODO: There's a weird dependency between these two services
            # AttributionIdSpineCombiner should operate independently of PIDStage
            next_spine_path = PIDStage.get_sharded_filepath(spine_path, shard)
            next_data_path = PIDStage.get_sharded_filepath(data_path, shard)
            next_output_path = PIDStage.get_sharded_filepath(output_path, shard)
            cmd_args = build_cmd_args(
                spine_path=next_spine_path,
                data_path=next_data_path,
                output_path=next_output_path,
                tmp_directory=tmp_directory,
                padding_size=padding_size,
                run_name=run_name,
                sort_strategy=sort_strategy,
            )
            cmd_args_list.append(cmd_args)

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
