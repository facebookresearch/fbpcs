#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import os
import pathlib
from typing import List, Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.arg_builder import build_cmd_args
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.service.pid_service.pid_stage import PIDStage


CPP_COMBINER_PATH = pathlib.Path(
    os.environ.get("CPP_LIFT_ID_SPINE_COMBINER_PATH", "cpp_bin/lift_id_spine_combiner")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class CppLiftIdSpineCombinerService:
    async def combine_on_container_async(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        container_timeout: Optional[int] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        cmds: List[str] = []
        for shard in range(num_shards):
            # TODO: There's a weird dependency between these two services
            # LiftIdSpineCombiner should operate independently of PIDStage
            next_spine_path = PIDStage.get_sharded_filepath(spine_path, shard)
            next_data_path = PIDStage.get_sharded_filepath(data_path, shard)
            next_output_path = PIDStage.get_sharded_filepath(output_path, shard)
            cmd = build_cmd_args(
                spine_path=next_spine_path,
                data_path=next_data_path,
                output_path=next_output_path,
                tmp_directory=tmp_directory,
            )
            cmds.append(cmd)

        pending_containers = onedocker_svc.start_containers(
            package_name=OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value,
            version=binary_version,
            cmd_args_list=cmds,
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
