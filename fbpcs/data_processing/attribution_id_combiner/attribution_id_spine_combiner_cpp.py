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
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.service.pid_service.pid_stage import PIDStage


# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class CppAttributionIdSpineCombinerService:
    def _get_combine_cmd_args_for_container(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        run_name: str,
        tmp_directory: str,
        padding_size: int,
        sort_strategy: str,
    ) -> str:
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        cmd_args = " ".join(
            [
                f"--spine_path={spine_path}",
                f"--data_path={data_path}",
                f"--output_path={output_path}",
                f"--run_name={run_name}",
                f"--tmp_directory={tmp_directory}",
                f"--padding_size={padding_size}",
                f"--sort_strategy={sort_strategy}",
            ]
        )
        return cmd_args

    def combine_on_container(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        run_name: str,
        onedocker_svc: OneDockerService,
        tmp_directory: str,
        padding_size: int,
        binary_version: str,
        sort_strategy: str = "sort",
        container_timeout: Optional[int] = None,
    ) -> None:
        asyncio.run(
            self.combine_on_container_async(
                spine_path,
                data_path,
                output_path,
                num_shards,
                run_name,
                onedocker_svc,
                tmp_directory,
                padding_size,
                binary_version,
                sort_strategy,
                container_timeout,
            )
        )

    async def combine_on_container_async(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        run_name: str,
        onedocker_svc: OneDockerService,
        tmp_directory: str,
        padding_size: int,
        binary_version: str,
        sort_strategy: str = "sort",
        container_timeout: Optional[int] = None,
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
            cmd_args = self._get_combine_cmd_args_for_container(
                next_spine_path,
                next_data_path,
                next_output_path,
                run_name,
                tmp_directory,
                padding_size,
                sort_strategy,
            )
            cmd_args_list.append(cmd_args)

        containers = await onedocker_svc.start_containers_async(
            package_name=OneDockerBinaryNames.ATTRIBUTION_ID_SPINE_COMBINER.value,
            version=binary_version,
            cmd_args_list=cmd_args_list,
            timeout=timeout,
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
