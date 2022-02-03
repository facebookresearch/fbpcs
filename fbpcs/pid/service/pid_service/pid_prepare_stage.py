#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
from typing import Optional

from fbpcs.data_processing.pid_preparer.union_pid_preparer_cpp import (
    CppUnionPIDDataPreparerService,
)
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


MAX_RETRY = 0


class PIDPrepareStage(PIDStage):
    async def run(
        self,
        stage_input: PIDStageInput,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        self.logger.info(f"[{self}] Called run")
        instance_id = stage_input.instance_id
        # First check that our input data is ready
        status = await self._ready(stage_input)
        await self.update_instance_status(instance_id=instance_id, status=status)
        if status != PIDStageStatus.READY:
            return status

        # Some invariant checking on the input and output paths
        input_paths = stage_input.input_paths
        output_paths = stage_input.output_paths
        num_shards = (
            stage_input.num_shards + 1
            if stage_input.is_validating
            else stage_input.num_shards
        )
        if len(input_paths) != 1:
            raise ValueError(f"Expected 1 input path, not {len(input_paths)}")
        if len(output_paths) != 1:
            raise ValueError(f"Expected 1 output path, not {len(output_paths)}")

        # And finally call the method that actually does the work
        await self.update_instance_status(
            instance_id=instance_id, status=PIDStageStatus.STARTED
        )
        status = await self.prepare(
            instance_id,
            input_paths[0],
            output_paths[0],
            num_shards,
            stage_input.fail_fast or False,
            wait_for_containers,
            container_timeout,
        )
        await self.update_instance_status(instance_id=instance_id, status=status)
        return status

    async def prepare(
        self,
        instance_id: str,
        input_path: str,
        output_path: str,
        num_shards: int,
        fail_fast: bool,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        self.logger.info(f"[{self}] Starting CppUnionPIDDataPreparerService")
        preparer = CppUnionPIDDataPreparerService()
        # TODO: Preparer could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        coroutines = []
        for shard in range(num_shards):
            next_input_path = self.get_sharded_filepath(input_path, shard)
            next_output_path = self.get_sharded_filepath(output_path, shard)
            coro = preparer.prepare_on_container_async(
                input_path=next_input_path,
                output_path=next_output_path,
                onedocker_svc=self.onedocker_svc,
                binary_version=self.onedocker_binary_config.binary_version,
                tmp_directory=self.onedocker_binary_config.tmp_directory,
                max_retry=0 if fail_fast else MAX_RETRY,
                wait_for_container=wait_for_containers,
                container_timeout=container_timeout,
            )
            coroutines.append(coro)

        # Wait for all coroutines to finish
        containers = await asyncio.gather(*coroutines)
        containers = list(containers)

        await self.update_instance_containers(instance_id, containers)
        status = self.get_stage_status_from_containers(containers)
        self.logger.info(f"PIDPrepareStatus is {status}")
        return status
