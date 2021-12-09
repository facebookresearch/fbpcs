#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Optional

from fbpcs.data_processing.service.sharding_service import ShardType, ShardingService
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


class PIDShardStage(PIDStage):
    async def run(
        self,
        stage_input: PIDStageInput,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        self.logger.info(f"[{self}] Called run")
        instance_id = stage_input.instance_id
        status = await self._ready(stage_input)
        await self.update_instance_status(instance_id=instance_id, status=status)
        if status != PIDStageStatus.READY:
            return status

        # Some invariant checking on the input and output paths
        input_paths = stage_input.input_paths
        output_paths = stage_input.output_paths
        num_shards = stage_input.num_shards
        is_validating = stage_input.is_validating

        if len(input_paths) != 1:
            raise ValueError(f"Expected 1 input path, not {len(input_paths)}")
        if len(output_paths) != 1:
            raise ValueError(f"Expected 1 output path, not {len(output_paths)}")

        # And finally call the method that actually does the work
        await self.update_instance_status(
            instance_id=instance_id, status=PIDStageStatus.STARTED
        )
        status = await self.shard(
            instance_id,
            input_paths[0],
            output_paths[0],
            num_shards,
            stage_input.hmac_key,
            wait_for_containers,
            container_timeout,
        )

        # if validating, copy synthetic shard and update instance.num_shards
        if is_validating:
            synthetic_data_output_path = f"{output_paths[0]}_{num_shards}"
            src_path = stage_input.synthetic_shard_path
            if src_path is None:
                raise ValueError(
                    "In validation mode, but src_path for synthetic shards is None"
                )
            self.copy_synthetic_shard(
                src_path,
                synthetic_data_output_path,
            )
            num_shards = stage_input.num_shards + 1
            await self.update_instance_num_shards(
                instance_id=instance_id, num_shards=num_shards
            )

        await self.update_instance_status(instance_id=instance_id, status=status)
        return status

    async def shard(
        self,
        instance_id: str,
        input_path: str,
        output_path: str,
        num_shards: int,
        hmac_key: Optional[str] = None,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        self.logger.info(f"[{self}] Starting ShardingService")
        sharder = ShardingService()

        try:
            args = sharder.build_args(
                input_path,
                output_base_path=output_path,
                file_start_index=0,
                num_output_files=num_shards,
                tmp_directory=self.onedocker_binary_config.tmp_directory,
                hmac_key=hmac_key,
            )
            binary_name = sharder.get_binary_name(ShardType.HASHED_FOR_PID)
            containers = await sharder.start_containers(
                cmd_args_list=[args],
                onedocker_svc=self.onedocker_svc,
                binary_version=self.onedocker_binary_config.binary_version,
                binary_name=binary_name,
                timeout=container_timeout,
                wait_for_containers_to_finish=wait_for_containers,
            )
            container = containers[0]  # there is always just 1 container
        except Exception as e:
            self.logger.exception(f"ShardingService failed: {e}")
            return PIDStageStatus.FAILED

        await self.update_instance_containers(instance_id, [container])
        status = self.get_stage_status_from_containers([container])
        self.logger.info(f"PIDShardStatus is {status}")
        return status

    async def _ready(
        self,
        stage_input: PIDStageInput,
    ) -> PIDStageStatus:
        """
        Check if this PIDStage is ready to run. Override the default behavior
        because we don't expect the input file to be sharded... since that's
        the purpose of this stage.
        """
        num_paths = len(stage_input.input_paths)
        self.logger.info(f"[{self}] Checking ready status of {num_paths} paths")
        if not self.files_exist(stage_input.input_paths):
            # If the input file *doesn't* exist, something happened. _ready is
            # only supposed to be called when the previous stage(s) succeeded.
            # In the case of the shard stage (the first stage), this likely
            # means that the user supplied a file that simply doesn't exist,
            # possibly by mistyping the input filepath.
            self.logger.error(
                f"Missing a necessary input file from {stage_input.input_paths}"
            )
            return PIDStageStatus.FAILED
        self.logger.info(f"[{self}] All files ready")
        return PIDStageStatus.READY
