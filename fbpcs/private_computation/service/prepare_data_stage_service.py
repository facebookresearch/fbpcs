#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import asyncio
import logging
import math
from typing import DefaultDict
from typing import List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.typing import checked_cast
from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.data_processing.sharding.sharding import ShardType
from fbpcs.data_processing.sharding.sharding_cpp import CppShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class PrepareDataStageService(PrivateComputationStageService):
    """Handles business logic for the private computation prepare data stage

    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _is_validating: if a test shard is injected to do run time correctness validation
        _log_cost_to_s3: if money cost of the computation will be logged to S3
        _update_status_to_complete: if the status of the pc_instance should be set to complete after run_async finishes
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        is_validating: bool = False,
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        update_status_to_complete: bool = False,
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._is_validating = is_validating
        self._log_cost_to_s3 = log_cost_to_s3
        self._update_status_to_complete = update_status_to_complete
        self._logger: logging.Logger = logging.getLogger(__name__)

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation prepare data stage

        Args:
            pc_instance: the private computation instance to run prepare data with
            server_ips: ignored

        Returns:
            An updated version of pc_instance
        """

        output_path = pc_instance.data_processing_output_path
        combine_output_path = output_path + "_combine"

        self._logger.info(f"[{self}] Starting id spine combiner service")

        # TODO: we will write log_cost_to_s3 to the instance, so this function interface
        #   will get simplified
        await self._run_combiner_service(
            pc_instance, combine_output_path, self._log_cost_to_s3
        )

        self._logger.info("Finished running CombinerService, starting to reshard")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        await self._run_sharder_service(pc_instance, combine_output_path)
        # currently, prepare data blocks and runs until completion or failure (exception is thrown)
        # this if statement will let the legacy way of calling prepare data NOT update the status,
        # whereas the new way of calling prepare data can update the status.
        if self._update_status_to_complete:
            pc_instance.status = pc_instance.current_stage.completed_status
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Currently, prepare data blocks until completion or failure, so
        there is no mechanism or need for updating the status.

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        return pc_instance.status

    async def _run_combiner_service(
        self,
        pl_instance: PrivateComputationInstance,
        combine_output_path: str,
        log_cost_to_s3: bool,
    ) -> None:
        stage_data = PrivateComputationServiceData.get(
            pl_instance.game_type
        ).combiner_stage

        binary_name = stage_data.binary_name
        binary_config = self._onedocker_binary_config_map[binary_name]

        # TODO: T106159008 Add on attribution specific args
        if pl_instance.game_type is PrivateComputationGameType.ATTRIBUTION:
            run_name = pl_instance.instance_id if log_cost_to_s3 else ""
            padding_size = checked_cast(int, pl_instance.padding_size)
        else:
            run_name = None
            padding_size = None

        combiner_service = checked_cast(
            IdSpineCombinerService,
            stage_data.service,
        )

        args = combiner_service.build_args(
            spine_path=pl_instance.pid_stage_output_spine_path,
            data_path=pl_instance.pid_stage_output_data_path,
            output_path=combine_output_path,
            num_shards=pl_instance.num_pid_containers + 1
            if pl_instance.is_validating
            else pl_instance.num_pid_containers,
            tmp_directory=binary_config.tmp_directory,
            run_name=run_name,
            padding_size=padding_size,
        )
        await combiner_service.start_and_wait_for_containers(
            args, self._onedocker_svc, binary_config.binary_version, binary_name
        )

    async def _run_sharder_service(
        self, pl_instance: PrivateComputationInstance, combine_output_path: str
    ) -> None:
        sharder = CppShardingService()
        self._logger.info("Instantiated sharder")

        coros = []
        for shard_index in range(
            pl_instance.num_pid_containers + 1
            if pl_instance.is_validating
            else pl_instance.num_pid_containers
        ):
            path_to_shard = PIDStage.get_sharded_filepath(
                combine_output_path, shard_index
            )
            self._logger.info(f"Input path to sharder: {path_to_shard}")

            shards_per_file = math.ceil(
                (pl_instance.num_mpc_containers / pl_instance.num_pid_containers)
                * pl_instance.num_files_per_mpc_container
            )
            shard_index_offset = shard_index * shards_per_file
            self._logger.info(
                f"Output base path to sharder: {pl_instance.data_processing_output_path}, {shard_index_offset=}"
            )

            binary_config = self._onedocker_binary_config_map[
                OneDockerBinaryNames.SHARDER.value
            ]
            coro = sharder.shard_on_container_async(
                shard_type=ShardType.ROUND_ROBIN,
                filepath=path_to_shard,
                output_base_path=pl_instance.data_processing_output_path,
                file_start_index=shard_index_offset,
                num_output_files=shards_per_file,
                onedocker_svc=self._onedocker_svc,
                binary_version=binary_config.binary_version,
                tmp_directory=binary_config.tmp_directory,
                should_log_container_urls=True,
            )
            coros.append(coro)

        # Wait for all coroutines to finish
        await asyncio.gather(*coros)
        self._logger.info("All sharding coroutines finished")
