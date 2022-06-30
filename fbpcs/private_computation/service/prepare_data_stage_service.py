#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import DefaultDict, List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    start_combiner_service,
    start_sharder_service,
)


class PrepareDataStageService(PrivateComputationStageService):
    """Handles business logic for the private computation prepare data stage

    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _log_cost_to_s3: if money cost of the computation will be logged to S3
        _update_status_to_complete: if the status of the pc_instance should be set to complete after run_async finishes
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        update_status_to_complete: bool = False,
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
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
        await start_combiner_service(
            pc_instance,
            self._onedocker_svc,
            self._onedocker_binary_config_map,
            combine_output_path,
            log_cost_to_s3=self._log_cost_to_s3,
            wait_for_containers=True,
        )
        self._logger.info("Finished running CombinerService, starting to reshard")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        await start_sharder_service(
            pc_instance,
            self._onedocker_svc,
            self._onedocker_binary_config_map,
            combine_output_path,
            wait_for_containers=True,
        )
        self._logger.info("All sharding coroutines finished")
        # currently, prepare data blocks and runs until completion or failure (exception is thrown)
        # this if statement will let the legacy way of calling prepare data NOT update the status,
        # whereas the new way of calling prepare data can update the status.
        if self._update_status_to_complete:
            pc_instance.infra_config.status = pc_instance.current_stage.completed_status
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
        return pc_instance.infra_config.status
