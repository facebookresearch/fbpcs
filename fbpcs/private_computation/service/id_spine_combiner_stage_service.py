#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import DefaultDict, List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.pid.service.pid_service.utils import (
    get_max_id_column_cnt,
    get_pid_protocol_from_num_shards,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    get_pc_status_from_stage_state,
    start_combiner_service,
)


class IdSpineCombinerStageService(PrivateComputationStageService):
    """Handles business logic for the private computation id spine combiner stage

    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _log_cost_to_s3: if money cost of the computation will be logged to S3
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        pid_svc: Optional[PIDService] = None,
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._pid_svc = pid_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._log_cost_to_s3 = log_cost_to_s3
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation prepare data stage - spine combiner stage

        Args:
            pc_instance: the private computation instance to run prepare data with
            server_ips: ignored

        Returns:
            An updated version of pc_instance
        """

        output_path = pc_instance.data_processing_output_path
        combine_output_path = output_path + "_combine"

        self._logger.info(f"[{self}] Starting id spine combiner service")

        pid_protocol = get_pid_protocol_from_num_shards(
            pc_instance.infra_config.num_pid_containers,
            False if self._pid_svc is None else self._pid_svc.multikey_enabled,
        )

        # TODO: we will write log_cost_to_s3 to the instance, so this function interface
        #   will get simplified
        container_instances = await start_combiner_service(
            pc_instance,
            self._onedocker_svc,
            self._onedocker_binary_config_map,
            combine_output_path,
            log_cost_to_s3=self._log_cost_to_s3,
            max_id_column_count=get_max_id_column_cnt(pid_protocol),
        )
        self._logger.info("Finished running CombinerService")

        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )

        pc_instance.infra_config.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)
