#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.service.onedocker import OneDockerService

from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.pid_mr_config import Protocol
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)

from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import get_pc_status_from_stage_state


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
        padding_size: Optional[int] = None,
        protocol_type: str = Protocol.PID_PROTOCOL.value,
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._log_cost_to_s3 = log_cost_to_s3
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.padding_size = padding_size
        self.protocol_type = protocol_type

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

        # TODO: we will write log_cost_to_s3 to the instance, so this function interface
        #   will get simplified
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )
        container_instances = await self._start_combiner_service(
            pc_instance,
            self._onedocker_svc,
            self._onedocker_binary_config_map,
            combine_output_path,
            log_cost_to_s3=self._log_cost_to_s3,
            max_id_column_count=pc_instance.product_config.common.pid_max_column_count,
            protocol_type=self.protocol_type,
            wait_for_containers_to_start_up=should_wait_spin_up,
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

    # TODO: If we're going to deprecate prepare_data_stage_service.py,
    # we can just move this method to id_spine_combiner_stage_service.py as private method
    async def _start_combiner_service(
        self,
        private_computation_instance: PrivateComputationInstance,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        combine_output_path: str,
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        wait_for_containers: bool = False,
        max_id_column_count: int = 1,
        protocol_type: str = Protocol.PID_PROTOCOL.value,
        wait_for_containers_to_start_up: bool = True,
    ) -> List[ContainerInstance]:
        """Run combiner service and return those container instances

        Args:
            private_computation_instance: The PC instance to run combiner service with
            onedocker_svc: Spins up containers that run binaries in the cloud
            onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
            combine_output_path: out put path for the combine result
            log_cost_to_s3: if money cost of the computation will be logged to S3
            wait_for_containers: block until containers to finish running, default False

        Returns:
            return: list of container instances running combiner service
        """
        stage_data = PrivateComputationServiceData.get(
            private_computation_instance.infra_config.game_type
        ).combiner_stage

        binary_name = stage_data.binary_name
        binary_config = onedocker_binary_config_map[binary_name]

        # TODO: T106159008 Add on attribution specific args
        if (
            private_computation_instance.infra_config.game_type
            is PrivateComputationGameType.ATTRIBUTION
        ):
            run_name = (
                private_computation_instance.infra_config.instance_id
                if log_cost_to_s3
                else ""
            )
            padding_size = checked_cast(
                int,
                private_computation_instance.product_config.common.padding_size,
            )
            multi_conversion_limit = None
            log_cost = log_cost_to_s3
        else:
            run_name = None
            padding_size = None
            multi_conversion_limit = (
                private_computation_instance.product_config.common.padding_size
            )
            log_cost = None

        combiner_service = checked_cast(
            IdSpineCombinerService,
            stage_data.service,
        )

        if protocol_type == Protocol.MR_PID_PROTOCOL.value:
            spine_path = private_computation_instance.pid_mr_stage_output_spine_path
            data_path = private_computation_instance.pid_mr_stage_output_data_path
        else:
            spine_path = private_computation_instance.pid_stage_output_spine_path
            data_path = private_computation_instance.pid_stage_output_data_path

        args = combiner_service.build_args(
            spine_path=spine_path,
            data_path=data_path,
            output_path=combine_output_path,
            num_shards=private_computation_instance.infra_config.num_pid_containers,
            tmp_directory=binary_config.tmp_directory,
            protocol_type=protocol_type,
            max_id_column_cnt=max_id_column_count,
            run_name=run_name,
            padding_size=padding_size,
            multi_conversion_limit=multi_conversion_limit,
            log_cost=log_cost,
            run_id=private_computation_instance.infra_config.run_id,
        )
        env_vars = {}
        if binary_config.repository_path:
            env_vars[ONEDOCKER_REPOSITORY_PATH] = binary_config.repository_path

        return await combiner_service.start_containers(
            cmd_args_list=args,
            onedocker_svc=onedocker_svc,
            binary_version=binary_config.binary_version,
            binary_name=binary_name,
            timeout=None,
            wait_for_containers_to_finish=wait_for_containers,
            env_vars=env_vars,
            wait_for_containers_to_start_up=wait_for_containers_to_start_up,
            existing_containers=private_computation_instance.get_existing_containers_for_retry(),
        )
