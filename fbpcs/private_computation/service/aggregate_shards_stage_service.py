#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import DefaultDict, List, Optional

from fbpcp.service.mpc import MPCService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    ResultVisibility,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    create_and_start_mpc_instance,
    get_updated_pc_status_mpc_game,
    map_private_computation_role_to_mpc_party,
)


class AggregateShardsStageService(PrivateComputationStageService):
    """Handles business logic for the private computation aggregate metrics stage

    Private attributes:
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _mpc_svc: creates and runs MPC instances
        _log_cost_to_s3: TODO
        _container_timeout: optional duration in seconds before cloud containers timeout
    """

    def __init__(
        self,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        mpc_service: MPCService,
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        container_timeout: Optional[int] = None,
    ) -> None:
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._mpc_service = mpc_service
        self._log_cost_to_s3 = log_cost_to_s3
        self._container_timeout = container_timeout

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation aggregate metrics stage

        Args:
            pc_instance: the private computation instance to run aggregate metrics with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        num_shards = (
            pc_instance.infra_config.num_mpc_containers
            * pc_instance.infra_config.num_files_per_mpc_container
        )

        # TODO T101225989: map aggregation_type from the compute stage to metrics_format_type
        metrics_format_type = (
            "lift"
            if pc_instance.infra_config.game_type is PrivateComputationGameType.LIFT
            else "ad_object"
        )

        binary_name = OneDockerBinaryNames.SHARD_AGGREGATOR.value
        binary_config = self._onedocker_binary_config_map[binary_name]

        # Get output path of previous stage depending on what stage flow we are using
        # Using "PrivateComputationDecoupledStageFlow" instead of PrivateComputationDecoupledStageFlow.get_cls_name() to avoid
        # circular import error.
        if pc_instance.get_flow_cls_name in [
            "PrivateComputationDecoupledStageFlow",
            "PrivateComputationDecoupledLocalTestStageFlow",
        ]:
            input_stage_path = pc_instance.decoupled_aggregation_stage_output_base_path
        elif pc_instance.get_flow_cls_name in [
            "PrivateComputationPCF2StageFlow",
            "PrivateComputationPCF2LocalTestStageFlow",
            "PrivateComputationPIDPATestStageFlow",
        ]:
            input_stage_path = pc_instance.pcf2_aggregation_stage_output_base_path
        elif pc_instance.get_flow_cls_name == "PrivateComputationPCF2LiftStageFlow":
            input_stage_path = pc_instance.pcf2_lift_stage_output_base_path
        else:
            input_stage_path = pc_instance.compute_stage_output_base_path

        if self._log_cost_to_s3:
            run_name = pc_instance.infra_config.instance_id

            if pc_instance.post_processing_data:
                pc_instance.post_processing_data.s3_cost_export_output_paths.add(
                    f"sa-logs/{run_name}_{pc_instance.infra_config.role.value.title()}.json",
                )
        else:
            run_name = ""

        # Create and start MPC instance
        game_args = [
            {
                "input_base_path": input_stage_path,
                "metrics_format_type": metrics_format_type,
                "num_shards": num_shards,
                "output_path": pc_instance.shard_aggregate_stage_output_path,
                "threshold": pc_instance.k_anonymity_threshold,
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
            },
        ]
        # We should only export visibility to scribe when it's set
        if pc_instance.result_visibility is not ResultVisibility.PUBLIC:
            result_visibility = int(pc_instance.result_visibility)
            for arg in game_args:
                arg["visibility"] = result_visibility

        mpc_instance = await create_and_start_mpc_instance(
            mpc_svc=self._mpc_service,
            instance_id=pc_instance.infra_config.instance_id
            + "_aggregate_shards"
            + str(pc_instance.infra_config.retry_counter),
            game_name=GameNames.SHARD_AGGREGATOR.value,
            mpc_party=map_private_computation_role_to_mpc_party(
                pc_instance.infra_config.role
            ),
            num_containers=1,
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=self._container_timeout,
            repository_path=binary_config.repository_path,
        )
        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pc_instance.infra_config.instances.append(
            PCSMPCInstance.from_mpc_instance(mpc_instance)
        )
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Updates the MPCInstances and gets latest PrivateComputationInstance status

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        return get_updated_pc_status_mpc_game(pc_instance, self._mpc_service)
