#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.service.mpc import MPCService
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.private_computation_instance import (
    AttributionRule,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    create_and_start_mpc_instance,
    get_updated_pc_status_mpc_game,
    map_private_computation_role_to_mpc_party,
)


class PCF2AttributionStageService(PrivateComputationStageService):
    """Handles business logic for the pcf2.0 based private attribution stage

    Private attributes:
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _mpc_svc: creates and runs MPC instances
        _is_validating: if a test shard is injected to do run time correctness validation
        _log_cost_to_s3: if money cost of the computation will be logged to S3
        _container_timeout: optional duration in seconds before cloud containers timeout
    """

    def __init__(
        self,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        mpc_service: MPCService,
        is_validating: bool = False,
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        container_timeout: Optional[int] = None,
    ) -> None:
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._mpc_service = mpc_service
        self._is_validating = is_validating
        self._log_cost_to_s3 = log_cost_to_s3
        self._container_timeout = container_timeout

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the pcf2.0 based private attribution stage

        Args:
            pc_instance: the private computation instance to run attribution stage
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        # Prepare arguments
        game_args = self._get_compute_metrics_game_args(
            pc_instance,
        )

        # We do this check here because depends on how game_args is generated, len(game_args) could be different,
        #   but we will always expect server_ips == len(game_args)
        if server_ips and len(server_ips) != len(game_args):
            raise ValueError(
                f"Unable to rerun MPC attribution game because there is a mismatch between the number of server ips given ({len(server_ips)}) and the number of containers ({len(game_args)}) to be spawned."
            )

        # Create and start MPC instance to run MPC compute
        logging.info("Starting to run MPC instance for pcf2.0 attribution.")
        stage_data = PrivateComputationServiceData.PCF2_ATTRIBUTION_STAGE_DATA
        binary_name = OneDockerBinaryNames.PCF2_ATTRIBUTION.value
        game_name = checked_cast(str, stage_data.game_name)

        binary_config = self._onedocker_binary_config_map[binary_name]
        retry_counter_str = str(pc_instance.retry_counter)
        mpc_instance = await create_and_start_mpc_instance(
            mpc_svc=self._mpc_service,
            instance_id=pc_instance.instance_id
            + "_pcf2_attribution"
            + retry_counter_str,
            game_name=game_name,
            mpc_party=map_private_computation_role_to_mpc_party(pc_instance.role),
            num_containers=len(game_args),
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=self._container_timeout,
            repository_path=binary_config.repository_path,
        )

        logging.info("MPC instance started running for pcf2.0 attribution.")

        # Push MPC instance to PrivateComputationInstance.instances and update PA Instance status
        pc_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
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

    # For now, only passing the attribution game arguments, as this game is currently only used for PA.
    def _get_compute_metrics_game_args(
        self,
        private_computation_instance: PrivateComputationInstance,
    ) -> List[Dict[str, Any]]:
        """Gets the game args passed to game binaries by onedocker

        When onedocker spins up containers to run games, it unpacks a dictionary containing the
        arguments required by the game binary being ran. This function prepares that dictionary.

        Args:
            pc_instance: the private computation instance to generate game args for

        Returns:
            MPC game args to be used by onedocker
        """

        attribution_rule = checked_cast(
            AttributionRule, private_computation_instance.attribution_rule
        )
        if self._log_cost_to_s3:
            run_name = (
                private_computation_instance.instance_id
                + "_"
                + GameNames.PCF2_ATTRIBUTION.value
            )
            if private_computation_instance.post_processing_data:
                private_computation_instance.post_processing_data.s3_cost_export_output_paths.add(
                    f"att-logs/{run_name}_{private_computation_instance.role.value.title()}.json"
                )
        else:
            run_name = ""

        common_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "output_base_path": private_computation_instance.pcf2_attribution_stage_output_base_path,
            "num_files": private_computation_instance.num_files_per_mpc_container,
            "concurrency": private_computation_instance.concurrency,
            "run_name": run_name,
            "max_num_touchpoints": private_computation_instance.padding_size,
            "max_num_conversions": private_computation_instance.padding_size,
            "log_cost": self._log_cost_to_s3,
            "attribution_rules": attribution_rule.value,
            "use_xor_encryption": True,
            "use_postfix": True,
        }

        game_args = [
            {
                **common_game_args,
                **{
                    "file_start_index": i
                    * private_computation_instance.num_files_per_mpc_container,
                },
            }
            for i in range(private_computation_instance.num_mpc_containers)
        ]

        return game_args
