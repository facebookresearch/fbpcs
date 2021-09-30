#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
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
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_stage_type import (
    PrivateComputationStageType,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    create_and_start_mpc_instance,
    gen_mpc_game_args_to_retry,
    map_private_computation_role_to_mpc_party,
    ready_for_partial_container_retry,
)


class ComputeMetricsStageService(PrivateComputationStageService):
    def __init__(
        self,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        mpc_service: MPCService,
        concurrency: Optional[int] = None,
        attribution_rule: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        is_validating: bool = False,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
        skip_partial_container_retry: bool = False,
    ) -> None:
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._mpc_service = mpc_service
        self._concurrency = concurrency
        self._attribution_rule = attribution_rule
        self._aggregation_type = aggregation_type
        self._is_validating = is_validating
        self._log_cost_to_s3 = log_cost_to_s3
        self._container_timeout = container_timeout
        self._skip_partial_container_retry = skip_partial_container_retry

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        # Prepare arguments for lift game
        # TODO T101225909: remove the option to pass in concurrency at the compute stage
        #   instead, always pass in at create_instance
        pc_instance.concurrency = self._concurrency or pc_instance.concurrency
        game_args = self._get_compute_metrics_game_args(
            pc_instance,
        )

        # We do this check here because depends on how game_args is generated, len(game_args) could be different,
        #   but we will always expect server_ips == len(game_args)
        if server_ips and len(server_ips) != len(game_args):
            raise ValueError(
                f"Unable to rerun MPC compute because there is a mismatch between the number of server ips given ({len(server_ips)}) and the number of containers ({len(game_args)}) to be spawned."
            )

        # Create and start MPC instance to run MPC compute
        logging.info("Starting to run MPC instance.")

        stage_data = PrivateComputationServiceData.get(
            pc_instance.game_type
        ).compute_stage
        binary_name = stage_data.binary_name
        game_name = checked_cast(str, stage_data.game_name)

        binary_config = self._onedocker_binary_config_map[binary_name]
        retry_counter_str = str(pc_instance.retry_counter)
        mpc_instance = await create_and_start_mpc_instance(
            mpc_svc=self._mpc_service,
            instance_id=pc_instance.instance_id
            + "_compute_metrics"
            + retry_counter_str,
            game_name=game_name,
            mpc_party=map_private_computation_role_to_mpc_party(pc_instance.role),
            num_containers=len(game_args),
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=self._container_timeout,
        )

        logging.info("MPC instance started running.")

        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pc_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
        return pc_instance

    @property
    def stage_type(self) -> PrivateComputationStageType:
        return PrivateComputationStageType.COMPUTE

    # TODO: Make an entity representation for game args that can dump a dict to pass
    # to mpc service. The entity will give us type checking and ensure that all args are
    # specified.
    def _get_compute_metrics_game_args(
        self,
        private_computation_instance: PrivateComputationInstance,
    ) -> List[Dict[str, Any]]:
        game_args = []

        # If this is to recover from a previous MPC compute failure
        if (
            ready_for_partial_container_retry(private_computation_instance)
            and not self._skip_partial_container_retry
        ):
            game_args_to_retry = gen_mpc_game_args_to_retry(
                private_computation_instance
            )
            if game_args_to_retry:
                game_args = game_args_to_retry

        # If this is a normal run, dry_run, or unable to get the game args to retry from mpc service
        if not game_args:
            num_containers = private_computation_instance.num_mpc_containers
            # update num_containers if is_vaildating = true
            if self._is_validating:
                num_containers += 1

            common_compute_game_args = {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.compute_stage_output_base_path,
                "num_files": private_computation_instance.num_files_per_mpc_container,
                "concurrency": private_computation_instance.concurrency,
            }

            # TODO: we eventually will want to get rid of the if-else here, which will be
            #   easy to do once the Lift and Attribution MPC compute games are consolidated
            if (
                private_computation_instance.game_type
                is PrivateComputationGameType.ATTRIBUTION
            ):
                # TODO: we will write aggregation_type, attribution_rule and log_cost_to_s3
                #   to the instance, so later this function interface will get simplified
                game_args = self._get_attribution_game_args(
                    private_computation_instance,
                    common_compute_game_args,
                )

            elif (
                private_computation_instance.game_type
                is PrivateComputationGameType.LIFT
            ):
                game_args = self._get_lift_game_args(
                    private_computation_instance, common_compute_game_args
                )

        return game_args

    def _get_lift_game_args(
        self,
        private_computation_instance: PrivateComputationInstance,
        common_compute_game_args: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        game_args = []
        game_args = [
            {
                **common_compute_game_args,
                **{
                    "file_start_index": i
                    * private_computation_instance.num_files_per_mpc_container
                },
            }
            for i in range(private_computation_instance.num_mpc_containers)
        ]
        return game_args

    def _get_attribution_game_args(
        self,
        private_computation_instance: PrivateComputationInstance,
        common_compute_game_args: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        game_args = []
        game_args = [
            {
                **common_compute_game_args,
                **{
                    "aggregators": self._aggregation_type,
                    "attribution_rules": self._attribution_rule,
                    "file_start_index": i
                    * private_computation_instance.num_files_per_mpc_container,
                    "use_xor_encryption": True,
                    "run_name": private_computation_instance.instance_id
                    if self._log_cost_to_s3
                    else "",
                    "max_num_touchpoints": private_computation_instance.padding_size,
                    "max_num_conversions": private_computation_instance.padding_size,
                },
            }
            for i in range(private_computation_instance.num_mpc_containers)
        ]
        return game_args
