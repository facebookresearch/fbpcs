#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.util.typing import checked_cast

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionConfig,
    AttributionRule,
)
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3

from fbpcs.private_computation.service.mpc.mpc import (
    map_private_computation_role_to_mpc_party,
    MPCService,
)
from fbpcs.private_computation.service.pcf2_lift_stage_service import (
    PCF2LiftStageService,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    stop_stage_service,
)


class ComputeMetricsStageService(PrivateComputationStageService):
    """Handles business logic for the private computation compute metrics stage

    Private attributes:
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        _mpc_svc: creates and runs MPC instances
        _log_cost_to_s3: if money cost of the computation will be logged to S3
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
        self._pcf2_lift_service = PCF2LiftStageService(
            onedocker_binary_config_map=onedocker_binary_config_map,
            mpc_service=mpc_service,
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation compute metrics stage

        Args:
            pc_instance: the private computation instance to run compute metrics with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        # The only difference between PL on PCF 1.0 and PL on PCF 2.0 is the computation stage.
        # i.e. PCF 1.0 using compute_metrics_stage, while PCF 2.0 uses pcf2_lift_stage.
        # Adding this logic, to control which of the two stages to call based on the flag.

        # If the PRIVATE_LIFT_PCF2_RELEASE feature is present toggled on, then instead
        # of compute stage, return an instance of pcf2_lift stage.
        if pc_instance.has_feature(PCSFeature.PRIVATE_LIFT_PCF2_RELEASE):
            logging.info(
                "As private_lift_pcf2_release feature is enabled, running PCF2 lift stage, instead of compute stage."
            )
            return await self._pcf2_lift_service.run_async(
                pc_instance=pc_instance,
                server_certificate_provider=server_certificate_provider,
                ca_certificate_provider=ca_certificate_provider,
                server_certificate_path=server_certificate_path,
                ca_certificate_path=ca_certificate_path,
                server_ips=server_ips,
            )

        # Prepare arguments for lift game
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
            pc_instance.infra_config.game_type
        ).compute_stage
        binary_name = stage_data.binary_name
        game_name = checked_cast(str, stage_data.game_name)

        binary_config = self._onedocker_binary_config_map[binary_name]
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )

        _, cmd_args_list = self._mpc_service.convert_cmd_args_list(
            game_name=game_name,
            game_args=game_args,
            mpc_party=map_private_computation_role_to_mpc_party(
                pc_instance.infra_config.role
            ),
            server_ips=server_ips,
        )

        env_vars = generate_env_vars_dict(
            repository_path=binary_config.repository_path,
            server_certificate_provider=server_certificate_provider,
            server_certificate_path=server_certificate_path,
            ca_certificate_provider=ca_certificate_provider,
            ca_certificate_path=ca_certificate_path,
        )

        container_instances = await self._mpc_service.start_containers(
            cmd_args_list=cmd_args_list,
            onedocker_svc=self._mpc_service.onedocker_svc,
            binary_version=binary_config.binary_version,
            binary_name=binary_name,
            timeout=self._container_timeout,
            env_vars=env_vars,
            wait_for_containers_to_start_up=should_wait_spin_up,
            existing_containers=pc_instance.get_existing_containers_for_retry(),
        )
        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )

        logging.info("MPC instance started running.")
        pc_instance.infra_config.instances.append(stage_state)
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
        if pc_instance.has_feature(PCSFeature.PRIVATE_LIFT_PCF2_RELEASE):
            return self._pcf2_lift_service.get_status(pc_instance)

        return get_pc_status_from_stage_state(
            pc_instance, self._mpc_service.onedocker_svc
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._mpc_service.onedocker_svc)

    # TODO: Make an entity representation for game args that can dump a dict to pass
    # to mpc service. The entity will give us type checking and ensure that all args are
    # specified.
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

        common_compute_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "output_base_path": private_computation_instance.compute_stage_output_base_path,
            "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
            "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
            "run_id": private_computation_instance.infra_config.run_id,
        }
        if private_computation_instance.feature_flags is not None:
            common_compute_game_args[
                "pc_feature_flags"
            ] = private_computation_instance.feature_flags

        game_args = []

        # TODO: we eventually will want to get rid of the if-else here, which will be
        #   easy to do once the Lift and Attribution MPC compute games are consolidated
        if (
            private_computation_instance.infra_config.game_type
            is PrivateComputationGameType.ATTRIBUTION
        ):
            game_args = self._get_attribution_game_args(
                private_computation_instance,
                common_compute_game_args,
            )

        elif (
            private_computation_instance.infra_config.game_type
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
        """Gets lift specific game args to be passed to game binaries by onedocker

        When onedocker spins up containers to run games, it unpacks a dictionary containing the
        arguments required by the game binary being ran. This function prepares arguments specific to
        lift games.

        Args:
            pc_instance: the private computation instance to generate game args for

        Returns:
            MPC game args to be used by onedocker
        """
        game_args = [
            {
                **common_compute_game_args,
                **{
                    "file_start_index": i
                    * private_computation_instance.infra_config.num_files_per_mpc_container
                },
            }
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        return game_args

    def _get_attribution_game_args(
        self,
        private_computation_instance: PrivateComputationInstance,
        common_compute_game_args: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Gets attribution specific game args to be passed to game binaries by onedocker

        When onedocker spins up containers to run games, it unpacks a dictionary containing the
        arguments required by the game binary being ran. This function prepares arguments specific to
        attribution games.

        Args:
            pc_instance: the private computation instance to generate game args for

        Returns:
            MPC game args to be used by onedocker
        """
        game_args = []

        attribution_config: AttributionConfig = checked_cast(
            AttributionConfig,
            private_computation_instance.product_config,
        )
        attribution_rule: AttributionRule = attribution_config.attribution_rule

        aggregation_type: AggregationType = attribution_config.aggregation_type

        game_args = [
            {
                **common_compute_game_args,
                **{
                    "aggregators": aggregation_type.value,
                    "attribution_rules": attribution_rule.value,
                    "file_start_index": i
                    * private_computation_instance.infra_config.num_files_per_mpc_container,
                    "use_xor_encryption": True,
                    "run_name": private_computation_instance.infra_config.instance_id
                    if self._log_cost_to_s3
                    else "",
                    "max_num_touchpoints": private_computation_instance.product_config.common.padding_size,
                    "max_num_conversions": private_computation_instance.product_config.common.padding_size,
                },
            }
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        return game_args
