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
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.argument_helper import get_tls_arguments
from fbpcs.private_computation.service.constants import (
    DEFAULT_LOG_COST_TO_S3,
    DEFAULT_PADDING_SIZE,
)

from fbpcs.private_computation.service.mpc.mpc import (
    map_private_computation_role_to_mpc_party,
    MPCService,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    distribute_files_among_containers,
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    get_server_uris,
    stop_stage_service,
)


class PCF2LiftStageService(PrivateComputationStageService):
    """Handles business logic for the private computation PCF2.0 Lift stage

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
        padding_size: Optional[int] = DEFAULT_PADDING_SIZE[
            PrivateComputationGameType.LIFT
        ],
        log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
        container_timeout: Optional[int] = None,
    ) -> None:
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._mpc_service = mpc_service
        self.padding_size = padding_size
        self._log_cost_to_s3 = log_cost_to_s3
        self._container_timeout = container_timeout

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
        server_hostnames: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation PCF2.0 Lift stage

        Args:
            pc_instance: the private computation instance to run lift with.
            server_certificate_providder: A provider class to get TLS server certificate.
            ca_certificate_provider: A provider class to get TLS CA certificate.
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.
            server_hostnames: only used by the partner role. These are hostname addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        # Prepare arguments for lift game
        game_args = self._get_compute_metrics_game_args(
            pc_instance,
            server_certificate_path,
            ca_certificate_path,
        )

        # We do this check here because depends on how game_args is generated, len(game_args) could be different,
        #   but we will always expect server_ips == len(game_args)
        if server_ips and len(server_ips) != len(game_args):
            raise ValueError(
                f"Unable to rerun MPC compute because there is a mismatch between the number of server ips given ({len(server_ips)}) and the number of containers ({len(game_args)}) to be spawned."
            )

        # Create and start MPC instance to run MPC compute
        logging.info("Starting to run MPC instance for PCF2.0 Lift.")

        stage_data = PrivateComputationServiceData.PCF2_LIFT_STAGE_DATA
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

        # TODO: T141115702 - Update to use env var collection per container with distinct server addresses, once supported
        env_vars = generate_env_vars_dict(
            repository_path=binary_config.repository_path,
            server_certificate_provider=server_certificate_provider,
            server_certificate_path=server_certificate_path,
            ca_certificate_provider=ca_certificate_provider,
            ca_certificate_path=ca_certificate_path,
            server_ip_address=server_ips[0] if server_ips else None,
            server_hostname=server_hostnames[0] if server_hostnames else None,
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
        server_uris = get_server_uris(
            server_domain=pc_instance.infra_config.server_domain,
            role=pc_instance.infra_config.role,
            num_containers=len(cmd_args_list),
        )
        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
            server_uris=server_uris,
        )

        logging.info("MPC instance started running for PCF2.0 Lift.")
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
        server_certificate_path: str,
        ca_certificate_path: str,
    ) -> List[Dict[str, Any]]:
        """Gets the game args passed to game binaries by onedocker

        When onedocker spins up containers to run games, it unpacks a dictionary containing the
        arguments required by the game binary being ran. This function prepares that dictionary.

        Args:
            pc_instance: the private computation instance to generate game args for.
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.

        Returns:
            MPC game args to be used by onedocker
        """

        run_name_base = f"{private_computation_instance.infra_config.instance_id}_{GameNames.PCF2_LIFT.value}"

        tls_args = get_tls_arguments(
            private_computation_instance.has_feature(PCSFeature.PCF_TLS),
            server_certificate_path,
            ca_certificate_path,
        )

        if private_computation_instance.has_feature(
            PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS
        ):
            num_lift_containers = (
                private_computation_instance.infra_config.num_lift_containers
            )
            shards_per_file = distribute_files_among_containers(
                private_computation_instance.infra_config.num_secure_random_shards,
                num_lift_containers,
            )
        else:
            num_lift_containers = (
                private_computation_instance.infra_config.num_mpc_containers
            )

        cmd_args_list = []
        for shard in range(num_lift_containers):
            run_name = f"{run_name_base}_{shard}" if self._log_cost_to_s3 else ""

            game_args: Dict[str, Any] = {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.pcf2_lift_stage_output_base_path,
                "file_start_index": shard
                * private_computation_instance.infra_config.num_files_per_mpc_container,
                "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
                "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
                "num_conversions_per_user": private_computation_instance.product_config.common.padding_size,
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
                "run_id": private_computation_instance.infra_config.run_id,
                "log_cost_s3_bucket": private_computation_instance.infra_config.log_cost_bucket,
                **tls_args,
            }

            if private_computation_instance.feature_flags is not None:
                game_args[
                    "pc_feature_flags"
                ] = private_computation_instance.feature_flags

            if private_computation_instance.has_feature(
                PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS
            ):
                game_args["file_start_index"] = sum(shards_per_file[0:shard])
                game_args["num_files"] = shards_per_file[shard]
                game_args["input_base_path"] = (
                    private_computation_instance.pcf2_lift_metadata_compaction_output_base_path
                    + "_secret_shares"
                )
                game_args[
                    "input_global_params_path"
                ] = f"{private_computation_instance.pcf2_lift_metadata_compaction_output_base_path}_global_params_{shard}"

            if (
                self._log_cost_to_s3
                and private_computation_instance.product_config.common.post_processing_data
            ):
                private_computation_instance.product_config.common.post_processing_data.s3_cost_export_output_paths.add(
                    f"pl-logs/{run_name}_{private_computation_instance.infra_config.role.value.title()}.json"
                )

            cmd_args_list.append(game_args)

        return cmd_args_list
