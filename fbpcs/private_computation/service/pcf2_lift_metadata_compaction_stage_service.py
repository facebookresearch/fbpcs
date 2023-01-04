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


class PCF2LiftMetadataCompactionStageService(PrivateComputationStageService):
    """
    Handles business logic for the private computation PCF2.0 Metadata Compaction Stage (UDP for Lift)

    Private attributed:
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
        """
        Args:
            pc_instance: the private computation instance to run lift with.
            server_certificate_providder: A provider class to get TLS server certificate.
            ca_certificate_provider: A provider class to get TLS CA certificate.
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.
            server_hostnames: ignored, TODO: T141115702 - configure hostname for TLS when supported by env vars

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        game_args = self._get_lift_metadata_compaction_game_args(
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

        logging.info(
            "Starting to run MPC instance for PCF2.0 Lift Metadata Compaction."
        )

        stage_data = PrivateComputationServiceData.PCF2_LIFT_METADATA_COMPACTION_DATA
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

        server_uris = get_server_uris(
            server_domain=pc_instance.infra_config.server_domain,
            role=pc_instance.infra_config.role,
            num_containers=len(cmd_args_list),
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
            server_uris=server_uris,
        )
        pc_instance.infra_config.instances.append(stage_state)
        logging.info(
            "MPC instance started running for PCF2.0 Lift Metadata Compaction."
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
        return get_pc_status_from_stage_state(
            pc_instance, self._mpc_service.onedocker_svc
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._mpc_service.onedocker_svc)

    def _get_lift_metadata_compaction_game_args(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_path: str,
        ca_certificate_path: str,
    ) -> List[Dict[str, Any]]:
        """Gets the game args passed to game binaries by onedocker

        When onedocker spins up containers to run games, it unpacks a dictionary containing the
        arguments required by the game binary being ran. This function prepares that dictionary.

        Args:
            pc_instance: the private computation instance to generate game args for
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.

        Returns:
            MPC game args to be used by onedocker
        """

        # id_combiner_output_path = pc_instance.data_processing_output_path + "_combine"
        sharder_output_path = pc_instance.secure_random_sharder_output_base_path
        num_metadata_compaction_containers = pc_instance.infra_config.num_udp_containers
        output_global_params_base_path = (
            pc_instance.pcf2_lift_metadata_compaction_output_base_path
            + "_global_params"
        )
        output_secret_shares_base_path = (
            pc_instance.pcf2_lift_metadata_compaction_output_base_path
            + "_secret_shares"
        )

        run_name_base = (
            pc_instance.infra_config.instance_id
            + "_"
            + GameNames.PCF2_LIFT_METADATA_COMPACTION.value
        )

        tls_args = get_tls_arguments(
            pc_instance.has_feature(PCSFeature.PCF_TLS),
            server_certificate_path,
            ca_certificate_path,
        )

        num_secure_random_shards = pc_instance.infra_config.num_secure_random_shards
        shards_per_container = distribute_files_among_containers(
            num_secure_random_shards, num_metadata_compaction_containers
        )
        cmd_args_list = []
        for shard in range(num_metadata_compaction_containers):
            logging.info(
                f"[{self}] {shard}-th metadata_compaction_containers stats: shards_per_container is {shards_per_container[shard]}"
            )
            run_name = f"{run_name_base}_{shard}" if self._log_cost_to_s3 else ""
            game_args: Dict[str, Any] = {
                "input_base_path": sharder_output_path,
                "output_global_params_base_path": output_global_params_base_path,
                "output_secret_shares_base_path": output_secret_shares_base_path,
                "file_start_index": sum(shards_per_container[0:shard]),
                "num_files": shards_per_container[shard],
                "concurrency": pc_instance.infra_config.mpc_compute_concurrency,
                "num_conversions_per_user": pc_instance.product_config.common.padding_size,
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
                "log_cost_s3_bucket": pc_instance.infra_config.log_cost_bucket,
                # TODO T133330151 Add run_id support to PL UDP binary
                # "run_id": private_computation_instance.infra_config.run_id,
                **tls_args,
            }
            if pc_instance.feature_flags is not None:
                game_args["pc_feature_flags"] = pc_instance.feature_flags

            if (
                pc_instance.product_config.common.post_processing_data
                and self._log_cost_to_s3
            ):
                pc_instance.product_config.common.post_processing_data.s3_cost_export_output_paths.add(
                    f"pl-logs/{run_name}_{pc_instance.infra_config.role.value.title()}.json"
                )

            cmd_args_list.append(game_args)
        return cmd_args_list
