#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import math
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.argument_helper import get_tls_arguments
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3

from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath
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


class SecureRandomShardStageService(PrivateComputationStageService):
    """Handles business logic for the SECURE_RANDOM_SHARDER stage

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

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the secure random shard stage service
        Args:
            pc_instance: the private computation instance to run secure random sharding with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """
        logging.info(f"[{self}] Starting Secure Random Sharding.")
        game_args = self._get_secure_random_sharder_args(
            pc_instance,
            server_certificate_path,
            ca_certificate_path,
        )

        if server_ips and len(server_ips) != len(game_args):
            raise ValueError(
                f"Unable to rerun secure random sharding compute because there is a mismatch between the number of server ips given ({len(server_ips)}) and the number of containers ({len(game_args)}) to be spawned."
            )

        logging.info(f"[{self}] Starting Secure Random Sharding.")

        stage_data = PrivateComputationServiceData.SECURE_RANDOM_SHARDER_DATA
        binary_name = stage_data.binary_name
        game_name = checked_cast(str, stage_data.game_name)

        binary_config = self._onedocker_binary_config_map[binary_name]
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )

        mpc_instance = await create_and_start_mpc_instance(
            mpc_svc=self._mpc_service,
            instance_id=pc_instance.infra_config.instance_id + "_secure_random_sharder",
            game_name=game_name,
            mpc_party=map_private_computation_role_to_mpc_party(
                pc_instance.infra_config.role
            ),
            num_containers=pc_instance.infra_config.num_pid_containers,
            binary_version=binary_config.binary_version,
            server_certificate_provider=server_certificate_provider,
            ca_certificate_provider=ca_certificate_provider,
            server_certificate_path=server_certificate_path,
            ca_certificate_path=ca_certificate_path,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=self._container_timeout,
            repository_path=binary_config.repository_path,
            wait_for_containers_to_start_up=should_wait_spin_up,
        )
        pc_instance.infra_config.instances.append(
            PCSMPCInstance.from_mpc_instance(mpc_instance)
        )
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Arguments:
            pc_instance: The private computation instance that is being updated

        Returns:
            The latest status for private computation instance
        """
        return get_updated_pc_status_mpc_game(pc_instance, self._mpc_service)

    def _get_secure_random_sharder_args(
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

        Returns:
            MPC game args to be used by onedocker
        """

        id_combiner_output_path = pc_instance.data_processing_output_path + "_combine"
        num_secure_random_sharder_containers = (
            pc_instance.infra_config.num_pid_containers
        )

        output_shards_base_path = pc_instance.secure_random_sharder_output_base_path

        tls_args = get_tls_arguments(
            pc_instance.has_feature(PCSFeature.PCF_TLS),
            server_certificate_path,
            ca_certificate_path,
        )

        shards_per_file = math.ceil(
            (
                pc_instance.infra_config.num_mpc_containers
                / pc_instance.infra_config.num_pid_containers
            )
            * pc_instance.infra_config.num_files_per_mpc_container
        )

        cmd_args_list = []
        for shard_index in range(num_secure_random_sharder_containers):
            path_to_input_shard = get_sharded_filepath(
                id_combiner_output_path, shard_index
            )
            args_per_shard: Dict[str, Any] = {
                "input_filename": path_to_input_shard,
                "output_base_path": output_shards_base_path,
                "file_start_index": shard_index * shards_per_file,
                "num_output_files": shards_per_file,
                # TODO T133330151 Add run_id support to PL UDP binary
                # "run_id": private_computation_instance.infra_config.run_id,
                **tls_args,
            }

            cmd_args_list.append(args_per_shard)
        return cmd_args_list
