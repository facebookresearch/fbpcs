#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import DefaultDict, List, Optional

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.private_key import PrivateKeyReferenceProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import DEFAULT_LOG_COST_TO_S3

from fbpcs.private_computation.service.mpc.mpc import (
    map_private_computation_role_to_mpc_party,
    MPCService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    gen_tls_server_hostnames_for_publisher,
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    stop_stage_service,
)


class PrivateIdDfcaAggregateStageService(PrivateComputationStageService):
    """Handles business logic for the Private ID DFCA aggregation stage

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

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
        server_hostnames: Optional[List[str]] = None,
        server_private_key_ref_provider: Optional[PrivateKeyReferenceProvider] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation aggregate metrics stage

        Args:
            pc_instance: the private computation instance to run aggregate metrics with.
            server_certificate_providder: A provider class to get TLS server certificate.
            ca_certificate_provider: A provider class to get TLS CA certificate.
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.
            server_hostnames: ignored
            server_private_key_ref_provider: ignored

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """

        binary_name = OneDockerBinaryNames.PRIVATE_ID_DFCA_AGGREGATOR.value
        binary_config = self._onedocker_binary_config_map[binary_name]

        if self._log_cost_to_s3:
            run_name = pc_instance.infra_config.instance_id

            log_name = "piddfca-logs"

            if pc_instance.product_config.common.post_processing_data:
                pc_instance.product_config.common.post_processing_data.s3_cost_export_output_paths.add(
                    f"{log_name}/{run_name}_{pc_instance.infra_config.role.value.title()}.json",
                )
        else:
            run_name = ""

        # Create and start MPC instances
        game_args = [
            {
                "input_path": f"{pc_instance.data_processing_output_path}_combine_{i}",
                "output_path": f"{pc_instance.private_id_dfca_aggregate_stage_output_path}_{i}",
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
                "run_id": pc_instance.infra_config.run_id,
            }
            for i in range(pc_instance.infra_config.num_mpc_containers)
        ]
        if pc_instance.feature_flags is not None:
            for arg in game_args:
                arg["pc_feature_flags"] = pc_instance.feature_flags

        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )

        _, cmd_args_list = self._mpc_service.convert_cmd_args_list(
            game_name=GameNames.PRIVATE_ID_DFCA_AGGREGATION.value,
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
        server_uris = gen_tls_server_hostnames_for_publisher(
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
