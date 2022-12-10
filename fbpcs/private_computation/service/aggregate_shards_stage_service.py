#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    AttributionConfig,
    ResultVisibility,
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
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    stop_stage_service,
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

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation aggregate metrics stage

        Args:
            pc_instance: the private computation instance to run aggregate metrics with.
            server_certificate_providder: A provider class to get TLS server certificate.
            ca_certificate_provider: A provider class to get TLS CA certificate.
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores an MPCInstance
        """
        binary_name = self.get_onedocker_binary_name(pc_instance)
        binary_config = self._onedocker_binary_config_map[binary_name]

        # Create and start MPC instance
        game_args = self.get_game_args(
            pc_instance, server_certificate_path, ca_certificate_path
        )
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )

        _, cmd_args_list = self._mpc_service.convert_cmd_args_list(
            game_name=self.get_game_name(pc_instance),
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
        pc_instance.infra_config.instances.append(stage_state)
        logging.info(
            f"MPC instance started running for game {self.get_game_name(pc_instance)}"
        )
        return pc_instance

    def get_game_args(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_path: str,
        ca_certificate_path: str,
    ) -> List[Dict[str, Any]]:
        if pc_instance.has_feature(PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS):
            num_shards = pc_instance.infra_config.num_secure_random_shards
        else:
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

        if self._log_cost_to_s3:
            run_name = pc_instance.infra_config.instance_id
            log_name = (
                "sc-logs"
                if pc_instance.has_feature(PCSFeature.SHARD_COMBINER_PCF2_RELEASE)
                else "sa-logs"
            )
            if pc_instance.product_config.common.post_processing_data:
                pc_instance.product_config.common.post_processing_data.s3_cost_export_output_paths.add(
                    f"{log_name}/{run_name}_{pc_instance.infra_config.role.value.title()}.json",
                )
        else:
            run_name = ""

        game_args = [
            {
                "input_base_path": self.get_input_stage_path(pc_instance),
                "metrics_format_type": metrics_format_type,
                "num_shards": num_shards,
                "output_path": self.get_output_path(pc_instance),
                "threshold": 0
                if isinstance(pc_instance.product_config, AttributionConfig)
                # pyre-ignore Undefined attribute [16]
                else pc_instance.product_config.k_anonymity_threshold,
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
                "log_cost_s3_bucket": pc_instance.infra_config.log_cost_bucket,
                "run_id": pc_instance.infra_config.run_id,
            },
        ]
        if pc_instance.feature_flags is not None:
            for arg in game_args:
                arg["pc_feature_flags"] = pc_instance.feature_flags

        # We should only export visibility to scribe when it's set
        if (
            pc_instance.product_config.common.result_visibility
            is not ResultVisibility.PUBLIC
        ):
            result_visibility = int(pc_instance.product_config.common.result_visibility)
            for arg in game_args:
                arg["visibility"] = result_visibility

        # remove shard_combiner_pcf2 unsupported arguments
        if pc_instance.has_feature(PCSFeature.SHARD_COMBINER_PCF2_RELEASE):
            arg.pop("run_id", None)
            arg.pop("pc_feature_flags", None)

        return game_args

    @classmethod
    def get_input_stage_path(cls, pc_instance: PrivateComputationInstance) -> str:
        # Get output path of previous stage depending on what stage flow we are using
        if pc_instance.get_flow_cls_name in [
            "PrivateComputationPCF2StageFlow",
            "PrivateComputationMRStageFlow",
            "PrivateComputationPCF2LocalTestStageFlow",
            "PrivateComputationPIDPATestStageFlow",
        ]:
            return pc_instance.pcf2_aggregation_stage_output_base_path
        elif pc_instance.get_flow_cls_name in [
            "PrivateComputationPCF2LiftStageFlow",
            "PrivateComputationPCF2LiftUDPStageFlow",
            "PrivateComputationPCF2LiftLocalTestStageFlow",
            "PrivateComputationMrPidPCF2LiftStageFlow",
        ]:
            return pc_instance.pcf2_lift_stage_output_base_path
        else:
            if pc_instance.has_feature(PCSFeature.PRIVATE_LIFT_PCF2_RELEASE):
                return pc_instance.pcf2_lift_stage_output_base_path
            else:
                return pc_instance.compute_stage_output_base_path

    @classmethod
    def get_output_path(cls, pc_instance: PrivateComputationInstance) -> str:
        if pc_instance.has_feature(PCSFeature.SHARD_COMBINER_PCF2_RELEASE):
            return pc_instance.pcf2_shard_combine_stage_output_path

        return pc_instance.shard_aggregate_stage_output_path

    @classmethod
    def get_game_name(cls, pc_instance: PrivateComputationInstance) -> str:
        if pc_instance.has_feature(PCSFeature.SHARD_COMBINER_PCF2_RELEASE):
            return GameNames.PCF2_SHARD_COMBINER.value

        return GameNames.SHARD_AGGREGATOR.value

    @classmethod
    def get_onedocker_binary_name(cls, pc_instance: PrivateComputationInstance) -> str:
        if pc_instance.has_feature(PCSFeature.SHARD_COMBINER_PCF2_RELEASE):
            return OneDockerBinaryNames.PCF2_SHARD_COMBINER.value

        return OneDockerBinaryNames.SHARD_AGGREGATOR.value

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
