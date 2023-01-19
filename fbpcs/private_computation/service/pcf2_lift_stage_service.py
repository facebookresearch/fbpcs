#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Any, DefaultDict, Dict, List, Optional

from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.argument_helper import get_tls_arguments
from fbpcs.private_computation.service.constants import (
    DEFAULT_LOG_COST_TO_S3,
    DEFAULT_PADDING_SIZE,
    SERVER_HOSTNAME_ENV_VAR,
    SERVER_IP_ADDRESS_ENV_VAR,
)

from fbpcs.private_computation.service.mpc.mpc import MPCService

from fbpcs.private_computation.service.pcf2_base_stage_service import (
    PCF2BaseStageService,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_computation.service.utils import (
    distribute_files_among_containers,
    stop_stage_service,
)


class PCF2LiftStageService(PCF2BaseStageService):
    """Handles business logic for the private computation PCF2.0 Lift stage"""

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
        super().__init__(
            onedocker_binary_config_map=onedocker_binary_config_map,
            mpc_service=mpc_service,
            stage_data=PrivateComputationServiceData.PCF2_LIFT_STAGE_DATA,
            instance_id_suffix="_pcf2_lift",
            stage_name="Lift",
            padding_size=padding_size,
            log_cost_to_s3=log_cost_to_s3,
            container_timeout=container_timeout,
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._mpc_service.onedocker_svc)

    # TODO: Make an entity representation for game args that can dump a dict to pass
    # to mpc service. The entity will give us type checking and ensure that all args are
    # specified.
    def get_game_args(
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
