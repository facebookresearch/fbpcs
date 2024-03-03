#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
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


class PCF2LiftMetadataCompactionStageService(PCF2BaseStageService):
    """
    Handles business logic for the private computation PCF2.0 Metadata Compaction Stage (UDP for Lift)
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
        super().__init__(
            onedocker_binary_config_map=onedocker_binary_config_map,
            mpc_service=mpc_service,
            stage_data=PrivateComputationServiceData.PCF2_LIFT_METADATA_COMPACTION_DATA,
            instance_id_suffix="_pcf_lift_metadata_compaction",
            stage_name="Metadata Compaction",
            padding_size=padding_size,
            log_cost_to_s3=log_cost_to_s3,
            container_timeout=container_timeout,
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._mpc_service.onedocker_svc)

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
            pc_instance: the private computation instance to generate game args for
            server_certificate_path: The path to write server certificate on a container.
            ca_certificate_path: The path to write CA certificate on a container.

        Returns:
            MPC game args to be used by onedocker
        """

        # id_combiner_output_path = pc_instance.data_processing_output_path + "_combine"
        sharder_output_path = (
            private_computation_instance.secure_random_sharder_output_base_path
        )
        num_metadata_compaction_containers = (
            private_computation_instance.infra_config.num_udp_containers
        )
        output_global_params_base_path = (
            private_computation_instance.pcf2_lift_metadata_compaction_output_base_path
            + "_global_params"
        )
        output_secret_shares_base_path = (
            private_computation_instance.pcf2_lift_metadata_compaction_output_base_path
            + "_secret_shares"
        )

        run_name_base = (
            private_computation_instance.infra_config.instance_id
            + "_"
            + GameNames.PCF2_LIFT_METADATA_COMPACTION.value
        )

        tls_args = get_tls_arguments(
            private_computation_instance.infra_config.is_tls_enabled,
            server_certificate_path,
            ca_certificate_path,
        )

        num_secure_random_shards = (
            private_computation_instance.infra_config.num_secure_random_shards
        )
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
                "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
                "num_conversions_per_user": private_computation_instance.product_config.common.padding_size,
                "run_name": run_name,
                "log_cost": self._log_cost_to_s3,
                "log_cost_s3_bucket": private_computation_instance.infra_config.log_cost_bucket,
                # TODO T133330151 Add run_id support to PL UDP binary
                # "run_id": private_computation_instance.infra_config.run_id,
                **tls_args,
            }

            if private_computation_instance.feature_flags is not None:
                game_args["pc_feature_flags"] = (
                    private_computation_instance.feature_flags
                )

            if (
                private_computation_instance.product_config.common.post_processing_data
                and self._log_cost_to_s3
            ):
                private_computation_instance.product_config.common.post_processing_data.s3_cost_export_output_paths.add(
                    f"pl-logs/{run_name}_{private_computation_instance.infra_config.role.value.title()}.json"
                )

            cmd_args_list.append(game_args)
        return cmd_args_list
