#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from fbpcp.entity.mpc_instance import MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE
from fbpcs.private_computation.service.pcf2_attribution_stage_service import (
    PCF2AttributionStageService,
)


class TestPCF2AttributionStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.mpc.MPCService")
    def setUp(self, mock_mpc_svc) -> None:
        self.mock_mpc_svc = mock_mpc_svc
        self.mock_mpc_svc.create_instance = MagicMock()

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = PCF2AttributionStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_attribution_stage(self) -> None:
        private_computation_instance = self._create_pc_instance()
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=private_computation_instance.infra_config.instance_id
            + "_pcf2_attribution0",
            game_name=GameNames.PCF2_ATTRIBUTION.value,
            mpc_party=MPCParty.CLIENT,
            num_workers=private_computation_instance.infra_config.num_mpc_containers,
        )

        self.mock_mpc_svc.start_instance_async = AsyncMock(return_value=mpc_instance)

        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        await self.stage_svc.run_async(private_computation_instance, test_server_ips)

        self.assertEqual(
            mpc_instance, private_computation_instance.infra_config.instances[0]
        )

    def test_get_game_args(self) -> None:
        private_computation_instance = self._create_pc_instance()

        common_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "output_base_path": private_computation_instance.pcf2_attribution_stage_output_base_path,
            "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
            "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
            "run_name": private_computation_instance.infra_config.instance_id
            + "_"
            + GameNames.PCF2_ATTRIBUTION.value
            if self.stage_svc._log_cost_to_s3
            else "",
            "max_num_touchpoints": private_computation_instance.product_config.common.padding_size,
            "max_num_conversions": private_computation_instance.product_config.common.padding_size,
            "attribution_rules": AttributionRule.LAST_CLICK_1D.value,
            "use_xor_encryption": True,
            "use_postfix": True,
            "log_cost": True,
        }
        test_game_args = [
            {
                **common_game_args,
                "file_start_index": 0,
            },
            {
                **common_game_args,
                "file_start_index": private_computation_instance.infra_config.num_files_per_mpc_container,
            },
        ]

        self.assertEqual(
            test_game_args,
            self.stage_svc._get_compute_metrics_game_args(private_computation_instance),
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.ATTRIBUTION,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
        )

        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
            padding_size=4,
        )
        product_config: ProductConfig = AttributionConfig(
            common=common,
            attribution_rule=AttributionRule.LAST_CLICK_1D,
            aggregation_type=AggregationType.MEASUREMENT,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
