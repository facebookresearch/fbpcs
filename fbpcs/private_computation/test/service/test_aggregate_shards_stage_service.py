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
    AttributionConfig,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE


class TestAggregateShardsStageService(IsolatedAsyncioTestCase):
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
        self.stage_svc = AggregateShardsStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_aggregate_shards(self) -> None:
        private_computation_instance = self._create_pc_instance()
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=private_computation_instance.infra_config.instance_id
            + "_aggregate_metrics0",
            game_name=GameNames.LIFT.value,
            mpc_party=MPCParty.CLIENT,
            num_workers=private_computation_instance.infra_config.num_mpc_containers,
        )

        self.mock_mpc_svc.start_instance_async = AsyncMock(return_value=mpc_instance)

        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        await self.stage_svc.run_async(private_computation_instance, test_server_ips)
        test_game_args = [
            {
                "input_base_path": private_computation_instance.compute_stage_output_base_path,
                "metrics_format_type": "lift",
                "num_shards": private_computation_instance.infra_config.num_mpc_containers
                * NUM_NEW_SHARDS_PER_FILE,
                "output_path": private_computation_instance.shard_aggregate_stage_output_path,
                "threshold": 0
                if isinstance(
                    private_computation_instance.product_config, AttributionConfig
                )
                # pyre-ignore Undefined attribute [16]
                else private_computation_instance.product_config.k_anonymity_threshold,
                "run_name": private_computation_instance.infra_config.instance_id
                if self.stage_svc._log_cost_to_s3
                else "",
                "log_cost": True,
            }
        ]

        self.assertEqual(
            GameNames.SHARD_AGGREGATOR.value,
            self.mock_mpc_svc.create_instance.call_args[1]["game_name"],
        )
        self.assertEqual(
            test_game_args,
            self.mock_mpc_svc.create_instance.call_args[1]["game_args"],
        )

        self.assertEqual(
            mpc_instance, private_computation_instance.infra_config.instances[0]
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
