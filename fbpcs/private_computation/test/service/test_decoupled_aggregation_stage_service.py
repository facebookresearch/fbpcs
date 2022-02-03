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
from fbpcs.private_computation.entity.private_computation_instance import (
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
    AggregationType,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import (
    NUM_NEW_SHARDS_PER_FILE,
)
from fbpcs.private_computation.service.decoupled_aggregation_stage_service import (
    AggregationStageService,
)
from fbpcs.private_computation.stage_flows.private_computation_decoupled_stage_flow import (
    PrivateComputationInstanceStatus,
)


class TestAggregationStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.mpc.MPCService")
    def setUp(self, mock_mpc_svc) -> None:
        self.mock_mpc_svc = mock_mpc_svc
        self.mock_mpc_svc.create_instance = MagicMock()

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/", binary_version="latest"
            )
        )
        self.stage_svc = AggregationStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_aggregation_stage(self) -> None:
        private_computation_instance = self._create_pc_instance()
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=private_computation_instance.instance_id
            + "_decoupled_aggregation0",
            game_name=GameNames.DECOUPLED_AGGREGATION.value,
            mpc_party=MPCParty.CLIENT,
            num_workers=private_computation_instance.num_mpc_containers,
        )

        self.mock_mpc_svc.start_instance_async = AsyncMock(return_value=mpc_instance)

        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.num_mpc_containers)
        ]
        await self.stage_svc.run_async(private_computation_instance, test_server_ips)

        self.assertEqual(mpc_instance, private_computation_instance.instances[0])

    def test_get_game_args(self) -> None:
        private_computation_instance = self._create_pc_instance()

        common_game_args = {
            "input_base_path": private_computation_instance.data_processing_output_path,
            "input_base_path_secret_share": private_computation_instance.decoupled_attribution_stage_output_base_path,
            "output_base_path": private_computation_instance.decoupled_aggregation_stage_output_base_path,
            "num_files": private_computation_instance.num_files_per_mpc_container,
            "concurrency": private_computation_instance.concurrency,
            "run_name": private_computation_instance.instance_id
            if self.stage_svc._log_cost_to_s3
            else "",
            "max_num_touchpoints": private_computation_instance.padding_size,
            "max_num_conversions": private_computation_instance.padding_size,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "attribution_rules": private_computation_instance.attribution_rule.value,
            # pyre-fixme[16]: Optional type has no attribute `value`.
            "aggregators": private_computation_instance.aggregation_type.value,
            "use_xor_encryption": True,
            "use_postfix": True,
        }
        test_game_args = [
            {
                **common_game_args,
                "file_start_index": 0,
            },
            {
                **common_game_args,
                "file_start_index": private_computation_instance.num_files_per_mpc_container,
            },
        ]

        actual_value = self.stage_svc._get_compute_metrics_game_args(
            private_computation_instance
        )
        self.assertEqual(
            test_game_args,
            actual_value,
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:

        return PrivateComputationInstance(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.DECOUPLED_ATTRIBUTION_COMPLETED,
            attribution_rule=AttributionRule.LAST_CLICK_1D,
            aggregation_type=AggregationType.MEASUREMENT,
            status_update_ts=1600000000,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            game_type=PrivateComputationGameType.ATTRIBUTION,
            input_path="456",
            output_dir="789",
            padding_size=4,
        )
