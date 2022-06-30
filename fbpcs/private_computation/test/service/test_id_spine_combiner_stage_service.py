#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE
from fbpcs.private_computation.service.id_spine_combiner_stage_service import (
    IdSpineCombinerStageService,
)


class TestIdSpineCombinerStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, onedocker_service) -> None:
        self.onedocker_service = onedocker_service
        self.test_num_containers = 2

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = IdSpineCombinerStageService(
            self.onedocker_service, self.onedocker_binary_config_map
        )

    async def test_id_spine_combiner(self) -> None:
        private_computation_instance = self.create_sample_instance()

        with patch.object(
            IdSpineCombinerService,
            "start_containers",
        ) as mock_combine:
            # call id_spine_combiner
            await self.stage_svc.run_async(private_computation_instance)
            mock_combine.assert_called()

    def create_sample_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
        )
        common_product_config: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
