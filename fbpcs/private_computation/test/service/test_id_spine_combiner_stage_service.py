#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from typing import Optional
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
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

        for test_run_id in (None, "2621fda2-0eca-11ed-861d-0242ac120002"):
            with self.subTest(test_run_id=test_run_id):
                private_computation_instance = self.create_sample_instance(test_run_id)

                with patch.object(
                    IdSpineCombinerService,
                    "start_containers",
                ) as mock_combine:
                    # call id_spine_combiner
                    pc_instance = await self.stage_svc.run_async(
                        private_computation_instance
                    )
                    mock_combine.assert_called()
                    self.assertEqual(pc_instance.infra_config.run_id, test_run_id)

    def create_sample_instance(
        self, test_run_id: Optional[str] = None
    ) -> PrivateComputationInstance:
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
            status_updates=[],
            run_id=test_run_id,
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
