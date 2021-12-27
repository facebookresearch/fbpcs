#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.data_processing.service.sharding_service import ShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.constants import (
    NUM_NEW_SHARDS_PER_FILE,
)
from fbpcs.private_computation.service.prepare_data_stage_service import (
    PrepareDataStageService,
)


class TestPrepareDataStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, onedocker_service):
        self.onedocker_service = onedocker_service
        self.test_num_containers = 2

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/", binary_version="latest"
            )
        )
        self.stage_svc = PrepareDataStageService(
            self.onedocker_service, self.onedocker_binary_config_map
        )

    async def test_prepare_data(self):
        private_computation_instance = self.create_sample_instance()

        with patch.object(
            IdSpineCombinerService,
            "start_containers",
        ) as mock_combine, patch.object(
            ShardingService,
            "start_containers",
        ) as mock_shard:
            # call prepare_data
            await self.stage_svc.run_async(private_computation_instance)

            binary_name = OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value
            binary_config = self.onedocker_binary_config_map[binary_name]
            args = IdSpineCombinerService.build_args(
                spine_path=private_computation_instance.pid_stage_output_spine_path,
                data_path=private_computation_instance.pid_stage_output_data_path,
                output_path=private_computation_instance.data_processing_output_path
                + "_combine",
                num_shards=self.test_num_containers,
                tmp_directory=binary_config.tmp_directory,
            )
            IdSpineCombinerService.start_containers(
                cmd_args_list=args,
                onedocker_svc=self.onedocker_service,
                binary_version=binary_config.binary_version,
                binary_name=binary_name,
                wait_for_containers_to_finish=True,
            )
            mock_combine.assert_called()
            mock_shard.assert_called()

    def create_sample_instance(self) -> PrivateComputationInstance:
        return PrivateComputationInstance(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
            status_update_ts=1600000000,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            game_type=PrivateComputationGameType.LIFT,
            input_path="456",
            output_dir="789",
        )
