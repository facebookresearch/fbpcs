#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, patch

from fbpcp.entity.container_permission import ContainerPermissionConfig

from fbpcs.data_processing.service.sharding_service import ShardingService
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
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
from fbpcs.private_computation.service.shard_stage_service import ShardStageService


class TestShardStageService(IsolatedAsyncioTestCase):
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

        self.stage_svc = ShardStageService(
            self.onedocker_service, self.onedocker_binary_config_map
        )
        self.container_permission_id = "test-container-permission"

    async def test_reshard_data(self) -> None:
        private_computation_instance = self.create_sample_instance()

        with patch.object(
            ShardingService,
            "start_containers",
        ) as mock_shard:
            # call re-sharding
            await self.stage_svc.run_async(
                private_computation_instance,
                NullCertificateProvider(),
                NullCertificateProvider(),
                "",
                "",
            )

            # TODO: T149505024 - Assert specific container arguments expected from Shard stage
            mock_shard.assert_called_once_with(
                cmd_args_list=ANY,
                onedocker_svc=ANY,
                binary_version=ANY,
                binary_name=ANY,
                timeout=ANY,
                wait_for_containers_to_finish=ANY,
                env_vars=ANY,
                wait_for_containers_to_start_up=ANY,
                existing_containers=ANY,
                permission=ContainerPermissionConfig(self.container_permission_id),
            )

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
            status_updates=[],
            container_permission_id=self.container_permission_id,
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
