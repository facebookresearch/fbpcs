#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from typing import Optional
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, patch

from fbpcp.entity.container_permission import ContainerPermissionConfig

from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService

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
from fbpcs.private_computation.service.id_spine_combiner_stage_service import (
    IdSpineCombinerStageService,
)


class TestIdSpineCombinerStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, onedocker_service, storage_svc) -> None:
        self.storage_svc = storage_svc
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
            self.storage_svc, self.onedocker_service, self.onedocker_binary_config_map
        )
        self.container_permission_id = "test-container-permission"

    async def test_id_spine_combiner(self) -> None:

        for test_run_id, test_log_cost_bucket in (
            (None, "test-log-bucket"),
            ("2621fda2-0eca-11ed-861d-0242ac120002", "test-log-bucket"),
        ):
            with self.subTest(
                test_run_id=test_run_id, test_log_cost_bucket=test_log_cost_bucket
            ):
                private_computation_instance = self.create_sample_instance(test_run_id)

                with patch.object(
                    IdSpineCombinerService,
                    "start_containers",
                ) as mock_combine:
                    # call id_spine_combiner
                    pc_instance = await self.stage_svc.run_async(
                        private_computation_instance,
                        NullCertificateProvider(),
                        NullCertificateProvider(),
                        "",
                        "",
                    )

                    # TODO: T149505091 - Assert specific container arguments expected during this stage
                    mock_combine.assert_called_once_with(
                        cmd_args_list=ANY,
                        onedocker_svc=ANY,
                        binary_version=ANY,
                        binary_name=ANY,
                        timeout=ANY,
                        wait_for_containers_to_finish=ANY,
                        env_vars=ANY,
                        wait_for_containers_to_start_up=ANY,
                        existing_containers=ANY,
                        container_type=ANY,
                        permission=ContainerPermissionConfig(
                            self.container_permission_id
                        ),
                    )
                    self.assertEqual(pc_instance.infra_config.run_id, test_run_id)
                    self.assertEqual(
                        pc_instance.infra_config.log_cost_bucket, test_log_cost_bucket
                    )

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
            log_cost_bucket="test-log-bucket",
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
