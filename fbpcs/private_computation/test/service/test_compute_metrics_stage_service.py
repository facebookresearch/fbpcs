#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from collections import defaultdict
from typing import Set
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
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
from fbpcs.private_computation.service.compute_metrics_stage_service import (
    ComputeMetricsStageService,
)
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE
from fbpcs.private_computation.service.mpc.mpc import MPCService


class TestComputeMetricsStageService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_mpc_svc = MagicMock(spec=MPCService)
        self.mock_mpc_svc.onedocker_svc = MagicMock()
        self.run_id = "681ba82c-16d9-11ed-861d-0242ac120002"

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = ComputeMetricsStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_compute_metrics(self) -> None:
        containers = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        self.mock_mpc_svc.start_containers.return_value = containers

        for binary_name, pcs_feature_set in (
            (
                "private_lift/lift",
                {PCSFeature.PCS_DUMMY},
            ),
            (
                "private_lift/pcf2_lift",
                {PCSFeature.PRIVATE_LIFT_PCF2_RELEASE},
            ),
        ):
            with self.subTest(binary_name=binary_name, pcs_feature_set=pcs_feature_set):
                private_computation_instance = self._create_pc_instance(pcs_feature_set)
                test_server_ips = [
                    f"192.0.2.{i}"
                    for i in range(
                        private_computation_instance.infra_config.num_mpc_containers
                    )
                ]
                self.mock_mpc_svc.convert_cmd_args_list.return_value = (
                    binary_name,
                    ["cmd_1", "cmd_2"],
                )
                self.mock_mpc_svc.start_containers.reset_mock(return_value=False)
                # act
                await self.stage_svc.run_async(
                    private_computation_instance,
                    NullCertificateProvider(),
                    NullCertificateProvider(),
                    "",
                    "",
                    test_server_ips,
                )

                # asserts
                self.mock_mpc_svc.start_containers.assert_called_once_with(
                    cmd_args_list=["cmd_1", "cmd_2"],
                    onedocker_svc=self.mock_mpc_svc.onedocker_svc,
                    binary_version="latest",
                    binary_name=binary_name,
                    timeout=None,
                    env_vars={"ONEDOCKER_REPOSITORY_PATH": "test_path/"},
                    wait_for_containers_to_start_up=True,
                    existing_containers=None,
                )
                self.assertEqual(
                    containers,
                    # pyre-ignore
                    private_computation_instance.infra_config.instances[-1].containers,
                )
                self.assertEqual(
                    "COMPUTE",
                    # pyre-ignore
                    private_computation_instance.infra_config.instances[-1].stage_name,
                )

    def test_get_game_args(self) -> None:
        # TODO: add game args test for attribution args
        pcs_feature = {PCSFeature.PCS_DUMMY}
        private_computation_instance = self._create_pc_instance(pcs_feature)
        test_game_args = [
            {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.compute_stage_output_base_path,
                "file_start_index": 0,
                "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
                "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
                "run_id": self.run_id,
                "pc_feature_flags": PCSFeature.PCS_DUMMY.value,
            },
            {
                "input_base_path": private_computation_instance.data_processing_output_path,
                "output_base_path": private_computation_instance.compute_stage_output_base_path,
                "file_start_index": private_computation_instance.infra_config.num_files_per_mpc_container,
                "num_files": private_computation_instance.infra_config.num_files_per_mpc_container,
                "concurrency": private_computation_instance.infra_config.mpc_compute_concurrency,
                "run_id": self.run_id,
                "pc_feature_flags": PCSFeature.PCS_DUMMY.value,
            },
        ]

        self.assertEqual(
            test_game_args,
            self.stage_svc._get_compute_metrics_game_args(private_computation_instance),
        )

    def _create_pc_instance(
        self, pcs_features: Set[PCSFeature]
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            pcs_features=pcs_features,
            run_id=self.run_id,
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
