#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus

from fbpcp.service.storage import StorageService
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.infra.certificate.private_key import StaticPrivateKeyReferenceProvider

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
from fbpcs.private_computation.service.constants import (
    CA_CERTIFICATE_ENV_VAR,
    CA_CERTIFICATE_PATH_ENV_VAR,
    NUM_NEW_SHARDS_PER_FILE,
    SERVER_CERTIFICATE_ENV_VAR,
    SERVER_CERTIFICATE_PATH_ENV_VAR,
    SERVER_PRIVATE_KEY_REF_ENV_VAR,
    SERVER_PRIVATE_KEY_REGION_ENV_VAR,
)
from fbpcs.private_computation.service.mpc.mpc import MPCService

from fbpcs.private_computation.service.secure_random_sharder_stage_service import (
    SecureRandomShardStageService,
)


class TestSecureRandomShardingStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.storage.StorageService")
    def setUp(self, mock_storage_svc: StorageService) -> None:
        self.mock_storage_svc = mock_storage_svc
        self.mock_mpc_svc = MagicMock(spec=MPCService)
        self.mock_mpc_svc.onedocker_svc = MagicMock()
        self.magic_mocks_read = []
        # normal case when intersection rate is over 1%, number of shards per file is determined by union_file_size
        self.magic_mocks_read.append(
            MagicMock(
                return_value=json.dumps(
                    {
                        "union_file_size": 1894,
                        "partner_input_size": 196,
                        "publisher_input_size": 1793,
                    }
                )
            )
        )
        # normal case when union_file_size is large and intersection rate is over 1%, number of shards per file is determined by union_file_size
        self.magic_mocks_read.append(
            MagicMock(
                return_value=json.dumps(
                    {
                        "union_file_size": 5569966,
                        "partner_input_size": 1057038,
                        "publisher_input_size": 4569271,
                    }
                )
            )
        )
        # edge case when intersection = 0
        self.magic_mocks_read.append(
            MagicMock(
                return_value=json.dumps(
                    {
                        "union_file_size": 1894,
                        "partner_input_size": 196,
                        "publisher_input_size": 1698,
                    }
                )
            )
        )
        # edge case when intersection rate is very low ( < 0.1%), number of shards per file is determined by intersection size and K_ANON
        self.magic_mocks_read.append(
            MagicMock(
                return_value=json.dumps(
                    {
                        "union_file_size": 386240,
                        "partner_input_size": 115872,
                        "publisher_input_size": 270538,
                    }
                )
            )
        )

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = SecureRandomShardStageService(
            self.mock_storage_svc,
            onedocker_binary_config_map,
            self.mock_mpc_svc,
        )

    async def test_run_async_with_udp(self) -> None:
        containers = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        self.mock_mpc_svc.start_containers.return_value = containers
        private_computation_instance = self._create_pc_instance()
        binary_name = "data_processing/secure_random_sharder"
        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_pid_containers)
        ]
        self.mock_mpc_svc.convert_cmd_args_list.return_value = (
            binary_name,
            ["cmd_1", "cmd_2"],
        )

        # act
        for magic_mock in self.magic_mocks_read:
            self.mock_mpc_svc.start_containers.reset_mock()
            self.mock_storage_svc.read = magic_mock
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
                "SECURE_RANDOM_RESHARDER",
                # pyre-ignore
                private_computation_instance.infra_config.instances[-1].stage_name,
            )

    async def test_get_game_args_with_secure_random_sharding(self) -> None:
        private_computation_instance = self._create_pc_instance()
        test_shards_per_file = [
            [1] * private_computation_instance.infra_config.num_pid_containers,
            [23] * private_computation_instance.infra_config.num_pid_containers,
            [1] * private_computation_instance.infra_config.num_pid_containers,
            [1] * private_computation_instance.infra_config.num_pid_containers,
        ]
        for i in range(len(self.magic_mocks_read)):
            self.mock_storage_svc.read = self.magic_mocks_read[i]
            test_game_args = [
                {
                    "input_filename": f"{private_computation_instance.data_processing_output_path}_combine_{j}",
                    "output_base_path": f"{private_computation_instance.secure_random_sharder_output_base_path}",
                    "file_start_index": sum(test_shards_per_file[i][0:j]),
                    "num_output_files": test_shards_per_file[i][j],
                    "use_tls": False,
                    "ca_cert_path": "",
                    "server_cert_path": "",
                    "private_key_path": "",
                }
                for j in range(
                    private_computation_instance.infra_config.num_pid_containers
                )
            ]
            self.assertEqual(
                test_game_args,
                await self.stage_svc._get_secure_random_sharder_args(
                    private_computation_instance, "", ""
                ),
            )

    async def test_get_union_stats(self) -> None:
        private_computation_instance = self._create_pc_instance()
        test_union_sizes = [
            [1894] * private_computation_instance.infra_config.num_pid_containers,
            [5569966] * private_computation_instance.infra_config.num_pid_containers,
            [1894] * private_computation_instance.infra_config.num_pid_containers,
            [386240] * private_computation_instance.infra_config.num_pid_containers,
        ]
        test_intersection_sizes = [
            [95] * private_computation_instance.infra_config.num_pid_containers,
            [56343] * private_computation_instance.infra_config.num_pid_containers,
            [0] * private_computation_instance.infra_config.num_pid_containers,
            [170] * private_computation_instance.infra_config.num_pid_containers,
        ]
        for i in range(len(self.magic_mocks_read)):
            self.mock_storage_svc.read = self.magic_mocks_read[i]
            union_sizes, intersection_sizes = await (
                self.stage_svc.get_union_stats(private_computation_instance)
            )
            self.assertEqual(test_union_sizes[i], union_sizes)
            self.assertEqual(test_intersection_sizes[i], intersection_sizes)

    async def test_get_dynamic_shards_num(self) -> None:
        private_computation_instance = self._create_pc_instance()
        test_shards_per_file = [
            [1] * private_computation_instance.infra_config.num_pid_containers,
            [23] * private_computation_instance.infra_config.num_pid_containers,
            [1] * private_computation_instance.infra_config.num_pid_containers,
            [1] * private_computation_instance.infra_config.num_pid_containers,
        ]
        for i in range(len(self.magic_mocks_read)):
            self.mock_storage_svc.read = self.magic_mocks_read[i]
            union_sizes, intersection_sizes = await (
                self.stage_svc.get_union_stats(private_computation_instance)
            )
            shards_per_file = self.stage_svc.get_dynamic_shards_num(
                union_sizes, intersection_sizes
            )
            self.assertEqual(test_shards_per_file[i], shards_per_file)

    async def test_setup_udp_lift_stages(self) -> None:
        test_num_lift_containers = [1, 2, 1, 1]
        test_num_udp_containers = [2, 46, 2, 2]
        for i in range(len(self.magic_mocks_read)):
            private_computation_instance = self._create_pc_instance()
            self.mock_storage_svc.read = self.magic_mocks_read[i]
            union_sizes, intersection_sizes = await (
                self.stage_svc.get_union_stats(private_computation_instance)
            )
            shards_per_file = self.stage_svc.get_dynamic_shards_num(
                union_sizes, intersection_sizes
            )
            self.stage_svc.setup_udp_lift_stages(
                private_computation_instance,
                union_sizes,
                intersection_sizes,
                shards_per_file,
            )

            self.assertEqual(
                test_num_lift_containers[i],
                private_computation_instance.infra_config.num_lift_containers,
            )
            self.assertEqual(
                test_num_udp_containers[i],
                private_computation_instance.infra_config.num_udp_containers,
            )

    async def test_tls_env_vars(self) -> None:
        self.mock_mpc_svc.start_containers.return_value = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        private_computation_instance = self._create_pc_instance()
        binary_name = "data_processing/secure_random_sharder"
        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_pid_containers)
        ]
        self.mock_mpc_svc.convert_cmd_args_list.return_value = (
            binary_name,
            ["cmd_1", "cmd_2"],
        )
        test_server_hostnames = [
            f"node{i}.test.com"
            for i in range(private_computation_instance.infra_config.num_pid_containers)
        ]

        expected_server_certificate = "test_server_cert"
        expected_ca_certificate = "test_ca_cert"
        expected_server_key_resource_id = "test_key"
        expected_server_key_region = "test_region"
        expected_server_certificate_path = "/test/server_certificate_path"
        expected_ca_certificate_path = "/test/server_certificate_path"

        # act
        for magic_mock in self.magic_mocks_read:
            self.mock_mpc_svc.start_containers.reset_mock()
            self.mock_storage_svc.read = magic_mock

            await self.stage_svc.run_async(
                private_computation_instance,
                self._get_mock_certificate_provider(expected_server_certificate),
                self._get_mock_certificate_provider(expected_ca_certificate),
                expected_server_certificate_path,
                expected_ca_certificate_path,
                test_server_ips,
                test_server_hostnames,
                StaticPrivateKeyReferenceProvider(
                    expected_server_key_resource_id, expected_server_key_region
                ),
            )

            # asserts
            self.mock_mpc_svc.start_containers.assert_called_once()
            call_kwargs = self.mock_mpc_svc.start_containers.call_args[1]
            call_env_args = call_kwargs["env_vars"]

            self.assertTrue(call_env_args)

            self.assertTrue("ONEDOCKER_REPOSITORY_PATH" in call_env_args)
            self.assertEqual("test_path/", call_env_args["ONEDOCKER_REPOSITORY_PATH"])

            self.assertTrue(SERVER_CERTIFICATE_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_server_certificate, call_env_args[SERVER_CERTIFICATE_ENV_VAR]
            )

            self.assertTrue(SERVER_PRIVATE_KEY_REF_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_server_key_resource_id,
                call_env_args[SERVER_PRIVATE_KEY_REF_ENV_VAR],
            )

            self.assertTrue(SERVER_PRIVATE_KEY_REGION_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_server_key_region,
                call_env_args[SERVER_PRIVATE_KEY_REGION_ENV_VAR],
            )

            self.assertTrue(CA_CERTIFICATE_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_ca_certificate, call_env_args[CA_CERTIFICATE_ENV_VAR]
            )

            self.assertTrue(SERVER_CERTIFICATE_PATH_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_server_certificate_path,
                call_env_args[SERVER_CERTIFICATE_PATH_ENV_VAR],
            )

            self.assertTrue(CA_CERTIFICATE_PATH_ENV_VAR in call_env_args)
            self.assertEqual(
                expected_ca_certificate_path, call_env_args[CA_CERTIFICATE_PATH_ENV_VAR]
            )

    def _create_pc_instance(self) -> PrivateComputationInstance:

        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            _stage_flow_cls_name="PrivateComputationPCF2LiftUDPStageFlow",
            status=PrivateComputationInstanceStatus.SECURE_RANDOM_SHARDER_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=4,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            log_cost_bucket="test_log_cost_bucket",
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

    def _get_mock_certificate_provider(self, certificate: str) -> MagicMock:
        certificate_provider = MagicMock()
        certificate_provider.get_certificate.return_value = certificate

        return certificate_provider
