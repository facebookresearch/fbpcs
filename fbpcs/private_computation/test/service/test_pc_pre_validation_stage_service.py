#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from typing import Optional, Set
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.container_permission import ContainerPermissionConfig
from fbpcp.entity.container_type import ContainerType
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationRole,
)

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)

from fbpcs.private_computation.service.pc_pre_validation_stage_service import (
    PCPreValidationStageService,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)
from fbpcs.private_computation.service.utils import generate_env_vars_dict


class TestPCPreValidationStageService(IsolatedAsyncioTestCase):
    def _get_infra_config(
        self,
        pcs_features: Optional[Set[PCSFeature]] = None,
        private_computation_role: PrivateComputationRole = PrivateComputationRole.PARTNER,
    ) -> InfraConfig:
        return InfraConfig(
            instance_id="123",
            role=private_computation_role,
            status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            pcs_features=pcs_features if pcs_features else {PCSFeature.UNKNOWN},
            status_updates=[],
            container_permission_id=self.container_permission_id,
        )

    def setUp(self) -> None:
        # create partner PrivateComputationInstance
        self.container_permission_id = "test-container-permission"
        self._infra_config: InfraConfig = self._get_infra_config()
        self._common: CommonProductConfig = CommonProductConfig(
            input_path="https://a-test-bucket.s3.us-west-2.amazonaws.com/lift/test/input_data1.csv",
            output_dir="789",
        )
        self._product_config: ProductConfig = LiftConfig(common=self._common)

        self._pc_instance = PrivateComputationInstance(
            infra_config=self._infra_config,
            product_config=self._product_config,
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )

        # create publisher PrivateComputationInstance
        infra_config_publisher: InfraConfig = InfraConfig(
            instance_id="123",
            role=PrivateComputationRole.PUBLISHER,
            status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            status_updates=[],
            container_permission_id=self.container_permission_id,
        )
        common_publisher: CommonProductConfig = CommonProductConfig(
            input_path="https://a-test-bucket.s3.us-west-2.amazonaws.com/lift/test/input_data1.csv",
            output_dir="789",
        )
        product_config_publisher: ProductConfig = LiftConfig(common=common_publisher)
        self._pc_instance_publisher = PrivateComputationInstance(
            infra_config=infra_config_publisher,
            product_config=product_config_publisher,
        )

    @patch.object(RunBinaryBaseService, "start_containers")
    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.StageStateInstance"
    )
    async def test_run_async_when_there_are_no_issues_running_onedocker_service(
        self, mock_stage_state_instance, mock_run_binary_base_service_start_containers
    ) -> None:
        pc_instance = self._pc_instance
        mock_container_instance = MagicMock()
        mock_onedocker_svc = MagicMock()
        mock_run_binary_base_service_start_containers.return_value = [
            mock_container_instance
        ]
        region = "us-west-1"
        expected_cmd_args = " ".join(
            [
                f"--input-file-path={self._pc_instance.product_config.common.input_path}",
                "--cloud-provider=AWS",
                f"--region={region}",
                "--binary-version=latest",
                f"--private-computation-role={PrivateComputationRole.PARTNER}",
            ]
        )
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )

        env_vars = generate_env_vars_dict(repository_path="test_path/")
        mock_run_binary_base_service_start_containers.assert_called_with(
            cmd_args_list=[expected_cmd_args],
            onedocker_svc=mock_onedocker_svc,
            binary_version="latest",
            binary_name=OneDockerBinaryNames.PC_PRE_VALIDATION.value,
            timeout=1200,
            env_vars=env_vars,
            wait_for_containers_to_start_up=True,
            existing_containers=None,
            container_type=ContainerType.LARGE,
            permission=ContainerPermissionConfig(self.container_permission_id),
        )

        mock_stage_state_instance.assert_called_with(
            self._pc_instance.infra_config.instance_id,
            self._pc_instance.current_stage.name,
            containers=[mock_container_instance],
        )
        self.assertEqual(
            pc_instance.infra_config.instances, [mock_stage_state_instance()]
        )

    @patch.object(RunBinaryBaseService, "start_containers")
    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.StageStateInstance"
    )
    async def test_run_async_when_there_are_no_issues_running_onedocker_service_publisher_role(
        self, mock_stage_state_instance, mock_run_binary_base_service_start_containers
    ) -> None:
        pc_instance = self._pc_instance
        infra_config: InfraConfig = self._get_infra_config(
            private_computation_role=PrivateComputationRole.PUBLISHER,
            pcs_features={
                PCSFeature.PRE_VALIDATION_FILE_STREAM,
                PCSFeature.PUBLISHER_PC_PRE_VALIDATION,
            },
        )
        pc_instance.infra_config = infra_config
        mock_container_instance = MagicMock()
        mock_onedocker_svc = MagicMock()
        mock_run_binary_base_service_start_containers.return_value = [
            mock_container_instance
        ]
        region = "us-west-1"
        expected_cmd_args = " ".join(
            [
                f"--input-file-path={self._pc_instance.product_config.common.input_path}",
                "--cloud-provider=AWS",
                f"--region={region}",
                "--binary-version=latest",
                f"--private-computation-role={PrivateComputationRole.PUBLISHER}",
                "--pre-validation-file-stream=enabled",
                "--publisher-pc-pre-validation=enabled",
            ]
        )
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
            pc_pre_validator_publisher_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )

        env_vars = generate_env_vars_dict(repository_path="test_path/")
        mock_run_binary_base_service_start_containers.assert_called_with(
            cmd_args_list=[expected_cmd_args],
            onedocker_svc=mock_onedocker_svc,
            binary_version="latest",
            binary_name=OneDockerBinaryNames.PC_PRE_VALIDATION.value,
            timeout=1200,
            env_vars=env_vars,
            wait_for_containers_to_start_up=False,
            existing_containers=None,
            container_type=ContainerType.LARGE,
            permission=ContainerPermissionConfig(self.container_permission_id),
        )

        mock_stage_state_instance.assert_called_with(
            self._pc_instance.infra_config.instance_id,
            self._pc_instance.current_stage.name,
            containers=[mock_container_instance],
        )
        self.assertEqual(
            pc_instance.infra_config.instances, [mock_stage_state_instance()]
        )

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pre_validator_is_not_enabled(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        expected_status = PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=False,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pc_pre_validator_publisher_enabled_is_not_enabled(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        infra_config: InfraConfig = self._get_infra_config(
            private_computation_role=PrivateComputationRole.PUBLISHER,
            pcs_features={
                PCSFeature.PRE_VALIDATION_FILE_STREAM,
                PCSFeature.PUBLISHER_PC_PRE_VALIDATION,
            },
        )
        pc_instance.infra_config = infra_config
        expected_status = PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
            pc_pre_validator_publisher_enabled=False,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
        self.assertFalse(
            stage_service._should_run_pre_validation(pc_instance=pc_instance)
        )
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pre_validator_is_enabled_but_the_role_is_publisher(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance_publisher
        expected_status = PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config,
            mock_onedocker_svc,
            self.onedocker_binary_config_map,
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )
        status = stage_service.get_status(pc_instance)
        mock_onedocker_svc.start_container.assert_not_called()
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    def test_get_status_returns_the_stage_status_from_stage_state(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        expected_status = PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )

        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )
        mock_get_pc_status_from_stage_state.side_effect = [expected_status]
        status = stage_service.get_status(pc_instance)

        self.assertEqual(status, expected_status)

    @patch.object(RunBinaryBaseService, "start_containers")
    async def test_run_async_it_can_raise_an_exception(
        self, mock_run_binary_base_service_start_containers
    ) -> None:
        pc_instance = self._pc_instance
        mock_onedocker_svc = MagicMock()
        exception_message = "Unexpected exception.."
        mock_run_binary_base_service_start_containers.side_effect = [
            RuntimeError(exception_message)
        ]
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        with self.assertRaisesRegex(RuntimeError, exception_message):
            await stage_service.run_async(
                pc_instance,
                NullCertificateProvider(),
                NullCertificateProvider(),
                "",
                "",
            )

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_get_status_logs_a_helpful_error_when_the_validation_fails(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        task_id = "test-task-id-123"
        cluster_name = "test-cluster-name"
        account_id = "1234567890"
        region = "us-west-1"
        instance_id = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{task_id}"
        container_instance = ContainerInstance(instance_id=instance_id)
        stage_state_instance = StageStateInstance(
            instance_id="instance-id-0",
            stage_name="PC_PRE_VALIDATION",
            containers=[container_instance],
        )
        unioned_pc_instances = [stage_state_instance]
        # pyre-ignore
        pc_instance.infra_config.instances = unioned_pc_instances
        expected_status = PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED
        onedocker_svc_mock = MagicMock()
        onedocker_svc_mock.get_cluster.side_effect = [cluster_name]
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
        )
        failed_task_link = f"https://{region}.console.aws.amazon.com/ecs/home?region={region}#/clusters/{cluster_name}/tasks/{task_id}/details"
        logger_mock = MagicMock()
        mock_get_pc_status_from_stage_state.side_effect = [expected_status]

        stage_service = PCPreValidationStageService(
            pc_validator_config, onedocker_svc_mock, self.onedocker_binary_config_map
        )
        stage_service._logger = logger_mock
        status = stage_service.get_status(pc_instance)

        self.assertEqual(status, expected_status)
        logger_mock.error.assert_called_with(
            f"[PCPreValidation] - stage failed because of some failed validations. Please check the logs in ECS for task id '{task_id}' to see the validation issues:\n"
            + f"Failed task link: {failed_task_link}"
        )

    @patch.object(RunBinaryBaseService, "start_containers")
    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.StageStateInstance"
    )
    async def test_run_async_when_file_stream_feature_is_enabled_passes_it_to_the_cli(
        self, mock_stage_state_instance, mock_run_binary_base_service_start_containers
    ) -> None:
        pc_instance = PrivateComputationInstance(
            infra_config=self._get_infra_config(
                {
                    PCSFeature.PRE_VALIDATION_FILE_STREAM,
                    PCSFeature.PUBLISHER_PC_PRE_VALIDATION,
                }
            ),
            product_config=self._product_config,
        )
        mock_container_instance = MagicMock()
        mock_onedocker_svc = MagicMock()
        mock_run_binary_base_service_start_containers.return_value = [
            mock_container_instance
        ]
        region = "us-west-1"
        expected_cmd_args = " ".join(
            [
                f"--input-file-path={pc_instance.product_config.common.input_path}",
                "--cloud-provider=AWS",
                f"--region={region}",
                "--binary-version=latest",
                f"--private-computation-role={PrivateComputationRole.PARTNER}",
                "--pre-validation-file-stream=enabled",
                "--publisher-pc-pre-validation=enabled",
            ]
        )
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(
            pc_instance, NullCertificateProvider(), NullCertificateProvider(), "", ""
        )

        env_vars = generate_env_vars_dict(repository_path="test_path/")
        mock_run_binary_base_service_start_containers.assert_called_with(
            cmd_args_list=[expected_cmd_args],
            onedocker_svc=mock_onedocker_svc,
            binary_version="latest",
            binary_name=OneDockerBinaryNames.PC_PRE_VALIDATION.value,
            timeout=1200,
            env_vars=env_vars,
            wait_for_containers_to_start_up=True,
            existing_containers=None,
            container_type=ContainerType.LARGE,
            permission=ContainerPermissionConfig(self.container_permission_id),
        )

        mock_stage_state_instance.assert_called_with(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=[mock_container_instance],
        )
        self.assertEqual(
            pc_instance.infra_config.instances, [mock_stage_state_instance()]
        )

    def test_should_run_pre_validation_gk_setting_publisher_role(self):
        pc_instance = self._pc_instance
        region = "us-west-1"
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
            pc_pre_validator_publisher_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )
        with self.subTest("publisher_gk_enabled"):
            infra_config: InfraConfig = self._get_infra_config(
                private_computation_role=PrivateComputationRole.PUBLISHER,
                pcs_features={
                    PCSFeature.PUBLISHER_PC_PRE_VALIDATION,
                },
            )
            pc_instance.infra_config = infra_config
            self.assertTrue(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )
        with self.subTest("publisher_gk_disabled"):
            infra_config: InfraConfig = self._get_infra_config(
                private_computation_role=PrivateComputationRole.PUBLISHER,
                pcs_features={},
            )
            pc_instance.infra_config = infra_config
            self.assertFalse(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )

    def test_should_run_pre_validation_pc_validator_config_flag_setting_publisher_role(
        self,
    ):
        pc_instance = self._pc_instance
        region = "us-west-1"
        mock_onedocker_svc = MagicMock()
        infra_config: InfraConfig = self._get_infra_config(
            private_computation_role=PrivateComputationRole.PUBLISHER,
            pcs_features={
                PCSFeature.PUBLISHER_PC_PRE_VALIDATION,
            },
        )
        pc_instance.infra_config = infra_config
        with self.subTest("pc_pre_validator_publisher_enabled"):
            pc_validator_config = PCValidatorConfig(
                region=region,
                pc_pre_validator_enabled=True,
                pc_pre_validator_publisher_enabled=True,
            )
            stage_service = PCPreValidationStageService(
                pc_validator_config,
                mock_onedocker_svc,
                self.onedocker_binary_config_map,
            )
            self.assertTrue(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )
        with self.subTest("pc_pre_validator_publisher_disabled"):
            pc_validator_config = PCValidatorConfig(
                region=region,
                pc_pre_validator_enabled=True,
                pc_pre_validator_publisher_enabled=False,
            )
            stage_service = PCPreValidationStageService(
                pc_validator_config,
                mock_onedocker_svc,
                self.onedocker_binary_config_map,
            )
            self.assertFalse(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )

        with self.subTest(
            "pc_pre_validator_publisher_disabled_pc_pre_validator_disabled"
        ):
            pc_validator_config = PCValidatorConfig(
                region=region,
                pc_pre_validator_enabled=False,
                pc_pre_validator_publisher_enabled=False,
            )
            stage_service = PCPreValidationStageService(
                pc_validator_config,
                mock_onedocker_svc,
                self.onedocker_binary_config_map,
            )
            self.assertFalse(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )

    def test_should_run_pre_validation_pc_validator_config_flag_setting_partner_role(
        self,
    ):
        pc_instance = self._pc_instance
        region = "us-west-1"
        mock_onedocker_svc = MagicMock()
        infra_config: InfraConfig = self._get_infra_config(
            private_computation_role=PrivateComputationRole.PARTNER,
            pcs_features={},
        )
        pc_instance.infra_config = infra_config
        with self.subTest("pc_pre_validator_enabled"):
            pc_validator_config = PCValidatorConfig(
                region=region,
                pc_pre_validator_enabled=True,
                pc_pre_validator_publisher_enabled=True,
            )
            stage_service = PCPreValidationStageService(
                pc_validator_config,
                mock_onedocker_svc,
                self.onedocker_binary_config_map,
            )
            self.assertTrue(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )
        with self.subTest("pc_pre_validator_disabled"):
            pc_validator_config = PCValidatorConfig(
                region=region,
                pc_pre_validator_enabled=False,
                pc_pre_validator_publisher_enabled=True,
            )
            stage_service = PCPreValidationStageService(
                pc_validator_config,
                mock_onedocker_svc,
                self.onedocker_binary_config_map,
            )
            self.assertFalse(
                stage_service._should_run_pre_validation(pc_instance=pc_instance)
            )
