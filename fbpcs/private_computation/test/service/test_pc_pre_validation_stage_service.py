#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.pc_pre_validation_stage_service import (
    PCPreValidationStageService,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class TestPCPreValidationStageService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        infra_config: InfraConfig = InfraConfig(
            instance_id="123",
        )
        self._pc_instance = PrivateComputationInstance(
            infra_config=infra_config,
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
            status_update_ts=1600000000,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            game_type=PrivateComputationGameType.LIFT,
            input_path="https://a-test-bucket.s3.us-west-2.amazonaws.com/lift/test/input_data1.csv",
            output_dir="789",
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
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
                f"--input-file-path={self._pc_instance.input_path}",
                "--cloud-provider=AWS",
                f"--region={region}",
                "--binary-version=latest",
            ]
        )
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
        )
        stage_service = PCPreValidationStageService(
            pc_validator_config, mock_onedocker_svc, self.onedocker_binary_config_map
        )

        await stage_service.run_async(pc_instance)

        env_vars = {ONEDOCKER_REPOSITORY_PATH: "test_path/"}
        mock_run_binary_base_service_start_containers.assert_called_with(
            [expected_cmd_args],
            mock_onedocker_svc,
            "latest",
            OneDockerBinaryNames.PC_PRE_VALIDATION.value,
            timeout=1200,
            env_vars=env_vars,
        )

        mock_stage_state_instance.assert_called_with(
            self._pc_instance.infra_config.instance_id,
            self._pc_instance.current_stage.name,
            containers=[mock_container_instance],
        )
        self.assertEqual(pc_instance.instances, [mock_stage_state_instance()])

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

        await stage_service.run_async(pc_instance)
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.pc_pre_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pre_validator_is_enabled_but_the_role_is_publisher(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        pc_instance.role = PrivateComputationRole.PUBLISHER
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

        await stage_service.run_async(pc_instance)
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
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
            await stage_service.run_async(pc_instance)

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
            stage_name="stage-name-1",
            containers=[container_instance],
        )
        unioned_pc_instances = [stage_state_instance]
        # pyre-fixme[8]: Attribute has type `List[Union[StageStateInstance,
        #  PCSMPCInstance, PIDInstance, PostProcessingInstance]]`; used as
        #  `List[StageStateInstance]`.
        pc_instance.instances = unioned_pc_instances
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
