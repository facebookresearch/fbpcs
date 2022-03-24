#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, MagicMock

from fbpcp.entity.container_instance import ContainerInstance
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.pc_validator_config import (
    PCValidatorConfig,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.input_data_validation_stage_service import (
    InputDataValidationStageService,
)


class TestInputDataValidationStageService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._pc_instance = PrivateComputationInstance(
            instance_id="123",
            role=PrivateComputationRole.PARTNER,
            instances=[],
            status=PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_STARTED,
            status_update_ts=1600000000,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            game_type=PrivateComputationGameType.LIFT,
            input_path="https://a-test-bucket.s3.us-west-2.amazonaws.com/lift/test/input_data1.csv",
            output_dir="789",
        )

    @patch(
        "fbpcs.private_computation.service.input_data_validation_stage_service.StageStateInstance"
    )
    async def test_run_async_when_there_are_no_issues_running_onedocker_service(
        self, mock_stage_state_instance
    ) -> None:
        pc_instance = self._pc_instance
        threshold_overrides = {
            "id_": 0.95,
            "value": 0.8,
        }
        threshold_overrides_str = json.dumps(threshold_overrides)
        mock_container_instance = MagicMock()
        mock_onedocker_svc = MagicMock()
        mock_onedocker_svc.start_container.side_effect = [mock_container_instance]
        region = "us-west-1"
        expected_cmd_args = " ".join(
            [
                f"--input-file-path={self._pc_instance.input_path}",
                "--cloud-provider=AWS",
                f"--region={region}",
                f"--valid-threshold-override='{threshold_overrides_str}'",
            ]
        )
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
            data_validation_threshold_overrides=threshold_overrides,
        )
        stage_service = InputDataValidationStageService(
            pc_validator_config, mock_onedocker_svc
        )

        await stage_service.run_async(pc_instance)

        mock_onedocker_svc.start_container.assert_called_with(
            package_name=OneDockerBinaryNames.PC_PRE_VALIDATION.value,
            timeout=1200,
            cmd_args=expected_cmd_args,
        )
        mock_stage_state_instance.assert_called_with(
            self._pc_instance.instance_id,
            self._pc_instance.current_stage.name,
            containers=[mock_container_instance],
        )
        self.assertEqual(pc_instance.instances, [mock_stage_state_instance()])

    @patch(
        "fbpcs.private_computation.service.input_data_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pre_validator_is_not_enabled(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        expected_status = (
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED
        )
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=False,
        )
        stage_service = InputDataValidationStageService(
            pc_validator_config, mock_onedocker_svc
        )

        await stage_service.run_async(pc_instance)
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.input_data_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_run_async_completes_when_the_pre_validator_is_enabled_but_the_role_is_publisher(
        self, mock_get_pc_status_from_stage_state
    ) -> None:
        pc_instance = self._pc_instance
        pc_instance.role = PrivateComputationRole.PUBLISHER
        expected_status = (
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED
        )
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )
        stage_service = InputDataValidationStageService(
            pc_validator_config, mock_onedocker_svc
        )

        await stage_service.run_async(pc_instance)
        status = stage_service.get_status(pc_instance)

        mock_onedocker_svc.start_container.assert_not_called()
        mock_get_pc_status_from_stage_state.assert_not_called()
        self.assertEqual(status, expected_status)

    @patch(
        "fbpcs.private_computation.service.input_data_validation_stage_service.get_pc_status_from_stage_state"
    )
    def test_get_status_returns_the_stage_status_from_stage_state(
        self, mock_get_pc_status_from_stage_state
    ):
        pc_instance = self._pc_instance
        expected_status = (
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED
        )
        mock_onedocker_svc = MagicMock()
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )

        stage_service = InputDataValidationStageService(
            pc_validator_config, mock_onedocker_svc
        )
        mock_get_pc_status_from_stage_state.side_effect = [expected_status]
        status = stage_service.get_status(pc_instance)

        self.assertEqual(status, expected_status)

    async def test_run_async_it_can_raise_an_exception(
        self,
    ) -> None:
        pc_instance = self._pc_instance
        mock_onedocker_svc = MagicMock()
        exception_message = "Unexpected exception.."
        mock_onedocker_svc.start_container.side_effect = [
            RuntimeError(exception_message)
        ]
        pc_validator_config = PCValidatorConfig(
            region="us-west-1",
            pc_pre_validator_enabled=True,
        )
        stage_service = InputDataValidationStageService(
            pc_validator_config, mock_onedocker_svc
        )

        with self.assertRaisesRegex(RuntimeError, exception_message):
            await stage_service.run_async(pc_instance)

    @patch(
        "fbpcs.private_computation.service.input_data_validation_stage_service.get_pc_status_from_stage_state"
    )
    async def test_get_status_logs_a_helpful_error_when_the_validation_fails(
        self, mock_get_pc_status_from_stage_state
    ):
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
        pc_instance.instances = unioned_pc_instances
        expected_status = PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_FAILED
        onedocker_svc_mock = MagicMock()
        container_svc_mock = MagicMock()
        container_svc_mock.get_cluster.side_effect = [cluster_name]
        onedocker_svc_mock.container_svc = container_svc_mock
        pc_validator_config = PCValidatorConfig(
            region=region,
            pc_pre_validator_enabled=True,
        )
        failed_task_link = f"https://{region}.console.aws.amazon.com/ecs/home?region={region}#/clusters/{cluster_name}/tasks/{task_id}/details"
        logger_mock = MagicMock()
        mock_get_pc_status_from_stage_state.side_effect = [expected_status]

        stage_service = InputDataValidationStageService(
            pc_validator_config, onedocker_svc_mock
        )
        stage_service._logger = logger_mock
        status = stage_service.get_status(pc_instance)

        self.assertEqual(status, expected_status)
        logger_mock.error.assert_called_with(
            f"[PCPreValidation] - stage failed because of some failed validations. Please check the logs in ECS for task id '{task_id}' to see the validation issues:\n"
            + f"Failed task link: {failed_task_link}"
        )
