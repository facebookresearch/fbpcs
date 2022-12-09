#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from logging import Logger
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcs.private_computation.service.pre_validate_service import PreValidateService


class TestPreValidateService(IsolatedAsyncioTestCase):
    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.wait_for_containers_async"
    )
    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.start_containers"
    )
    async def test_run_pre_validate_async(
        self, start_containers_mock, wait_for_containers_async_mock
    ) -> None:
        self._setup_mocks_for_pre_validate_async()
        self.finished_container_mock.status = ContainerInstanceStatus.COMPLETED
        start_containers_return_value = [self.started_container_mock]
        start_containers_mock.return_value = start_containers_return_value
        wait_for_containers_async_mock.return_value = [self.finished_container_mock]
        expected_success_message = "".join(
            [
                "SUCCESS - All validation containers returned success.\n",
                "Container count: 1\n",
                f"Input paths: {self.input_paths}",
            ]
        )

        await PreValidateService.run_pre_validate_async(
            self.private_computation_service_mock, self.input_paths, self.logger_mock
        )

        wait_for_containers_async_mock.assert_called_once_with(
            self.onedocker_svc_mock, start_containers_return_value
        )
        self.logger_info_mock.assert_called_with(expected_success_message)

    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.wait_for_containers_async"
    )
    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.start_containers"
    )
    async def test_run_pre_validate_async_raises_an_error_when_container_fails(
        self, start_containers_mock, wait_for_containers_async_mock
    ) -> None:
        self._setup_mocks_for_pre_validate_async()
        self.finished_container_mock.status = ContainerInstanceStatus.FAILED
        start_containers_return_value = [self.started_container_mock]
        start_containers_mock.return_value = start_containers_return_value
        wait_for_containers_async_mock.return_value = [self.finished_container_mock]

        with self.assertRaisesRegex(
            Exception, r"ERROR - Number of containers that failed validation: 1\n.*"
        ):
            await PreValidateService.run_pre_validate_async(
                self.private_computation_service_mock,
                self.input_paths,
                self.logger_mock,
            )

    @patch(
        "fbpcs.private_computation.service.pre_validate_service.build_private_computation_service"
    )
    @patch(
        "fbpcs.private_computation.service.pre_validate_service.PreValidateService.run_pre_validate_async"
    )
    def test_pre_validate(
        self, run_pre_validate_async_mock, build_private_computation_service_mock
    ) -> None:
        logger_mock = Mock("logger")
        logger_mock.info = logger_mock
        input_paths = [
            "https://fb-pc-data-test-123.s3.us-west-2.amazonaws.com/query-results/input_file1.csv"
        ]
        config = {
            "private_computation": "private_computation0",
            "mpc": "mpc1",
            "pid": "pid2",
        }
        pc_service_mock = Mock("pc_service")
        build_private_computation_service_mock.return_value = pc_service_mock

        PreValidateService.pre_validate(config, input_paths, logger_mock)

        run_pre_validate_async_mock.assert_called_once_with(
            pc_service_mock, input_paths, logger_mock
        )

    def _setup_mocks_for_pre_validate_async(self):
        self.logger_mock = Mock(Logger)
        self.logger_info_mock = self.logger_mock.info
        self.logger_error_mock = self.logger_mock.error
        self.private_computation_service_mock = Mock("private_computation_service")
        self.onedocker_svc_mock = Mock("onedocker-service")
        self.onedocker_svc_mock.get_cluster = Mock(return_value="test_cluster_name")
        self.private_computation_service_mock.onedocker_svc = self.onedocker_svc_mock
        binary_config_mock = Mock("binary_config")
        binary_config_mock.repository_path = "test_repository_path"
        binary_config_mock.binary_version = "test_binary_version"
        self.private_computation_service_mock.onedocker_binary_config_map = {
            "validation/pc_pre_validation_cli": binary_config_mock
        }
        self.private_computation_service_mock.pc_validator_config = (
            self.private_computation_service_mock
        )
        self.private_computation_service_mock.region = "us-west-2"
        self.input_paths = [
            "https://fb-pc-data-test-123.s3.us-west-2.amazonaws.com/query-results/input_file1.csv"
        ]
        self.started_container_mock = Mock("started_container")
        self.finished_container_mock = Mock("finished_container")
        self.finished_container_mock.instance_id = "instance_id_123"
