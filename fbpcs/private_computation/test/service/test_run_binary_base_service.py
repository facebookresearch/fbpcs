#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.error.pcp import PcpError
from fbpcp.service.onedocker import OneDockerService
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class TestRunBinaryBaseService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.container.ContainerService")
    def setUp(self, MockContainerService) -> None:
        self.container_svc = MockContainerService()
        self.onedocker_svc = OneDockerService(self.container_svc, "task_def")

    @mock.patch("fbpcp.service.onedocker.OneDockerService.wait_for_pending_containers")
    @mock.patch("fbpcp.service.onedocker.OneDockerService.start_containers")
    async def test_start_containers_success(
        self, start_containers_mock, wait_for_pending_containers_mock
    ) -> None:
        container_1_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.STARTED,
        )
        start_containers_mock.return_value = [container_1_start]
        wait_for_pending_containers_mock.return_value = [container_1_start]

        # ack
        containers = await RunBinaryBaseService().start_containers(
            cmd_args_list=["ls"],
            onedocker_svc=self.onedocker_svc,
            binary_version="latest",
            binary_name="test",
            timeout=10,
        )

        # asserts
        start_containers_mock.assert_called_once_with(
            package_name="test",
            version="latest",
            cmd_args_list=["ls"],
            timeout=10,
            env_vars=None,
        )
        self.assertEqual(containers, [container_1_start])

    @mock.patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.stop_containers"
    )
    @mock.patch("fbpcp.service.onedocker.OneDockerService.wait_for_pending_containers")
    @mock.patch("fbpcp.service.onedocker.OneDockerService.start_containers")
    async def test_start_containers_fail(
        self,
        start_containers_mock,
        wait_for_pending_containers_mock,
        stop_containers_mock,
    ) -> None:
        container_1_fail = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.FAILED,
        )
        start_containers_mock.return_value = [container_1_fail]
        wait_for_pending_containers_mock.return_value = [container_1_fail]

        # ack
        with self.assertRaises(RuntimeError) as cm:
            containers = await RunBinaryBaseService().start_containers(
                cmd_args_list=["ls"],
                onedocker_svc=self.onedocker_svc,
                binary_version="latest",
                binary_name="test",
                timeout=10,
            )

            self.assertEqual(containers, [container_1_fail])

        # asserts
        self.assertEqual(
            str(cm.exception),
            "One or more containers failed to stop. See the logs above to find the exact container_id",
        )
        start_containers_mock.assert_called_once_with(
            package_name="test",
            version="latest",
            cmd_args_list=["ls"],
            timeout=10,
            env_vars=None,
        )
        stop_containers_mock.assert_called_once_with(
            onedocker_svc=self.onedocker_svc, containers=[container_1_fail]
        )

    @mock.patch("fbpcp.service.onedocker.OneDockerService.stop_containers")
    def test_stop_containers_success(self, stop_containers_mock) -> None:
        container_1_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.STARTED,
        )
        container_2_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_2",
            "192.0.2.1",
            ContainerInstanceStatus.STARTED,
        )
        stop_containers_mock.return_value = [None, None]
        containers = [container_1_start, container_2_start]
        RunBinaryBaseService.stop_containers(self.onedocker_svc, containers)
        stop_containers_mock.assert_called_with(
            [
                "arn:aws:ecs:region:account_id:task/container_id_1",
                "arn:aws:ecs:region:account_id:task/container_id_2",
            ]
        )

    @mock.patch("fbpcp.service.onedocker.OneDockerService.stop_containers")
    def test_stop_containers_fail(self, stop_containers_mock) -> None:
        container_1_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.STARTED,
        )
        stop_containers_mock.return_value = [PcpError("Force Error")]
        containers = [container_1_start]
        with self.assertRaises(RuntimeError) as cm:
            RunBinaryBaseService.stop_containers(self.onedocker_svc, containers)

        self.assertEqual(
            str(cm.exception),
            "We encountered errors when stopping containers: [('arn:aws:ecs:region:account_id:task/container_id_1', PcpError('Force Error'))]",
        )
        stop_containers_mock.assert_called_with(
            ["arn:aws:ecs:region:account_id:task/container_id_1"]
        )

    @mock.patch("fbpcp.service.onedocker.OneDockerService.get_containers")
    async def test_wait_for_containers_success(self, get_containers) -> None:
        container_1_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.STARTED,
        )
        container_2_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_2",
            "192.0.2.1",
            ContainerInstanceStatus.STARTED,
        )

        container_1_complete = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.COMPLETED,
        )

        container_2_complete = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_2",
            "192.0.2.1",
            ContainerInstanceStatus.COMPLETED,
        )

        get_containers.side_effect = [
            [container_1_start],
            [container_1_complete],
            [container_2_complete],
        ]

        containers = [
            container_1_start,
            container_2_start,
        ]

        updated_containers = await RunBinaryBaseService.wait_for_containers_async(
            self.onedocker_svc, containers, poll=0
        )

        self.assertEqual(updated_containers[0], container_1_complete)
        self.assertEqual(updated_containers[1], container_2_complete)

    @mock.patch("fbpcp.service.onedocker.OneDockerService.get_containers")
    async def test_wait_for_containers_fail(self, get_containers) -> None:
        container_1_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.STARTED,
        )
        container_2_start = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_2",
            "192.0.2.1",
            ContainerInstanceStatus.STARTED,
        )

        container_1_complete = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_1",
            "192.0.2.0",
            ContainerInstanceStatus.COMPLETED,
        )

        container_2_fail = ContainerInstance(
            "arn:aws:ecs:region:account_id:task/container_id_2",
            "192.0.2.1",
            ContainerInstanceStatus.FAILED,
        )

        get_containers.side_effect = [
            [container_1_start],
            [container_1_complete],
            [container_2_fail],
        ]

        containers = [
            container_1_start,
            container_2_start,
        ]

        updated_containers = await RunBinaryBaseService.wait_for_containers_async(
            self.onedocker_svc, containers, poll=0
        )

        self.assertEqual(updated_containers[0], container_1_complete)
        self.assertEqual(updated_containers[1], container_2_fail)
