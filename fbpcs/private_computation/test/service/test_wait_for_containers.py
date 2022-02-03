#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import mock, IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.onedocker import (
    OneDockerService,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class TestWaitForContainersAsync(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.container.ContainerService")
    def setUp(self, MockContainerService) -> None:
        self.container_svc = MockContainerService()
        self.onedocker_svc = OneDockerService(self.container_svc, "task_def")

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
