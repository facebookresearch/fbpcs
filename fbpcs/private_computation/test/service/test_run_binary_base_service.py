#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class TestRunBinaryBaseService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.container.ContainerService")
    def setUp(self, MockContainerService) -> None:
        self.container_svc = MockContainerService()
        self.onedocker_svc = OneDockerService(self.container_svc, "task_def")

    def test_get_containers_to_start_no_existing_containers(self) -> None:
        for num_containers in range(2):
            with self.subTest(num_containers=num_containers):
                containers_to_start = RunBinaryBaseService._get_containers_to_start(
                    ["arg"] * num_containers
                )
                self.assertEqual(containers_to_start, list(range(num_containers)))

    def test_get_containers_to_start_existing_containers(self) -> None:
        for existing_statuses in (
            (ContainerInstanceStatus.FAILED,),
            (
                ContainerInstanceStatus.FAILED,
                ContainerInstanceStatus.FAILED,
            ),
            (
                ContainerInstanceStatus.FAILED,
                ContainerInstanceStatus.COMPLETED,
            ),
            (
                ContainerInstanceStatus.STARTED,
                ContainerInstanceStatus.FAILED,
            ),
        ):
            # expect to start only the failed containers
            expected_result = [
                i
                for i, status in enumerate(existing_statuses)
                if status is ContainerInstanceStatus.FAILED
            ]
            with self.subTest(
                existing_statuses=existing_statuses, expected_result=expected_result
            ):
                containers_to_start = RunBinaryBaseService._get_containers_to_start(
                    ["arg"] * len(existing_statuses),
                    [
                        ContainerInstance("id", "ip", status)
                        for status in existing_statuses
                    ],
                )
                self.assertEqual(containers_to_start, expected_result)

    def test_get_containers_to_start_invalid_args(self) -> None:
        # expect failure because num of command arguments != number existing containers
        with self.assertRaises(ValueError):
            RunBinaryBaseService._get_containers_to_start(
                ["arg"] * 2,
                [
                    ContainerInstance("id", "ip", ContainerInstanceStatus.FAILED),
                ],
            )

    def test_get_pending_containers(self) -> None:
        for existing_statuses, containers_to_start in (
            # no existing containers case
            ((), [0, 1]),
            ((ContainerInstanceStatus.FAILED, ContainerInstanceStatus.FAILED), [0, 1]),
            ((ContainerInstanceStatus.FAILED, ContainerInstanceStatus.STARTED), [0]),
            ((ContainerInstanceStatus.STARTED, ContainerInstanceStatus.FAILED), [1]),
        ):
            existing_containers = [
                ContainerInstance(str(i), "ip", status)
                for i, status in enumerate(existing_statuses)
            ]
            new_pending_containers = [
                ContainerInstance(str(i), "ip", ContainerInstanceStatus.STARTED)
                for i in containers_to_start
            ]
            expected_containers = [
                ContainerInstance(str(i), "ip", ContainerInstanceStatus.STARTED)
                for i in range(2)
            ]

            with self.subTest(
                new_pending_containers=new_pending_containers,
                containers_to_start=containers_to_start,
                existing_containers=existing_containers,
            ):
                pending_containers = RunBinaryBaseService._get_pending_containers(
                    new_pending_containers, containers_to_start, existing_containers
                )
                self.assertEqual(pending_containers, expected_containers)

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
