#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.private_computation.service.mpc.entity.mpc_instance import (
    MPCInstance,
    MPCInstanceStatus,
    MPCParty,
)
from fbpcs.private_computation.service.mpc.mpc import MPCService


TEST_INSTANCE_ID = "123"
TEST_GAME_NAME = "lift"
TEST_MPC_ROLE = MPCParty.SERVER
TEST_NUM_WORKERS = 1
TEST_SERVER_IPS = ["192.0.2.0", "192.0.2.1"]
TEST_INPUT_ARGS = "test_input_file"
TEST_OUTPUT_ARGS = "test_output_file"
TEST_CONCURRENCY_ARGS = 1
TEST_INPUT_DIRECTORY = "TEST_INPUT_DIRECTORY/"
TEST_OUTPUT_DIRECTORY = "TEST_OUTPUT_DIRECTORY/"
TEST_TASK_DEFINITION = "test_task_definition"
INPUT_DIRECTORY = "input_directory"
OUTPUT_DIRECTORY = "output_directory"
GAME_ARGS = [
    {
        "input_filenames": TEST_INPUT_ARGS,
        "input_directory": TEST_INPUT_DIRECTORY,
        "output_filenames": TEST_OUTPUT_ARGS,
        "output_directory": TEST_OUTPUT_DIRECTORY,
        "concurrency": TEST_CONCURRENCY_ARGS,
    }
]


class TestMPCService(IsolatedAsyncioTestCase):
    def setUp(self):
        cspatcher = patch("fbpcp.service.container.ContainerService")
        irpatcher = patch(
            "fbpcs.private_computation.service.mpc.repository.mpc_instance.MPCInstanceRepository"
        )
        gspatcher = patch(
            "fbpcs.private_computation.service.mpc.mpc_game.MPCGameService"
        )
        container_svc = cspatcher.start()
        instance_repository = irpatcher.start()
        mpc_game_svc = gspatcher.start()
        for patcher in (cspatcher, irpatcher, gspatcher):
            self.addCleanup(patcher.stop)
        self.mpc_service = MPCService(
            container_svc,
            instance_repository,
            "test_task_definition",
            mpc_game_svc,
        )

    @staticmethod
    def _get_sample_mpcinstance():
        return MPCInstance(
            TEST_INSTANCE_ID,
            TEST_GAME_NAME,
            TEST_MPC_ROLE,
            TEST_NUM_WORKERS,
            TEST_SERVER_IPS,
            [],
            MPCInstanceStatus.CREATED,
            GAME_ARGS,
            [],
        )

    @staticmethod
    def _get_sample_mpcinstance_with_game_args():
        return MPCInstance(
            TEST_INSTANCE_ID,
            TEST_GAME_NAME,
            TEST_MPC_ROLE,
            TEST_NUM_WORKERS,
            TEST_SERVER_IPS,
            [],
            MPCInstanceStatus.CREATED,
            GAME_ARGS,
            [],
        )

    @staticmethod
    def _get_sample_mpcinstance_client():
        return MPCInstance(
            TEST_INSTANCE_ID,
            TEST_GAME_NAME,
            MPCParty.CLIENT,
            TEST_NUM_WORKERS,
            TEST_SERVER_IPS,
            [],
            MPCInstanceStatus.CREATED,
            GAME_ARGS,
            [],
        )

    async def test_spin_up_containers_onedocker_inconsistent_arguments(self):
        with self.assertRaisesRegex(
            ValueError,
            "The number of containers is not consistent with the number of game argument dictionary.",
        ):
            await self.mpc_service._spin_up_containers_onedocker(
                game_name=TEST_GAME_NAME,
                mpc_party=MPCParty.SERVER,
                num_containers=TEST_NUM_WORKERS,
                game_args=[],
            )

        with self.assertRaisesRegex(
            ValueError,
            "The number of containers is not consistent with number of ip addresses.",
        ):
            await self.mpc_service._spin_up_containers_onedocker(
                game_name=TEST_GAME_NAME,
                mpc_party=MPCParty.CLIENT,
                num_containers=TEST_NUM_WORKERS,
                ip_addresses=TEST_SERVER_IPS,
            )

    def test_create_instance_with_game_args(self):
        self.mpc_service.create_instance(
            instance_id=TEST_INSTANCE_ID,
            game_name=TEST_GAME_NAME,
            mpc_party=TEST_MPC_ROLE,
            num_workers=TEST_NUM_WORKERS,
            server_ips=TEST_SERVER_IPS,
            game_args=GAME_ARGS,
        )
        self.mpc_service.instance_repository.create.assert_called()
        self.assertEqual(
            self._get_sample_mpcinstance_with_game_args(),
            self.mpc_service.instance_repository.create.call_args[0][0],
        )

    def test_create_instance(self):
        self.mpc_service.create_instance(
            instance_id=TEST_INSTANCE_ID,
            game_name=TEST_GAME_NAME,
            mpc_party=TEST_MPC_ROLE,
            num_workers=TEST_NUM_WORKERS,
            server_ips=TEST_SERVER_IPS,
            game_args=GAME_ARGS,
        )
        # check that instance with correct instance_id was created
        self.mpc_service.instance_repository.create.assert_called()
        self.assertEqual(
            self._get_sample_mpcinstance(),
            self.mpc_service.instance_repository.create.call_args[0][0],
        )

    def _read_side_effect_start(self, instance_id: str):
        """mock MPCInstanceRepository.read for test_start"""
        if instance_id == TEST_INSTANCE_ID:
            return self._get_sample_mpcinstance()
        else:
            raise RuntimeError(f"{instance_id} does not exist")

    def test_start_instance(self):
        self.mpc_service.instance_repository.read = MagicMock(
            side_effect=self._read_side_effect_start
        )
        created_instances = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/57850450-7a81-43cc-8c73-2071c52e4a68",  # noqa
                "10.0.1.130",
                ContainerInstanceStatus.STARTED,
            )
        ]
        self.mpc_service.onedocker_svc.start_containers = MagicMock(
            return_value=created_instances
        )
        self.mpc_service.onedocker_svc.wait_for_pending_containers = AsyncMock(
            return_value=created_instances
        )
        built_onedocker_args = ("private_lift/lift", "test one docker arguments")
        self.mpc_service.mpc_game_svc.build_onedocker_args = MagicMock(
            return_value=built_onedocker_args
        )
        # check that update is called with correct status
        self.mpc_service.start_instance(TEST_INSTANCE_ID)
        self.mpc_service.instance_repository.update.assert_called()
        latest_update = self.mpc_service.instance_repository.update.call_args_list[-1]
        updated_status = latest_update[0][0].status
        self.assertEqual(updated_status, MPCInstanceStatus.STARTED)

    def test_start_instance_missing_ips(self):
        self.mpc_service.instance_repository.read = MagicMock(
            return_value=self._get_sample_mpcinstance_client()
        )
        # Exception because role is client but server ips are not given
        with self.assertRaises(ValueError):
            self.mpc_service.start_instance(TEST_INSTANCE_ID)

    def test_start_instance_skip_start_up(self):
        # prep
        self.mpc_service.instance_repository.read = MagicMock(
            side_effect=self._read_side_effect_start
        )
        created_instances = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/57850450-7a81-43cc-8c73-2071c52e4a68",  # noqa
                None,
                ContainerInstanceStatus.UNKNOWN,
            )
        ]
        self.mpc_service.onedocker_svc.start_containers = MagicMock(
            return_value=created_instances
        )
        self.mpc_service.onedocker_svc.wait_for_pending_containers = AsyncMock()
        built_onedocker_args = ("private_lift/lift", "test one docker arguments")
        self.mpc_service.mpc_game_svc.build_onedocker_args = MagicMock(
            return_value=built_onedocker_args
        )
        # check that update is called with correct status
        self.mpc_service.start_instance(
            instance_id=TEST_INSTANCE_ID, wait_for_containers_to_start_up=False
        )
        # asserts
        self.mpc_service.onedocker_svc.wait_for_pending_containers.assert_not_called()
        self.mpc_service.instance_repository.update.assert_called()
        latest_update = self.mpc_service.instance_repository.update.call_args_list[-1]
        updated_status = latest_update[0][0].status
        self.assertEqual(updated_status, MPCInstanceStatus.CREATED)

    def _read_side_effect_update(self, instance_id):
        """
        mock MPCInstanceRepository.read for test_update,
        with instance.containers is not None
        """
        if instance_id == TEST_INSTANCE_ID:
            mpc_instance = self._get_sample_mpcinstance()
        else:
            raise RuntimeError(f"{instance_id} does not exist")

        mpc_instance.status = MPCInstanceStatus.STARTED
        mpc_instance.containers = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/57850450-7a81-43cc-8c73-2071c52e4a68",  # noqa
                "10.0.1.130",
                ContainerInstanceStatus.STARTED,
            )
        ]
        return mpc_instance

    def test_update_instance(self):
        self.mpc_service.instance_repository.read = MagicMock(
            side_effect=self._read_side_effect_update
        )
        container_instances = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/cd34aed2-321f-49d1-8641-c54baff8b77b",  # noqa
                "10.0.1.130",
                ContainerInstanceStatus.STARTED,
            )
        ]
        self.mpc_service.container_svc.get_instances = MagicMock(
            return_value=container_instances
        )
        self.mpc_service.update_instance(TEST_INSTANCE_ID)
        self.mpc_service.instance_repository.update.assert_called()

    def test_update_instance_from_unknown(self):
        # prep
        mpc_instance = self._get_sample_mpcinstance()
        mpc_instance.containers = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/cd34aed2-321f-49d1-8641-c54baff8b77b",  # noqa
                None,
                ContainerInstanceStatus.UNKNOWN,
            )
        ]
        self.mpc_service.instance_repository.read = MagicMock(return_value=mpc_instance)
        updated_container_instances = [
            ContainerInstance(
                "arn:aws:ecs:us-west-1:592513842793:task/cd34aed2-321f-49d1-8641-c54baff8b77b",  # noqa
                "10.0.1.130",
                ContainerInstanceStatus.STARTED,
            )
        ]
        self.mpc_service.container_svc.get_instances = MagicMock(
            return_value=updated_container_instances
        )
        # act
        self.mpc_service.update_instance(TEST_INSTANCE_ID)
        # asserts
        self.mpc_service.instance_repository.update.assert_called()
        latest_update = self.mpc_service.instance_repository.update.call_args_list[-1]
        updated_instance = latest_update[0][0]
        self.assertEqual(updated_instance.status, MPCInstanceStatus.STARTED)
        self.assertEqual(updated_instance.server_ips, ["10.0.1.130"])

    def test_stop_instance(self):
        self.mpc_service.instance_repository.read = MagicMock(
            side_effect=self._read_side_effect_update
        )
        self.mpc_service.onedocker_svc.stop_containers = MagicMock(return_value=[None])
        mpc_instance = self.mpc_service.stop_instance(TEST_INSTANCE_ID)
        self.mpc_service.onedocker_svc.stop_containers.assert_called_with(
            [
                "arn:aws:ecs:us-west-1:592513842793:task/57850450-7a81-43cc-8c73-2071c52e4a68"
            ]
        )
        expected_mpc_instance = self._read_side_effect_update(TEST_INSTANCE_ID)
        expected_mpc_instance.status = MPCInstanceStatus.CANCELED
        self.assertEqual(expected_mpc_instance, mpc_instance)
        self.mpc_service.instance_repository.update.assert_called_with(
            expected_mpc_instance
        )

    def test_get_updated_instance(self):
        # Arrange
        queried_container = ContainerInstance(
            TEST_INSTANCE_ID,  # noqa
            TEST_SERVER_IPS[0],
            ContainerInstanceStatus.COMPLETED,
        )
        existing_container_started = ContainerInstance(
            TEST_INSTANCE_ID,
            TEST_SERVER_IPS[0],
            ContainerInstanceStatus.STARTED,
        )
        existing_container_completed = ContainerInstance(
            TEST_INSTANCE_ID,
            TEST_SERVER_IPS[0],
            ContainerInstanceStatus.COMPLETED,
        )

        # Act and Assert
        self.assertEqual(
            queried_container,
            self.mpc_service._get_updated_container(
                queried_container, existing_container_started
            ),
        )
        self.assertIsNone(
            self.mpc_service._get_updated_container(None, existing_container_started)
        )
        self.assertEqual(
            existing_container_completed,
            self.mpc_service._get_updated_container(None, existing_container_completed),
        )

    def test_get_containers_to_start_no_existing_containers(self) -> None:
        for num_containers in range(2):
            with self.subTest(num_containers=num_containers):
                containers_to_start = self.mpc_service.get_containers_to_start(
                    num_containers
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
                containers_to_start = self.mpc_service.get_containers_to_start(
                    len(existing_statuses),
                    [
                        ContainerInstance("id", "ip", status)
                        for status in existing_statuses
                    ],
                )
                self.assertEqual(containers_to_start, expected_result)

    def test_get_containers_to_start_invalid_args(self) -> None:
        # expect failure because num of command arguments != number existing containers
        with self.assertRaises(ValueError):
            self.mpc_service.get_containers_to_start(
                2,
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
                pending_containers = self.mpc_service.get_pending_containers(
                    new_pending_containers, containers_to_start, existing_containers
                )
                self.assertEqual(pending_containers, expected_containers)
