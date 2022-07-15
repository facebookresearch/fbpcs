#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import unittest
from typing import Dict, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.data_processing.pid_preparer.union_pid_preparer_cpp import (
    CppUnionPIDDataPreparerService,
)
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.pid_service.pid_prepare_stage import PIDPrepareStage
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


def data_test_run() -> Tuple[
    Dict[str, bool],
    Dict[str, bool],
]:
    return ({"wait_for_containers": True}, {"wait_for_containers": False})


class TestPIDPrepareStage(unittest.TestCase):
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_prepare(
        self,
        mock_instance_repo: unittest.mock.MagicMock,
    ) -> None:
        async def _run_sub_test(
            wait_for_containers: bool,
            expected_container_status: ContainerInstanceStatus,
        ) -> None:
            with patch.object(
                CppUnionPIDDataPreparerService, "prepare_on_container_async"
            ) as mock_prepare_on_container_async, patch.object(
                PIDStage, "update_instance_containers"
            ):
                container = ContainerInstance(
                    instance_id="123",
                    ip_address="192.0.2.0",
                    status=expected_container_status,
                )
                mock_prepare_on_container_async.return_value = container
                stage = PIDPrepareStage(
                    stage=UnionPIDStage.PUBLISHER_PREPARE,
                    instance_repository=mock_instance_repo,
                    storage_svc="STORAGE",  # pyre-ignore
                    onedocker_svc="ONEDOCKER",  # pyre-ignore
                    onedocker_binary_config=MagicMock(
                        task_definition="offline-task:1#container",
                        tmp_directory="/tmp/",
                        binary_version="latest",
                    ),
                )

                res = await stage.prepare(
                    instance_id="123",
                    input_path="in",
                    output_path="out",
                    num_shards=1,
                    wait_for_containers=wait_for_containers,
                )
                self.assertEqual(
                    PIDStage.get_stage_status_from_containers([container]),
                    res,
                )

        data_tests = (
            (True, ContainerInstanceStatus.COMPLETED),
            (True, ContainerInstanceStatus.FAILED),
            (False, ContainerInstanceStatus.STARTED),
        )
        for data_test in data_tests:
            with self.subTest(
                wait_for_containers=data_test[0],
                expected_container_status=data_test[1],
            ):
                # reset mocks for each subTests
                mock_instance_repo.reset_mock()
                await _run_sub_test(
                    data_test[0],
                    data_test[1],
                )

    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.wait_for_containers_async"
    )
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @patch("fbpcp.service.onedocker.OneDockerService")
    async def test_run(
        self,
        mock_onedocker_svc: unittest.mock.MagicMock,
        mock_instance_repo: unittest.mock.MagicMock,
        mock_storage_svc: unittest.mock.MagicMock,
        mock_wait_for_containers_async: unittest.mock.MagicMock,
    ) -> None:
        async def _run_sub_test(
            wait_for_containers: bool,
        ) -> None:
            ip = "192.0.2.0"
            container = ContainerInstance(
                instance_id="123", ip_address=ip, status=ContainerInstanceStatus.STARTED
            )

            mock_onedocker_svc.start_containers = MagicMock(return_value=[container])
            mock_onedocker_svc.wait_for_pending_containers = AsyncMock(
                return_value=[container]
            )

            container.status = (
                ContainerInstanceStatus.COMPLETED
                if wait_for_containers
                else ContainerInstanceStatus.STARTED
            )
            mock_wait_for_containers_async.return_value = [container]

            stage = PIDPrepareStage(
                stage=UnionPIDStage.PUBLISHER_PREPARE,
                instance_repository=mock_instance_repo,
                storage_svc=mock_storage_svc,
                onedocker_svc=mock_onedocker_svc,
                onedocker_binary_config=MagicMock(
                    task_definition="offline-task:1#container",
                    tmp_directory="/tmp/",
                    binary_version="latest",
                ),
            )
            instance_id = "444"
            stage_input = PIDStageInput(
                input_paths=["in"],
                output_paths=["out"],
                num_shards=2,
                instance_id=instance_id,
            )

            # Basic test: All good
            with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
                mock_fe.return_value = True
                stage = PIDPrepareStage(
                    stage=UnionPIDStage.PUBLISHER_PREPARE,
                    instance_repository=mock_instance_repo,
                    storage_svc=mock_storage_svc,
                    onedocker_svc=mock_onedocker_svc,
                    onedocker_binary_config=MagicMock(
                        task_definition="offline-task:1#container",
                        tmp_directory="/tmp/",
                        binary_version="latest",
                    ),
                )
                status = await stage.run(
                    stage_input, wait_for_containers=wait_for_containers
                )
                self.assertEqual(
                    PIDStageStatus.COMPLETED
                    if wait_for_containers
                    else PIDStageStatus.STARTED,
                    status,
                )

                self.assertEqual(mock_onedocker_svc.start_containers.call_count, 2)
                if wait_for_containers:
                    self.assertEqual(mock_wait_for_containers_async.call_count, 2)
                else:
                    mock_wait_for_containers_async.assert_not_called()
                mock_instance_repo.read.assert_called_with(instance_id)
                self.assertEqual(mock_instance_repo.read.call_count, 4)
                self.assertEqual(mock_instance_repo.update.call_count, 4)

            with patch.object(PIDPrepareStage, "files_exist") as mock_fe, patch.object(
                PIDPrepareStage, "prepare"
            ) as mock_prepare:
                mock_fe.return_value = True
                status = await stage.run(
                    stage_input, wait_for_containers=wait_for_containers
                )
                mock_prepare.assert_called_with(
                    instance_id, "in", "out", 2, wait_for_containers, None
                )

            # Input not ready
            with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
                mock_fe.return_value = False
                status = await stage.run(
                    stage_input, wait_for_containers=wait_for_containers
                )
                self.assertEqual(PIDStageStatus.FAILED, status)

            # Multiple input paths (invariant exception)
            with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
                with self.assertRaises(ValueError):
                    mock_fe.return_value = True
                    stage_input.input_paths = ["in1", "in2"]
                    stage = PIDPrepareStage(
                        stage=UnionPIDStage.PUBLISHER_PREPARE,
                        instance_repository=mock_instance_repo,
                        storage_svc=mock_storage_svc,
                        onedocker_svc=mock_onedocker_svc,
                        onedocker_binary_config=MagicMock(
                            task_definition="offline-task:1#container",
                            tmp_directory="/tmp/",
                            binary_version="latest",
                        ),
                    )
                    status = await stage.run(
                        stage_input, wait_for_containers=wait_for_containers
                    )

        for data_test in data_test_run():
            wait_for_containers = data_test["wait_for_containers"]
            with self.subTest(
                "Subtest with wait_for_containers: {wait_for_containers}",
                wait_for_containers=wait_for_containers,
            ):
                # reset mocks for each subTests
                mock_onedocker_svc.reset_mock()
                mock_instance_repo.reset_mock()
                mock_storage_svc.reset_mock()
                mock_wait_for_containers_async.reset_mock()
                await _run_sub_test(wait_for_containers)
