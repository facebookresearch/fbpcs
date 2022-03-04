#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch, MagicMock, AsyncMock

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pcf.tests.async_utils import to_sync
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.coordination.file_coordination import FileCoordinationService
from fbpcs.pid.service.pid_service.pid_run_protocol_stage import PIDProtocolRunStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


class TestPIDProtocolRunStage(unittest.TestCase):
    def setUp(self) -> None:
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp/",
            binary_version="latest",
        )

    @to_sync
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_not_ready(self, mock_instance_repo) -> None:
        stage_input = PIDStageInput(
            input_paths=["not_exists"],
            output_paths=["out"],
            num_shards=1,
            instance_id="123",
        )
        adv_run_stage = PIDProtocolRunStage(
            stage=UnionPIDStage.ADV_RUN_PID,
            instance_repository=mock_instance_repo,
            # pyre-fixme[6]: For 3rd param expected `StorageService` but got `str`.
            storage_svc="STORAGE",
            # pyre-fixme[6]: For 4th param expected `OneDockerService` but got `str`.
            onedocker_svc="ONEDOCKER",
            onedocker_binary_config=self.onedocker_binary_config,
        )

        self.assertEqual(
            PIDStageStatus.FAILED,
            await adv_run_stage._ready(stage_input=stage_input),
        )

    @to_sync
    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.wait_for_containers_async"
    )
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    async def test_run_publisher(
        self,
        mock_onedocker_service,
        mock_instance_repo,
        mock_storage_service,
        mock_wait_for_containers_async,
    ) -> None:
        async def _run_sub_test(wait_for_containers: bool) -> None:
            ip = "192.0.2.0"
            container = ContainerInstance(instance_id="123", ip_address=ip)
            mock_onedocker_service.start_containers = MagicMock(
                return_value=[container]
            )
            mock_onedocker_service.wait_for_pending_containers = AsyncMock(
                return_value=[container]
            )
            container.status = (
                ContainerInstanceStatus.COMPLETED
                if wait_for_containers
                else ContainerInstanceStatus.STARTED
            )
            mock_wait_for_containers_async.return_value = [container]

            with patch.object(
                PIDProtocolRunStage, "files_exist"
            ) as mock_files_exist, patch.object(
                PIDProtocolRunStage, "put_server_ips"
            ) as mock_put_server_ips:
                mock_files_exist.return_value = True

                num_shards = 2
                input_path = "in"
                output_path = "out"

                # Run publisher
                publisher_run_stage = PIDProtocolRunStage(
                    stage=UnionPIDStage.PUBLISHER_RUN_PID,
                    instance_repository=mock_instance_repo,
                    storage_svc=mock_storage_service,
                    onedocker_svc=mock_onedocker_service,
                    onedocker_binary_config=self.onedocker_binary_config,
                )
                instance_id = "123"
                stage_input = PIDStageInput(
                    input_paths=[input_path],
                    output_paths=[output_path],
                    num_shards=num_shards,
                    instance_id=instance_id,
                )

                # if we are waiting for containers, then the stage should finish
                # otherwise, it should start and then return
                self.assertEqual(
                    PIDStageStatus.COMPLETED
                    if wait_for_containers
                    else PIDStageStatus.STARTED,
                    await publisher_run_stage.run(
                        stage_input=stage_input,
                        wait_for_containers=wait_for_containers,
                    ),
                )

                # Check create_instances_async was called with the correct parameters
                if wait_for_containers:
                    mock_wait_for_containers_async.assert_called_once()
                else:
                    mock_wait_for_containers_async.assert_not_called()
                mock_onedocker_service.start_containers.assert_called_once()
                (
                    _,
                    called_kwargs,
                ) = mock_onedocker_service.start_containers.call_args_list[0]
                self.assertEqual(num_shards, len(called_kwargs["cmd_args_list"]))

                # Check `put_payload` was called with the correct parameters
                mock_put_server_ips.assert_called_once_with(
                    instance_id=instance_id, server_ips=[ip]
                )

                # if wait for containers is False, there are 4 updates.
                # if wait_for_containers is True, then there is another update
                # that updates the instance status and containers to complete, so 5
                mock_instance_repo.read.assert_called_with(instance_id)
                self.assertEqual(
                    mock_instance_repo.read.call_count, 4 + int(wait_for_containers)
                )
                self.assertEqual(
                    mock_instance_repo.update.call_count,
                    4 + int(wait_for_containers),
                )

        for wait_for_containers in (True, False):
            with self.subTest(
                "Subtest with wait_for_containers: {wait_for_containers}",
                wait_for_containers=wait_for_containers,
            ):
                # reset mocks for each subTests
                mock_onedocker_service.reset_mock()
                mock_instance_repo.reset_mock()
                mock_storage_service.reset_mock()
                mock_wait_for_containers_async.reset_mock()
                await _run_sub_test(wait_for_containers)

    @to_sync
    @patch(
        "fbpcs.private_computation.service.run_binary_base_service.RunBinaryBaseService.wait_for_containers_async"
    )
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    async def test_run_partner(
        self,
        mock_onedocker_service,
        mock_instance_repo,
        mock_storage_service,
        mock_wait_for_containers_async,
    ) -> None:
        async def _run_sub_test(wait_for_containers: bool) -> None:
            ip = "192.0.2.0"
            container = ContainerInstance(instance_id="123", ip_address=ip)
            mock_onedocker_service.start_containers = MagicMock(
                return_value=[container]
            )
            mock_onedocker_service.wait_for_pending_containers = AsyncMock(
                return_value=[container]
            )
            container.status = (
                ContainerInstanceStatus.COMPLETED
                if wait_for_containers
                else ContainerInstanceStatus.STARTED
            )
            mock_wait_for_containers_async.return_value = [container]

            with patch.object(
                PIDProtocolRunStage, "files_exist"
            ) as mock_files_exist, patch.object(
                FileCoordinationService, "wait"
            ) as mock_wait, patch.object(
                FileCoordinationService, "get_payload"
            ) as mock_get_payload:
                mock_files_exist.return_value = True

                input_path = "in"
                output_path = "out"
                ip_address = "192.0.2.0"
                mock_wait.return_value = True
                mock_get_payload.return_value = [ip_address]

                # Run advertiser
                adv_run_stage = PIDProtocolRunStage(
                    stage=UnionPIDStage.ADV_RUN_PID,
                    instance_repository=mock_instance_repo,
                    storage_svc=mock_storage_service,
                    onedocker_svc=mock_onedocker_service,
                    onedocker_binary_config=self.onedocker_binary_config,
                    server_ips=[ip_address],
                )
                instance_id = "456"
                stage_input = PIDStageInput(
                    input_paths=[input_path],
                    output_paths=[output_path],
                    num_shards=1,
                    instance_id=instance_id,
                )

                # if we are waiting for containers, then the stage should finish
                # otherwise, it should start and then return
                self.assertEqual(
                    PIDStageStatus.COMPLETED
                    if wait_for_containers
                    else PIDStageStatus.STARTED,
                    await adv_run_stage.run(
                        stage_input=stage_input, wait_for_containers=wait_for_containers
                    ),
                )

                # Check create_instances_async was called with the correct parameters
                if wait_for_containers:
                    mock_wait_for_containers_async.assert_called_once()
                else:
                    mock_wait_for_containers_async.assert_not_called()

                mock_onedocker_service.start_containers.assert_called_once()
                (
                    _,
                    called_kwargs,
                ) = mock_onedocker_service.start_containers.call_args_list[0]

                # if wait for containers is False, there are 4 updates.
                # if wait_for_containers is True, then there is another update
                # that updates the instance status and containers to complete, so 5
                mock_instance_repo.read.assert_called_with(instance_id)
                self.assertEqual(
                    mock_instance_repo.read.call_count, 4 + int(wait_for_containers)
                )
                self.assertEqual(
                    mock_instance_repo.update.call_count, 4 + int(wait_for_containers)
                )

        for wait_for_containers in (True, False):
            with self.subTest(
                f"Subtest with wait_for_containers: {wait_for_containers}",
                wait_for_containers=wait_for_containers,
            ):
                # reset mocks for each subTests
                mock_onedocker_service.reset_mock()
                mock_instance_repo.reset_mock()
                mock_storage_service.reset_mock()
                mock_wait_for_containers_async.reset_mock()
                await _run_sub_test(wait_for_containers)
