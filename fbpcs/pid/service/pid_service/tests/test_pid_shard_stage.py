#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.data_processing.service.sharding_service import ShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.pid_service.pid_shard_stage import PIDShardStage
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


class TestPIDShardStage(unittest.TestCase):
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_ready(self, mock_instance_repo) -> None:
        stage = PIDShardStage(
            stage=UnionPIDStage.PUBLISHER_SHARD,
            instance_repository=mock_instance_repo,
            # pyre-fixme[6]: For 3rd param expected `StorageService` but got `str`.
            storage_svc="STORAGE",
            # pyre-fixme[6]: For 4th param expected `OneDockerService` but got `str`.
            onedocker_svc="ONEDOCKER",
            # pyre-fixme[6]: For 5th param expected `OneDockerBinaryConfig` but got
            #  `str`.
            onedocker_binary_config="OD_CONFIG",
        )
        stage_input = PIDStageInput(
            input_paths=["in"],
            output_paths=["out"],
            num_shards=123,
            instance_id="444",
        )

        with patch.object(PIDShardStage, "files_exist") as mock_fe:
            mock_fe.return_value = True
            res = await stage._ready(stage_input)
            self.assertTrue(res)

    @patch(
        "fbpcs.data_processing.service.sharding_service.ShardingService.wait_for_containers_async"
    )
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_run(
        self,
        mock_instance_repo,
        mock_onedocker_svc,
        mock_storage_svc,
        mock_wait_for_containers_async,
    ) -> None:
        async def _run_sub_test(
            wait_for_containers: bool,
        ) -> None:
            ip = "192.0.2.0"
            container = ContainerInstance(instance_id="123", ip_address=ip)

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
            test_onedocker_binary_config = OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
            stage = PIDShardStage(
                stage=UnionPIDStage.PUBLISHER_SHARD,
                instance_repository=mock_instance_repo,
                storage_svc=mock_storage_svc,
                onedocker_svc=mock_onedocker_svc,
                onedocker_binary_config=test_onedocker_binary_config,
            )
            instance_id = "444"
            stage_input = PIDStageInput(
                input_paths=["in"],
                output_paths=["out"],
                num_shards=123,
                instance_id=instance_id,
            )

            # Basic test: All good
            with patch.object(PIDShardStage, "files_exist") as mock_fe:
                mock_fe.return_value = True
                stage = PIDShardStage(
                    stage=UnionPIDStage.PUBLISHER_SHARD,
                    instance_repository=mock_instance_repo,
                    storage_svc=mock_storage_svc,
                    onedocker_svc=mock_onedocker_svc,
                    onedocker_binary_config=test_onedocker_binary_config,
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

                mock_onedocker_svc.start_containers.assert_called_once()
                if wait_for_containers:
                    mock_wait_for_containers_async.assert_called_once()
                else:
                    mock_wait_for_containers_async.assert_not_called()
                # instance status is updated to READY, STARTED, then COMPLETED
                mock_instance_repo.read.assert_called_with(instance_id)
                self.assertEqual(mock_instance_repo.read.call_count, 4)
                self.assertEqual(mock_instance_repo.update.call_count, 4)

            # Input not ready
            with patch.object(PIDShardStage, "files_exist") as mock_fe:
                mock_fe.return_value = False
                status = await stage.run(
                    stage_input, wait_for_containers=wait_for_containers
                )
                self.assertEqual(PIDStageStatus.FAILED, status)

            # Multiple input paths (invariant exception)
            with patch.object(PIDShardStage, "files_exist") as mock_fe:
                with self.assertRaises(ValueError):
                    mock_fe.return_value = True
                    stage_input.input_paths = ["in1", "in2"]
                    stage = PIDShardStage(
                        stage=UnionPIDStage.PUBLISHER_SHARD,
                        instance_repository=mock_instance_repo,
                        storage_svc=mock_storage_svc,
                        onedocker_svc=mock_onedocker_svc,
                        onedocker_binary_config=test_onedocker_binary_config,
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

        for wait_for_containers in (True, False):
            with self.subTest(
                "Subtest with wait_for_containers: {wait_for_containers}",
                wait_for_containers=wait_for_containers,
            ):
                # reset mocks for each subTests
                mock_instance_repo.reset_mock()
                mock_onedocker_svc.reset_mock()
                mock_storage_svc.reset_mock()
                mock_wait_for_containers_async.reset_mock()
                await _run_sub_test(wait_for_containers)

    @patch.object(ShardingService, "start_containers")
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_shard(
        self,
        mock_instance_repo,
        mock_onedocker_svc,
        mock_storage_svc,
        mock_sharder,
    ) -> None:
        async def _run_sub_test(
            wait_for_containers: bool,
            expected_container_status: ContainerInstanceStatus,
        ) -> None:
            with patch.object(PIDStage, "update_instance_containers"):
                test_onedocker_binary_config = OneDockerBinaryConfig(
                    tmp_directory="/test_tmp_directory/",
                    binary_version="latest",
                    repository_path="test_path/",
                )
                container = ContainerInstance(
                    instance_id="123",
                    ip_address="192.0.2.0",
                    status=expected_container_status,
                )
                mock_sharder.return_value = [container]
                stage = PIDShardStage(
                    stage=UnionPIDStage.PUBLISHER_SHARD,
                    instance_repository=mock_instance_repo,
                    storage_svc=mock_storage_svc,
                    onedocker_svc=mock_onedocker_svc,
                    onedocker_binary_config=test_onedocker_binary_config,
                )

                test_input_path = "foo"
                test_output_path = "bar"
                test_num_shards = 1
                test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

                shard_path = PIDShardStage.get_sharded_filepath(test_output_path, 0)
                self.assertEqual(f"{test_output_path}_0", shard_path)

                res = await stage.shard(
                    "123",
                    test_input_path,
                    test_output_path,
                    test_num_shards,
                    test_hmac_key,
                    wait_for_containers=wait_for_containers,
                )
                self.assertEqual(
                    PIDStage.get_stage_status_from_containers([container]),
                    res,
                )

                mock_sharder.assert_called_once()

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
                mock_onedocker_svc.reset_mock()
                mock_storage_svc.reset_mock()
                mock_sharder.reset_mock()
                await _run_sub_test(
                    data_test[0],
                    data_test[1],
                )
