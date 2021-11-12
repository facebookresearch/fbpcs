#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch, MagicMock

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pcf.tests.async_utils import to_sync
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.coordination.file_coordination import FileCoordinationService
from fbpcs.pid.service.pid_service.pid_run_protocol_stage import PIDProtocolRunStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput
from libfb.py.asyncio.mock import AsyncMock
from libfb.py.testutil import data_provider

CONFIG = {
    "dependency": {
        "CoordinationService": {
            "class": "fbpcs.pid.service.coordination.file_coordination.FileCoordinationService",
            "constructor": {
                "coordination_objects": {
                    "pid_ip_addrs": {
                        "value": "ip_config.txt",
                    },
                },
            },
        },
    },
    "CloudCredentialService": {
        "class": "fbpcs.pid.service.credential_service.simple_cloud_credential_service.SimpleCloudCredentialService",
        "constructor": {
            "access_key_id": "key_id",
            "access_key_data": "key_data",
        },
    },
}


class TestPIDProtocolRunStage(unittest.TestCase):
    def setUp(self):
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp/",
            binary_version="latest",
        )

    @to_sync
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_not_ready(self, mock_instance_repo):
        stage_input = PIDStageInput(
            input_paths=["not_exists"],
            output_paths=["out"],
            num_shards=1,
            instance_id="123",
        )
        adv_run_stage = PIDProtocolRunStage(
            stage=UnionPIDStage.ADV_RUN_PID,
            config=CONFIG,
            instance_repository=mock_instance_repo,
            storage_svc="STORAGE",
            onedocker_svc="ONEDOCKER",
            onedocker_binary_config=self.onedocker_binary_config,
        )

        self.assertEqual(
            PIDStageStatus.FAILED,
            await adv_run_stage._ready(stage_input=stage_input),
        )

    @data_provider(
        lambda: ({"wait_for_containers": True}, {"wait_for_containers": False})
    )
    @to_sync
    @patch(
        "fbpcs.pid.service.pid_service.pid_run_protocol_stage.wait_for_containers_async"
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
        wait_for_containers: bool,
    ):
        ip = "192.0.2.0"
        container = ContainerInstance(instance_id="123", ip_address=ip)
        mock_onedocker_service.start_containers = MagicMock(return_value=[container])
        mock_onedocker_service.wait_for_pending_containers = AsyncMock(return_value=[container])
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
                config=CONFIG,
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
                mock_instance_repo.update.call_count, 4 + int(wait_for_containers)
            )

    @data_provider(
        lambda: ({"wait_for_containers": True}, {"wait_for_containers": False})
    )
    @to_sync
    @patch(
        "fbpcs.pid.service.pid_service.pid_run_protocol_stage.wait_for_containers_async"
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
        wait_for_containers: bool,
    ):
        ip = "192.0.2.0"
        container = ContainerInstance(instance_id="123", ip_address=ip)
        mock_onedocker_service.start_containers = MagicMock(return_value=[container])
        mock_onedocker_service.wait_for_pending_containers = AsyncMock(return_value=[container])
        container.status = ContainerInstanceStatus.COMPLETED if wait_for_containers else ContainerInstanceStatus.STARTED
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
                config=CONFIG,
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
                PIDStageStatus.COMPLETED if wait_for_containers else PIDStageStatus.STARTED,
                await adv_run_stage.run(stage_input=stage_input, wait_for_containers=wait_for_containers),
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
