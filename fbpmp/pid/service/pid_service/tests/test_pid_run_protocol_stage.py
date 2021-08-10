#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch, MagicMock

from fbpcs.entity.container_instance import ContainerInstance
from fbpcs.service.container_aws import AWSContainerService
from fbpcs.service.onedocker import OneDockerService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pcf.tests.async_utils import awaitable, to_sync
from fbpmp.pid.entity.pid_instance import PIDStageStatus
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.repository.pid_instance_local import LocalPIDInstanceRepository
from fbpmp.pid.service.coordination.file_coordination import FileCoordinationService
from fbpmp.pid.service.pid_service.pid_run_protocol_stage import PIDProtocolRunStage
from fbpmp.pid.service.pid_service.pid_stage_input import PIDStageInput
from libfb.py.asyncio.mock import AsyncMock


CONFIG = {
    "dependency": {
        "CoordinationService": {
            "class": "fbpmp.pid.service.coordination.file_coordination.FileCoordinationService",
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
        "class": "fbpmp.pid.service.credential_service.simple_cloud_credential_service.SimpleCloudCredentialService",
        "constructor": {
            "access_key_id": "key_id",
            "access_key_data": "key_data",
        }
    },
}


class TestPIDProtocolRunStage(unittest.TestCase):
    def setUp(self):
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp/",
            binary_version="latest",
        )

    @to_sync
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
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

    @to_sync
    @patch("fbpcs.service.onedocker.OneDockerService", spec=OneDockerService)
    async def test_run_publisher(self, mock_onedocker_service):
        ip = "192.0.2.0"
        mock_onedocker_service.start_containers_async = AsyncMock(
            return_value=[ContainerInstance(instance_id="123", ip_address=ip)]
        )
        mock_instance_repo = LocalPIDInstanceRepository(base_dir=".")
        mock_instance_repo.read = MagicMock()
        mock_instance_repo.update = MagicMock()

        with patch.object(
            PIDProtocolRunStage, "files_exist"
        ) as mock_files_exist, patch.object(
            PIDProtocolRunStage,
            "_wait_for_containers",
            return_value=awaitable(True),
        ) as mock_wait_for_containers, patch.object(
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
                storage_svc="STORAGE",
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

            # Check whether the run was completed
            self.assertEqual(
                PIDStageStatus.COMPLETED,
                await publisher_run_stage.run(stage_input=stage_input),
            )

            # Check create_instances_async was called with the correct parameters
            mock_wait_for_containers.assert_called_once()
            mock_onedocker_service.start_containers_async.assert_called_once()
            (
                _,
                called_kwargs,
            ) = mock_onedocker_service.start_containers_async.call_args_list[0]
            self.assertEqual(num_shards, len(called_kwargs["cmd_args_list"]))

            # Check `put_payload` was called with the correct parameters
            mock_put_server_ips.assert_called_once_with(
                instance_id=instance_id, server_ips=[ip]
            )

            # instance status is updated to READY, STARTED and COMPLETED,
            # then containers STARTED and COMPLETED
            mock_instance_repo.read.assert_called_with(instance_id)
            self.assertEqual(mock_instance_repo.read.call_count, 5)
            self.assertEqual(mock_instance_repo.update.call_count, 5)

    @to_sync
    @patch("fbpcs.service.onedocker.OneDockerService", spec=OneDockerService)
    async def test_run_partner(self, mock_onedocker_service):
        ip = "192.0.2.0"
        mock_onedocker_service.start_containers_async = AsyncMock(
            return_value=[ContainerInstance(instance_id="123", ip_address=ip)]
        )
        mock_instance_repo = LocalPIDInstanceRepository(base_dir=".")
        mock_instance_repo.read = MagicMock()
        mock_instance_repo.update = MagicMock()

        with patch.object(
            PIDProtocolRunStage, "files_exist"
        ) as mock_files_exist, patch.object(
            PIDProtocolRunStage,
            "_wait_for_containers",
            return_value=awaitable(True),
        ) as mock_wait_for_containers, patch.object(
            AWSContainerService,
            "create_instances_async",
            return_value=[ContainerInstance(instance_id="123", ip_address=ip)],
        ) as mock_create_instances_async, patch.object(
            FileCoordinationService, "wait"
        ) as mock_wait, patch.object(
            FileCoordinationService, "get_payload"
        ) as mock_get_payload:
            mock_files_exist.return_value = True

            input_path = "in"
            output_path = "out"
            ip_address = "192.0.2.0"
            hostname = f"https://{ip_address}"
            mock_wait.return_value = True
            mock_get_payload.return_value = [ip_address]

            # Run advertiser
            adv_run_stage = PIDProtocolRunStage(
                stage=UnionPIDStage.ADV_RUN_PID,
                config=CONFIG,
                instance_repository=mock_instance_repo,
                storage_svc="STORAGE",
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

            # Check whether the run was completed
            self.assertEqual(
                PIDStageStatus.COMPLETED,
                await adv_run_stage.run(stage_input=stage_input),
            )

            # Check reate_instances_async was called with the correct parameters
            mock_wait_for_containers.assert_called_once()
            mock_onedocker_service.start_containers_async.assert_called_once()
            (
                _,
                called_kwargs,
            ) = mock_onedocker_service.start_containers_async.call_args_list[0]
            # instance status is updated to READY, STARTED and COMPLETED,
            # then containers STARTED and COMPLETED
            mock_instance_repo.read.assert_called_with(instance_id)
            self.assertEqual(mock_instance_repo.read.call_count, 5)
            self.assertEqual(mock_instance_repo.update.call_count, 5)
