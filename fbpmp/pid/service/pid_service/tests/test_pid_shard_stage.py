#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch, MagicMock

from fbpmp.data_processing.sharding.sharding import ShardType
from fbpmp.data_processing.sharding.sharding_cpp import CppShardingService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pcf.tests.async_utils import to_sync
from fbpmp.pid.entity.pid_instance import PIDStageStatus
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.repository.pid_instance_local import LocalPIDInstanceRepository
from fbpmp.pid.service.pid_service.pid_shard_stage import PIDShardStage
from fbpmp.pid.service.pid_service.pid_stage_input import PIDStageInput


CONFIG = {
    "s3_coordination_file": "ip_config"
}


async def async_wrapper(value):
    return value


class TestPIDShardStage(unittest.TestCase):
    @to_sync
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    async def test_ready(self, mock_instance_repo):
        stage = PIDShardStage(
            stage=UnionPIDStage.PUBLISHER_SHARD,
            config=CONFIG,
            instance_repository=mock_instance_repo,
            storage_svc="STORAGE",
            onedocker_svc="ONEDOCKER",
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

    @patch.object(
        PIDShardStage,
        "shard",
        return_value=async_wrapper(PIDStageStatus.COMPLETED),
    )
    @to_sync
    async def test_run(self, mock_shard):
        mock_instance_repo = LocalPIDInstanceRepository(base_dir=".")
        mock_instance_repo.read = MagicMock()
        mock_instance_repo.update = MagicMock()
        stage = PIDShardStage(
            stage=UnionPIDStage.PUBLISHER_SHARD,
            config=CONFIG,
            instance_repository=mock_instance_repo,
            storage_svc="STORAGE",
            onedocker_svc="ONEDOCKER",
            onedocker_binary_config="OD_CONFIG",
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
                config=CONFIG,
                instance_repository=mock_instance_repo,
                storage_svc="STORAGE",
                onedocker_svc="ONEDOCKER",
                onedocker_binary_config="OD_CONFIG",
            )
            status = await stage.run(stage_input)
            # instance status is updated to READY, STARTED, then COMPLETED
            mock_instance_repo.read.assert_called_with(instance_id)
            self.assertEqual(mock_instance_repo.read.call_count, 3)
            self.assertEqual(mock_instance_repo.update.call_count, 3)

        # Input not ready
        with patch.object(PIDShardStage, "files_exist") as mock_fe:
            mock_fe.return_value = False
            status = await stage.run(stage_input)
            self.assertEqual(PIDStageStatus.FAILED, status)

        # Multiple input paths (invariant exception)
        with patch.object(PIDShardStage, "files_exist") as mock_fe:
            with self.assertRaises(ValueError):
                mock_fe.return_value = True
                stage_input.input_paths = ["in1", "in2"]
                stage = PIDShardStage(
                    stage=UnionPIDStage.PUBLISHER_SHARD,
                    config=CONFIG,
                    instance_repository=mock_instance_repo,
                    storage_svc="STORAGE",
                    onedocker_svc="ONEDOCKER",
                    onedocker_binary_config="OD_CONFIG",
                )
                status = await stage.run(stage_input)
                self.assertEqual(PIDStageStatus.COMPLETED, status)
                mock_shard.assert_called_once_with("in1", "out", 123)

    @patch.object(CppShardingService, "shard_on_container_async")
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_shard(self, mock_instance_repo, mock_sharder):
        test_onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/test_tmp_directory/",
            binary_version="latest",
        )
        stage = PIDShardStage(
            stage=UnionPIDStage.PUBLISHER_SHARD,
            config=CONFIG,
            instance_repository=mock_instance_repo,
            storage_svc="STORAGE",
            onedocker_svc="ONEDOCKER",
            onedocker_binary_config=test_onedocker_binary_config,
        )

        test_input_path = "foo"
        test_output_path = "bar"
        test_num_shards = 1
        test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

        shard_path = PIDShardStage.get_sharded_filepath(test_output_path, 0)
        self.assertEqual(f"{test_output_path}_0", shard_path)

        res = await stage.shard(
            test_input_path, test_output_path, test_num_shards, test_hmac_key
        )
        self.assertEqual(PIDStageStatus.COMPLETED, res)

        mock_sharder.assert_called_once_with(
            ShardType.HASHED_FOR_PID,
            test_input_path,
            output_base_path=test_output_path,
            file_start_index=0,
            num_output_files=test_num_shards,
            onedocker_svc=stage.onedocker_svc,
            binary_version=test_onedocker_binary_config.binary_version,
            tmp_directory=test_onedocker_binary_config.tmp_directory,
            hmac_key=test_hmac_key,
        )
