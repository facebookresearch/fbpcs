#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch, MagicMock

from fbpmp.data_processing.pid_preparer.union_pid_preparer_cpp import (
    CppUnionPIDDataPreparerService,
)
from fbpmp.pcf.tests.async_utils import to_sync
from fbpmp.pid.entity.pid_instance import PIDStageStatus
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.repository.pid_instance_local import LocalPIDInstanceRepository
from fbpmp.pid.service.pid_service.pid_prepare_stage import PIDPrepareStage
from fbpmp.pid.service.pid_service.pid_stage_input import PIDStageInput


CONFIG = {
    "s3_coordination_file": "ip_config"
}


async def async_wrapper(value):
    return value


class TestPIDPrepareStage(unittest.TestCase):
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    @to_sync
    async def test_prepare(self, mock_instance_repo):
        with patch.object(CppUnionPIDDataPreparerService, "prepare_on_container_async"):
            stage = PIDPrepareStage(
                stage=UnionPIDStage.PUBLISHER_PREPARE,
                config=CONFIG,
                instance_repository=mock_instance_repo,
                storage_svc="STORAGE",
                onedocker_svc="ONEDOCKER",
                onedocker_binary_config=MagicMock(
                    task_definition="offline-task:1#container",
                    tmp_directory="/tmp/",
                    binary_version="latest",
                ),
            )

            res = await stage.prepare(
                input_path="in", output_path="out", num_shards=1, fail_fast=False
            )
            self.assertEqual(res, PIDStageStatus.COMPLETED)

    @patch.object(
        PIDPrepareStage,
        "prepare",
        return_value=async_wrapper(PIDStageStatus.COMPLETED),
    )
    @to_sync
    async def test_run(self, mock_prepare):
        mock_instance_repo = LocalPIDInstanceRepository(base_dir=".")
        mock_instance_repo.read = MagicMock()
        mock_instance_repo.update = MagicMock()
        stage = PIDPrepareStage(
            stage=UnionPIDStage.PUBLISHER_PREPARE,
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
            num_shards=2,
            instance_id=instance_id,
        )

        # Basic test: All good
        with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
            mock_fe.return_value = True
            stage = PIDPrepareStage(
                stage=UnionPIDStage.PUBLISHER_PREPARE,
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

        # fail_fast = True
        with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
            mock_fe.return_value = True
            stage_input.fail_fast = True
            await stage.run(stage_input)
            mock_prepare.assert_called_with(
                "in", "out", 2, True
            )  # make sure the last parameter matches stage_input.fail_fast

        # Input not ready
        with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
            mock_fe.return_value = False
            status = await stage.run(stage_input)
            self.assertEqual(PIDStageStatus.FAILED, status)

        # Multiple input paths (invariant exception)
        with patch.object(PIDPrepareStage, "files_exist") as mock_fe:
            with self.assertRaises(ValueError):
                mock_fe.return_value = True
                stage_input.input_paths = ["in1", "in2"]
                stage = PIDPrepareStage(
                    stage=UnionPIDStage.PUBLISHER_PREPARE,
                    config=CONFIG,
                    instance_repository=mock_instance_repo,
                    storage_svc="STORAGE",
                    onedocker_svc="ONEDOCKER",
                    onedocker_binary_config="OD_CONFIG",
                )
                status = await stage.run(stage_input)
