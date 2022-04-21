#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.pcf.tests.async_utils import to_sync
from fbpcs.pid.entity.pid_instance import (
    PIDStageStatus,
    PIDInstanceStatus,
    PIDInstance,
    PIDProtocol,
    PIDRole,
)
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.pid.service.pid_service.pid_dispatcher import PIDDispatcher


TEST_INSTANCE_ID = "123"
TEST_PROTOCOL = PIDProtocol.UNION_PID
TEST_PID_ROLE = PIDRole.PUBLISHER
TEST_NUM_SHARDS = 4
TEST_INPUT_PATH = "in"
TEST_OUTPUT_PATH = "out"
TEST_DATA_PATH = "data"
TEST_SPINE_PATH = "spine"
TEST_IS_VALIDATING = False
TEST_HMAC_KEY = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="


class TestPIDService(unittest.TestCase):
    @patch(
        "fbpcs.onedocker_binary_config.OneDockerBinaryConfig",
        spec="OneDockerBinaryConfig",
    )
    @patch("fbpcp.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcp.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpcs.pid.repository.pid_instance.PIDInstanceRepository")
    def setUp(
        self,
        mock_instance_repo,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_onedocker_binary_config,
    ):
        self.pid_service = PIDService(
            mock_onedocker_service,
            mock_s3_storage_service,
            mock_instance_repo,
            onedocker_binary_config_map={"default": mock_onedocker_binary_config},
        )

    def test_create_instance(self):
        self.pid_service.create_instance(
            instance_id=TEST_INSTANCE_ID,
            pid_role=TEST_PID_ROLE,
            num_shards=TEST_NUM_SHARDS,
            input_path=TEST_INPUT_PATH,
            output_path=TEST_OUTPUT_PATH,
            data_path=TEST_DATA_PATH,
            spine_path=TEST_SPINE_PATH,
            hmac_key=TEST_HMAC_KEY,
        )
        # check that the right parameters are used when creating pid instance
        self.pid_service.instance_repository.create.assert_called()
        create_call_params = self.pid_service.instance_repository.create.call_args[0][0]
        self.assertEqual(TEST_INSTANCE_ID, create_call_params.instance_id)
        self.assertEqual(TEST_PROTOCOL, create_call_params.protocol)
        self.assertEqual(TEST_PID_ROLE, create_call_params.pid_role)
        self.assertEqual(TEST_NUM_SHARDS, create_call_params.num_shards)
        self.assertEqual(TEST_INPUT_PATH, create_call_params.input_path)
        self.assertEqual(TEST_OUTPUT_PATH, create_call_params.output_path)
        self.assertEqual(TEST_DATA_PATH, create_call_params.data_path)
        self.assertEqual(TEST_SPINE_PATH, create_call_params.spine_path)
        self.assertEqual(TEST_HMAC_KEY, create_call_params.hmac_key)

    def test_update_instance(self):
        stage1 = UnionPIDStage.PUBLISHER_SHARD
        stage2 = UnionPIDStage.PUBLISHER_PREPARE

        pid_instance = PIDInstance(
            instance_id=TEST_INSTANCE_ID,
            protocol=TEST_PROTOCOL,
            pid_role=TEST_PID_ROLE,
            num_shards=TEST_NUM_SHARDS,
            input_path=TEST_INPUT_PATH,
            output_path=TEST_INPUT_PATH,
            status=PIDInstanceStatus.STARTED,
            stages_status={
                stage1: PIDStageStatus.UNKNOWN,
                stage2: PIDStageStatus.UNKNOWN,
            },
        )

        # no containers have spawned yet, so the status doesn't change
        self.pid_service.instance_repository.read = MagicMock(return_value=pid_instance)
        pid_instance = self.pid_service.update_instance(TEST_INSTANCE_ID)
        self.assertEqual(pid_instance.status, PIDInstanceStatus.STARTED)
        self.assertIsNone(pid_instance.current_stage)
        self.assertEqual(
            pid_instance.stages_status[stage1],
            PIDStageStatus.UNKNOWN,
        )

        containers = [
            self._create_container(i, ContainerInstanceStatus.STARTED) for i in range(2)
        ]
        pid_instance.stages_containers[stage1] = containers
        self.pid_service.onedocker_svc.get_containers = MagicMock(
            return_value=containers
        )
        # stage 1 containers are both in started state, so the stage status should be started
        self.pid_service.instance_repository.read = MagicMock(return_value=pid_instance)
        pid_instance = self.pid_service.update_instance(TEST_INSTANCE_ID)
        self.assertEqual(pid_instance.status, PIDInstanceStatus.STARTED)
        self.assertEqual(pid_instance.current_stage, stage1)
        self.assertEqual(
            pid_instance.stages_status[stage1],
            PIDStageStatus.STARTED,
        )

        # stage 1 containers are finished, so the stage status should be COMPLETED
        self.pid_service.onedocker_svc.get_containers = MagicMock(
            return_value=[
                self._create_container(i, ContainerInstanceStatus.COMPLETED)
                for i in range(2)
            ]
        )
        self.pid_service.instance_repository.read = MagicMock(return_value=pid_instance)
        pid_instance = self.pid_service.update_instance(TEST_INSTANCE_ID)
        self.assertEqual(pid_instance.status, PIDInstanceStatus.STARTED)
        self.assertEqual(pid_instance.current_stage, stage1)
        self.assertEqual(
            pid_instance.stages_status[stage1],
            PIDStageStatus.COMPLETED,
        )

        # start stage 2 containers
        pid_instance.stages_containers[stage2] = [
            self._create_container(i, ContainerInstanceStatus.STARTED) for i in range(2)
        ]
        # when we get them, have their updated status be failed
        self.pid_service.onedocker_svc.get_containers = MagicMock(
            return_value=[
                self._create_container(i, ContainerInstanceStatus.FAILED)
                for i in range(2)
            ]
        )
        # shard stage containers are both in a failed state, so the stage status should be failed
        # and the instance status should also be failed
        self.pid_service.instance_repository.read = MagicMock(return_value=pid_instance)
        pid_instance = self.pid_service.update_instance(TEST_INSTANCE_ID)
        self.assertEqual(pid_instance.status, PIDInstanceStatus.FAILED)
        self.assertEqual(pid_instance.current_stage, stage2)
        self.assertEqual(
            pid_instance.stages_status[stage2],
            PIDStageStatus.FAILED,
        )

        # start that stage over
        pid_instance.status = PIDInstanceStatus.STARTED
        pid_instance.stages_status[stage2] = PIDStageStatus.STARTED

        # when we get containers this time, it will succeed
        self.pid_service.onedocker_svc.get_containers = MagicMock(
            return_value=[
                self._create_container(i, ContainerInstanceStatus.COMPLETED)
                for i in range(2)
            ]
        )
        # shard stage containers are both in a completed state, so the stage status should be completed
        # All stages are done, so instance status should also be completed
        self.pid_service.instance_repository.read = MagicMock(return_value=pid_instance)
        pid_instance = self.pid_service.update_instance(TEST_INSTANCE_ID)
        self.assertEqual(pid_instance.status, PIDInstanceStatus.COMPLETED)
        self.assertEqual(pid_instance.current_stage, stage2)
        self.assertEqual(
            pid_instance.stages_status[stage2],
            PIDStageStatus.COMPLETED,
        )

    def _create_container(
        self, id: int, status: ContainerInstanceStatus
    ) -> ContainerInstance:
        return ContainerInstance(
            f"arn:aws:ecs:region:account_id:task/container_id_{id}",
            f"192.0.2.{id}",
            status,
        )

    @to_sync
    async def test_run_instance(self):
        with patch.object(PIDDispatcher, "__init__") as mock_init, patch.object(
            PIDDispatcher, "build_stages"
        ) as mock_build_stages, patch.object(PIDDispatcher, "run_all") as mock_run_all:
            # add the line below to avoid "TypeError: __init__() should return None, not 'MagicMock'""
            mock_init.return_value = None
            sample_pid_instance = self._get_sample_pid_instance()
            self.pid_service.instance_repository.read = MagicMock(
                return_value=sample_pid_instance
            )
            await self.pid_service.run_instance(
                instance_id=TEST_INSTANCE_ID,
            )
            mock_init.assert_called_once()
            init_call_params = mock_init.call_args[1]
            self.assertEqual(TEST_INSTANCE_ID, init_call_params["instance_id"])
            mock_build_stages.assert_called_once()
            mock_run_all.assert_called_once()

    def test_get_instance(self):
        sample_pid_instance = self._get_sample_pid_instance()
        self.pid_service.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )

        self.assertEqual(
            sample_pid_instance, self.pid_service.get_instance(TEST_INSTANCE_ID)
        )

    def test_stop_instance(self):
        sample_pid_instance = self._get_sample_pid_instance()
        sample_pid_instance.current_stage = UnionPIDStage.ADV_RUN_PID
        sample_pid_instance.stages_containers[UnionPIDStage.ADV_RUN_PID] = [
            self._create_container(i, ContainerInstanceStatus.STARTED) for i in range(2)
        ]
        self.pid_service.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )
        self.pid_service.onedocker_svc.stop_containers = MagicMock(return_value=[None])
        canceled_instance = self.pid_service.stop_instance(TEST_INSTANCE_ID)
        self.pid_service.onedocker_svc.stop_containers.assert_called_with(
            [
                "arn:aws:ecs:region:account_id:task/container_id_0",
                "arn:aws:ecs:region:account_id:task/container_id_1",
            ]
        )
        expected_pid_instance = sample_pid_instance
        expected_pid_instance.status = PIDInstanceStatus.CANCELED
        self.assertEqual(expected_pid_instance, canceled_instance)
        self.pid_service.instance_repository.update.assert_called_with(
            expected_pid_instance
        )

    def _get_sample_pid_instance(self) -> PIDInstance:
        return PIDInstance(
            instance_id=TEST_INSTANCE_ID,
            protocol=TEST_PROTOCOL,
            pid_role=TEST_PID_ROLE,
            num_shards=TEST_NUM_SHARDS,
            is_validating=TEST_IS_VALIDATING,
            input_path=TEST_INPUT_PATH,
            output_path=TEST_OUTPUT_PATH,
            data_path=TEST_DATA_PATH,
            spine_path=TEST_SPINE_PATH,
        )
