#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpcs.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.pid_stage_service import PIDStageService


class TestPIDStageService(IsolatedAsyncioTestCase):
    @patch("fbpcs.pid.service.pid_service.pid.PIDService")
    async def test_run_shard_async(self, pid_svc_mock) -> None:
        pc_instance = self._create_pc_instance()
        pid_instance = self._create_pid_instance(
            pc_instance.input_path,
            pc_instance.pid_stage_output_data_path,
            UnionPIDStage.PUBLISHER_SHARD,
        )

        pid_svc_mock.run_stage_or_next = AsyncMock(return_value=pid_instance)

        stage_svc = PIDStageService(
            pid_svc_mock,
            publisher_stage=UnionPIDStage.PUBLISHER_SHARD,
            partner_stage=UnionPIDStage.ADV_SHARD,
            protocol=PIDProtocol.UNION_PID,
        )

        self.assertEqual(len(pc_instance.instances), 0)
        await stage_svc.run_async(pc_instance)

        # an instance should be created and added to pc_instance.instances
        pid_svc_mock.create_instance.assert_called()
        self.assertEqual(len(pc_instance.instances), 1)

        # verifies that the shard stage service returns an instance
        self.assertIsInstance(pc_instance.instances[0], PIDInstance)

    @patch("fbpcs.pid.service.pid_service.pid.PIDService")
    async def test_run_prepare(self, pid_svc_mock) -> None:
        pc_instance = self._create_pc_instance()
        old_pid_instance = self._create_pid_instance(
            pc_instance.input_path,
            pc_instance.pid_stage_output_data_path,
            UnionPIDStage.PUBLISHER_SHARD,
        )
        new_pid_instance = self._create_pid_instance(
            pc_instance.input_path,
            pc_instance.pid_stage_output_data_path,
            UnionPIDStage.PUBLISHER_PREPARE,
        )

        pid_svc_mock.run_stage_or_next = AsyncMock(return_value=new_pid_instance)

        stage_svc = PIDStageService(
            pid_svc_mock,
            publisher_stage=UnionPIDStage.PUBLISHER_PREPARE,
            partner_stage=UnionPIDStage.ADV_PREPARE,
            protocol=PIDProtocol.UNION_PID,
        )

        self.assertEqual(len(pc_instance.instances), 0)

        with self.assertRaises(RuntimeError):
            # pid run won't create a pid instance
            await stage_svc.run_async(pc_instance)

        pc_instance.instances.append(old_pid_instance)
        await stage_svc.run_async(pc_instance)

        # a new instance should not be created (all pid stages share an instance)
        pid_svc_mock.create_instance.assert_not_called()
        # verifies that the stage svc stores the latest instance
        self.assertEqual(len(pc_instance.instances), 1)
        self.assertEqual(pc_instance.instances[0], new_pid_instance)

    @patch("fbpcs.pid.service.pid_service.pid.PIDService")
    async def test_run_pid_run(self, pid_svc_mock) -> None:
        pc_instance = self._create_pc_instance()
        old_pid_instance = self._create_pid_instance(
            pc_instance.input_path,
            pc_instance.pid_stage_output_data_path,
            UnionPIDStage.PUBLISHER_PREPARE,
        )
        new_pid_instance = self._create_pid_instance(
            pc_instance.input_path,
            pc_instance.pid_stage_output_data_path,
            UnionPIDStage.PUBLISHER_RUN_PID,
        )

        pid_svc_mock.run_stage_or_next = AsyncMock(return_value=new_pid_instance)

        stage_svc = PIDStageService(
            pid_svc_mock,
            publisher_stage=UnionPIDStage.PUBLISHER_RUN_PID,
            partner_stage=UnionPIDStage.ADV_RUN_PID,
            protocol=PIDProtocol.UNION_PID,
        )

        self.assertEqual(len(pc_instance.instances), 0)

        with self.assertRaises(RuntimeError):
            # pid run won't create a pid instance
            await stage_svc.run_async(pc_instance)

        pc_instance.instances.append(old_pid_instance)
        await stage_svc.run_async(pc_instance)

        # a new instance should not be created (all pid stages share an instance)
        pid_svc_mock.create_instance.assert_not_called()
        # verifies that the stage svc stores the latest instance
        self.assertEqual(len(pc_instance.instances), 1)
        self.assertEqual(pc_instance.instances[0], new_pid_instance)

    def test_map_private_computation_role_to_pid_role(self) -> None:
        self.assertEqual(
            PIDRole.PUBLISHER,
            PIDStageService._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PUBLISHER
            ),
        )
        self.assertEqual(
            PIDRole.PARTNER,
            PIDStageService._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PARTNER
            ),
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        return PrivateComputationInstance(
            instance_id="123",
            role=PrivateComputationRole.PUBLISHER,
            instances=[],
            status=PrivateComputationInstanceStatus.UNKNOWN,
            status_update_ts=1600000000,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            game_type=PrivateComputationGameType.LIFT,
            input_path="456",
            output_dir="789",
        )

    def _create_pid_instance(
        self, input_path: str, output_path: str, current_stage: UnionPIDStage
    ) -> PIDInstance:
        return PIDInstance(
            instance_id="123_id_match0",
            protocol=PIDProtocol.UNION_PID,
            pid_role=PIDRole.PUBLISHER,
            num_shards=2,
            input_path=input_path,
            output_path=output_path,
            status=PIDInstanceStatus.STARTED,
            current_stage=current_stage,
        )
