#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpcs.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.id_match_stage_service import IdMatchStageService


class TestIdMatchStageService(IsolatedAsyncioTestCase):
    @patch("fbpcs.pid.service.pid_service.pid.PIDService")
    async def test_run_async(self, pid_svc_mock) -> None:

        pc_instance = PrivateComputationInstance(
            instance_id="123",
            role=PrivateComputationRole.PUBLISHER,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1600000000,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            game_type=PrivateComputationGameType.LIFT,
            input_path="456",
            output_dir="789",
        )

        pid_instance = PIDInstance(
            instance_id="123_id_match0",
            protocol=PIDProtocol.UNION_PID,
            pid_role=PIDRole.PUBLISHER,
            num_shards=2,
            input_path=pc_instance.input_path,
            output_path=pc_instance.pid_stage_output_data_path,
            status=PIDInstanceStatus.STARTED,
        )

        pid_svc_mock.run_instance = AsyncMock(return_value=pid_instance)

        stage_svc = IdMatchStageService(
            pid_svc_mock,
            pid_config={},
            protocol=PIDProtocol.UNION_PID,
        )
        await stage_svc.run_async(pc_instance)
        self.assertIsInstance(pc_instance.instances[0], PIDInstance)

    def test_map_private_computation_role_to_pid_role(self):
        self.assertEqual(
            PIDRole.PUBLISHER,
            IdMatchStageService._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PUBLISHER
            ),
        )
        self.assertEqual(
            PIDRole.PARTNER,
            IdMatchStageService._map_private_computation_role_to_pid_role(
                PrivateComputationRole.PARTNER
            ),
        )
