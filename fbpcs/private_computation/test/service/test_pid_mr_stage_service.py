#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.pid_mr_stage_service import PIDMRStageService
from fbpcs.private_computation.stage_flows.private_computation_mr_stage_flow import (
    PrivateComputationMRStageFlow,
)
from fbpcs.service.workflow import WorkflowStatus
from fbpcs.service.workflow_sfn import SfnWorkflowService


class TestPIDMRStageService(IsolatedAsyncioTestCase):
    @patch("fbpcs.private_computation.service.pid_mr_stage_service.PIDMRStageService")
    async def test_run_async(self, pid_mr_svc_mock) -> None:
        infra_config: InfraConfig = InfraConfig(
            instance_id="publisher_123",
            role=PrivateComputationRole.PUBLISHER,
            status=PrivateComputationInstanceStatus.PID_MR_STARTED,
            status_update_ts=1600000000,
        )
        pc_instance = PrivateComputationInstance(
            infra_config,
            instances=[],
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            game_type=PrivateComputationGameType.LIFT,
            input_path="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test_data/stress_test/input.csv",
            output_dir="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test/output",
            pid_configs={
                "pid_mr": {
                    "PIDWorkflowConfigs": {"state_machine_arn": "machine_arn"},
                    "PIDRunConfigs": {"conf": "conf1"},
                    "sparkConfigs": {"conf-2": "conf2"},
                }
            },
        )
        flow = PrivateComputationMRStageFlow
        pc_instance._stage_flow_cls_name = flow.get_cls_name()

        service = SfnWorkflowService("us-west-2", "access_key", "access_data")
        service.start_workflow = MagicMock(return_value="execution_arn")
        service.get_workflow_status = MagicMock(return_value=WorkflowStatus.COMPLETED)
        stage_svc = PIDMRStageService(
            service,
        )
        await stage_svc.run_async(pc_instance)

        self.assertEqual(
            stage_svc.get_status(pc_instance),
            PrivateComputationInstanceStatus.PID_MR_COMPLETED,
        )
        self.assertEqual(
            pc_instance.pid_mr_stage_output_data_path,
            "https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test/output/publisher_123_out_dir/pid_mr",
        )
        self.assertEqual(pc_instance.instances[0].instance_id, "execution_arn")
        self.assertIsInstance(pc_instance.instances[0], StageStateInstance)
