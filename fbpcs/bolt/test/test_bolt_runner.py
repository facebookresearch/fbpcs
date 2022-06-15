#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from typing import List, Optional
from unittest import mock

from fbpcs.bolt.bolt_job import BoltJob
from fbpcs.bolt.bolt_runner import BoltRunner, BoltState
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)


class TestBoltRunner(unittest.IsolatedAsyncioTestCase):
    @mock.patch("fbpcs.bolt.oss_bolt_pcs.BoltPCSClient", new_callable=mock.AsyncMock)
    @mock.patch("fbpcs.bolt.oss_bolt_pcs.BoltPCSClient", new_callable=mock.AsyncMock)
    def setUp(self, mock_publisher_client, mock_partner_client) -> None:
        self.test_runner = BoltRunner(
            publisher_client=mock_publisher_client,
            partner_client=mock_partner_client,
        )

    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    async def test_joint_stage(
        self,
        mock_publisher_args,
        mock_partner_args,
    ) -> None:
        # testing that the correct server ips are used when a joint stage is run
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        test_server_ips = ["1.1.1.1"]
        mock_partner_run_stage = self.prepare_mock_client_functions(
            test_publisher_id,
            test_partner_id,
            PrivateComputationStageFlow.ID_MATCH,
            test_server_ips,
        )

        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
            stage_flow=DummyJointStageFlow,
        )

        await self.test_runner.run_async([test_job])

        mock_partner_run_stage.assert_called_with(
            instance_id=test_partner_id,
            stage=DummyJointStageFlow.JOINT_STAGE,
            server_ips=test_server_ips,
        )

    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    async def test_non_joint_stage(self, mock_publisher_args, mock_partner_args):
        # testing that server ips are not used when non-joint stage is run
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        mock_partner_run_stage = self.prepare_mock_client_functions(
            test_publisher_id, test_partner_id, PrivateComputationStageFlow.PID_SHARD
        )

        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
            stage_flow=DummyNonJointStageFlow,
        )
        await self.test_runner.run_async([test_job])

        mock_partner_run_stage.assert_called_with(
            instance_id="test_part_id",
            stage=DummyNonJointStageFlow.NON_JOINT_STAGE,
            server_ips=None,
        )

    def prepare_mock_client_functions(
        self,
        test_publisher_id: str,
        test_partner_id: str,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> mock.AsyncMock:
        self.test_runner.publisher_client.create_instance = mock.AsyncMock(
            return_value=test_publisher_id,
        )
        self.test_runner.partner_client.create_instance = mock.AsyncMock(
            return_value=test_partner_id
        )
        test_start_state = BoltState(
            pc_instance_status=stage.started_status, server_ips=server_ips
        )
        test_completed_state = BoltState(pc_instance_status=stage.completed_status)
        if server_ips:
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[test_start_state, test_completed_state]
            )
        else:
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[test_completed_state]
            )
        self.test_runner.partner_client.update_instance = mock.AsyncMock(
            side_effect=[test_completed_state]
        )
        self.test_runner.publisher_client.run_stage = mock.AsyncMock()
        mock_partner_run_stage = mock.AsyncMock()
        self.test_runner.partner_client.run_stage = mock_partner_run_stage
        return mock_partner_run_stage


class DummyJointStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )

    JOINT_STAGE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        True,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()


class DummyNonJointStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )

    NON_JOINT_STAGE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PID_SHARD_STARTED,
        PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
        PrivateComputationInstanceStatus.PID_SHARD_FAILED,
        False,
    )
