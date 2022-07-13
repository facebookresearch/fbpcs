#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from typing import List, Optional, Tuple
from unittest import mock

from fbpcs.bolt.bolt_job import BoltJob
from fbpcs.bolt.bolt_runner import BoltRunner, BoltState
from fbpcs.bolt.constants import DEFAULT_NUM_TRIES
from fbpcs.bolt.exceptions import StageFailedException
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

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    async def test_joint_stage(
        self,
        mock_publisher_args,
        mock_partner_args,
        mock_sleep,
    ) -> None:
        # testing that the correct server ips are used when a joint stage is run
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        test_server_ips = ["1.1.1.1"]
        mock_partner_run_stage = self._prepare_mock_client_functions(
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

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    async def test_non_joint_stage(
        self, mock_publisher_args, mock_partner_args, mock_sleep
    ):
        # testing that server ips are not used when non-joint stage is run
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        mock_partner_run_stage = self._prepare_mock_client_functions(
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

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch(
        "fbpcs.bolt.bolt_runner.BoltRunner.run_next_stage", new_callable=mock.AsyncMock
    )
    async def test_auto_stage_retry(
        self, mock_run_next_stage, mock_publisher_args, mock_partner_args, mock_sleep
    ) -> None:
        for is_retryable in (True, False):
            with self.subTest(is_retryable=is_retryable):
                # mock runner has default num_tries = 2
                mock_run_next_stage.reset_mock()
                mock_run_next_stage.side_effect = [Exception(1), Exception(2)]
                test_job = BoltJob(
                    job_name="test",
                    publisher_bolt_args=mock_publisher_args,
                    partner_bolt_args=mock_partner_args,
                    stage_flow=DummyRetryableStageFlow
                    if is_retryable
                    else DummyNonRetryableStageFlow,
                )
                await self.test_runner.run_async([test_job])
                if is_retryable:
                    self.assertEqual(mock_run_next_stage.call_count, DEFAULT_NUM_TRIES)
                else:
                    self.assertEqual(mock_run_next_stage.call_count, 1)

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    async def test_auto_stage_retry_one_sided_failure(self, mock_sleep) -> None:
        for failing_side in ("publisher", "partner"):
            (
                mock_publisher_run_stage,
                mock_partner_run_stage,
                test_job,
            ) = self._prepare_one_sided_failure_retry(failing_side=failing_side)
            with self.subTest(failing_side=failing_side):
                # if one side fails but the other doesn't and it's not a joint stage,
                # only the failing side should retry. The joint stage case involves cancelling,
                # which is tested separately
                await self.test_runner.run_async([test_job])
                if failing_side == "publisher":
                    self.assertEquals(mock_publisher_run_stage.call_count, 2)
                    self.assertEquals(mock_partner_run_stage.call_count, 1)
                else:
                    self.assertEquals(mock_publisher_run_stage.call_count, 1)
                    self.assertEquals(mock_partner_run_stage.call_count, 2)

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    async def test_wait_stage_complete(self, mock_sleep) -> None:
        for (
            stage,
            publisher_statuses,
            partner_statuses,
            result,
        ) in self._get_wait_stage_complete_data():
            self.test_runner.partner_client.cancel_current_stage = mock.AsyncMock()
            with self.subTest(
                stage=stage,
                publisher_statuses=publisher_statuses,
                partner_statuses=partner_statuses,
                result=result,
            ):
                self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                    side_effect=[BoltState(status) for status in publisher_statuses]
                )
                self.test_runner.partner_client.update_instance = mock.AsyncMock(
                    side_effect=[BoltState(status) for status in partner_statuses]
                )

                if not result:
                    with self.assertRaises(StageFailedException):
                        # stage should fail and raise an exception
                        await self.test_runner.wait_stage_complete(
                            publisher_id="test_pub_id",
                            partner_id="test_part_id",
                            stage=stage,
                            poll_interval=5,
                        )

                    if stage.is_joint_stage:
                        # make sure it calls cancel_current_stage
                        self.test_runner.partner_client.cancel_current_stage.assert_called_once_with(
                            instance_id="test_part_id"
                        )
                    else:
                        self.test_runner.partner_client.cancel_current_stage.assert_not_called()
                else:
                    # stage should succeed
                    await self.test_runner.wait_stage_complete(
                        publisher_id="test_pub_id",
                        partner_id="test_part_id",
                        stage=stage,
                        poll_interval=5,
                    )
                    self.test_runner.partner_client.cancel_current_stage.assert_not_called()

    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    def _prepare_one_sided_failure_retry(
        self, mock_publisher_args, mock_partner_args, failing_side: str
    ) -> Tuple[mock.AsyncMock, mock.AsyncMock, BoltJob]:
        mock_publisher_run_stage = mock.AsyncMock()
        mock_partner_run_stage = mock.AsyncMock()
        self.test_runner.publisher_client.run_stage = mock_publisher_run_stage
        self.test_runner.partner_client.run_stage = mock_partner_run_stage
        if failing_side == "publisher":
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_STARTED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_FAILED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_FAILED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                ]
            )
            self.test_runner.partner_client.update_instance = mock.AsyncMock(
                side_effect=[
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_STARTED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                ]
            )
        if failing_side == "partner":
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_STARTED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                ]
            )
            self.test_runner.partner_client.update_instance = mock.AsyncMock(
                side_effect=[
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_STARTED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_FAILED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_FAILED),
                    BoltState(PrivateComputationInstanceStatus.PID_SHARD_COMPLETED),
                ]
            )
        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
            stage_flow=DummyNonJointStageFlow,
        )
        return mock_publisher_run_stage, mock_partner_run_stage, test_job

    def _prepare_mock_client_functions(
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
                side_effect=[test_start_state, test_start_state, test_completed_state]
            )
        else:
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[test_start_state, test_completed_state]
            )
        self.test_runner.partner_client.update_instance = mock.AsyncMock(
            side_effect=[test_start_state, test_completed_state]
        )
        self.test_runner.publisher_client.run_stage = mock.AsyncMock()
        mock_partner_run_stage = mock.AsyncMock()
        self.test_runner.partner_client.run_stage = mock_partner_run_stage
        return mock_partner_run_stage

    def _get_wait_stage_complete_data(
        self,
    ) -> List[
        Tuple[
            PrivateComputationBaseStageFlow,
            List[PrivateComputationInstanceStatus],
            List[PrivateComputationInstanceStatus],
            bool,
        ]
    ]:
        """
        Tuple represents:
            * Stage
            * Order of the publisher statuses
            * Order of the partner statuses
            * Does the stage succeed
        """
        return [
            (
                PrivateComputationStageFlow.ID_MATCH,
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                ],
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                    PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.ID_MATCH,
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                ],
                [
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
                ],
                False,
            ),
            (
                PrivateComputationStageFlow.PC_PRE_VALIDATION,
                [
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
                ],
                [
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
                ],
                True,
            ),
            (
                PrivateComputationStageFlow.PC_PRE_VALIDATION,
                [
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
                ],
                [
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
                    PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
                ],
                False,
            ),
        ]


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


class DummyRetryableStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )

    RETRYABLE_STAGE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
        PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
        PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
        False,
        is_retryable=True,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()


class DummyNonRetryableStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.CREATION_STARTED,
        PrivateComputationInstanceStatus.CREATED,
        PrivateComputationInstanceStatus.CREATION_FAILED,
        False,
    )

    NON_RETRYABLE_STAGE = PrivateComputationStageFlowData(
        PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        True,
        is_retryable=False,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()
