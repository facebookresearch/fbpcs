#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from typing import List, Optional, Tuple
from unittest import mock

from fbpcs.bolt.bolt_client import BoltState
from fbpcs.bolt.bolt_job import BoltJob
from fbpcs.bolt.bolt_runner import BoltRunner
from fbpcs.bolt.constants import DEFAULT_NUM_TRIES
from fbpcs.bolt.exceptions import IncompatibleStageError, StageFailedException

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
        self.test_runner.job_is_finished = mock.AsyncMock(return_value=False)
        self.test_runner.wait_valid_publisher_status = mock.AsyncMock()

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_next_valid_stage")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_stage_flow")
    async def test_joint_stage(
        self,
        mock_get_stage_flow,
        mock_next_stage,
        mock_publisher_args,
        mock_partner_args,
        mock_sleep,
    ) -> None:
        mock_get_stage_flow.return_value = DummyJointStageFlow
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        self.test_runner.publisher_client.get_or_create_instance = mock.AsyncMock(
            return_value=test_publisher_id
        )
        self.test_runner.partner_client.get_or_create_instance = mock.AsyncMock(
            return_value=test_partner_id
        )
        # testing that the correct server ips are used when a joint stage is run
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
        )
        mock_next_stage.return_value = DummyJointStageFlow.JOINT_STAGE

        await self.test_runner.run_async([test_job])

        mock_partner_run_stage.assert_called_with(
            instance_id=test_partner_id,
            stage=DummyJointStageFlow.JOINT_STAGE,
            server_ips=test_server_ips,
        )

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_next_valid_stage")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_stage_flow")
    async def test_non_joint_stage(
        self,
        mock_get_stage_flow,
        mock_next_stage,
        mock_publisher_args,
        mock_partner_args,
        mock_sleep,
    ):
        mock_get_stage_flow.return_value = DummyNonJointStageFlow
        test_publisher_id = "test_pub_id"
        test_partner_id = "test_part_id"
        # testing that server ips are not used when non-joint stage is run
        self.test_runner.publisher_client.get_or_create_instance = mock.AsyncMock(
            return_value=test_publisher_id
        )
        self.test_runner.partner_client.get_or_create_instance = mock.AsyncMock(
            return_value=test_partner_id
        )
        mock_partner_run_stage = self._prepare_mock_client_functions(
            test_publisher_id, test_partner_id, PrivateComputationStageFlow.PID_SHARD
        )

        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
        )
        mock_next_stage.return_value = DummyNonJointStageFlow.NON_JOINT_STAGE
        await self.test_runner.run_async([test_job])

        mock_partner_run_stage.assert_called_with(
            instance_id="test_part_id",
            stage=DummyNonJointStageFlow.NON_JOINT_STAGE,
            server_ips=None,
        )

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    async def test_get_server_ips_after_start(self, mock_sleep) -> None:
        mock_server_ips = ["1.1.1.1"]
        for (test_stage, test_publisher_status) in (
            (  # test Non-joint stage should get None server ips
                DummyNonJointStageFlow.NON_JOINT_STAGE,
                DummyNonJointStageFlow.NON_JOINT_STAGE.completed_status,
            ),
            (
                DummyJointStageFlow.JOINT_STAGE,
                DummyJointStageFlow.JOINT_STAGE.started_status,
            ),
            (
                DummyJointStageFlow.JOINT_STAGE,
                DummyJointStageFlow.JOINT_STAGE.failed_status,
            ),
            (
                DummyJointStageFlow.JOINT_STAGE,
                DummyJointStageFlow.JOINT_STAGE.completed_status,
            ),
        ):
            with self.subTest(
                test_stage=test_stage,
                test_publisher_status=test_publisher_status,
            ):
                self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(
                        pc_instance_status=test_publisher_status,
                        server_ips=mock_server_ips,
                    )
                )
                if (
                    test_publisher_status is test_stage.started_status
                    or not test_stage.is_joint_stage
                ):
                    server_ips = await self.test_runner.get_server_ips_after_start(
                        instance_id="publisher_id",
                        stage=test_stage,
                        timeout=10,
                        poll_interval=5,
                    )
                    if test_stage.is_joint_stage:
                        self.assertEqual(server_ips, mock_server_ips)
                    else:
                        self.assertEqual(server_ips, None)
                else:
                    with self.assertRaises(StageFailedException):
                        await self.test_runner.get_server_ips_after_start(
                            instance_id="publisher_id",
                            stage=test_stage,
                            timeout=10,
                            poll_interval=5,
                        )

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    async def test_joint_stage_retry_gets_ips(self, mock_sleep) -> None:
        # test that server ips are gotten when a joint stage is retried with STARTED status
        # specifically, publisher status STARTED and partner status FAILED
        server_ips = ["1.1.1.1"]
        self.test_runner.get_server_ips_after_start = mock.AsyncMock(
            return_value=server_ips
        )
        self.test_runner.publisher_client.update_instance = mock.AsyncMock(
            return_value=BoltState(DummyJointStageFlow.JOINT_STAGE.started_status)
        )
        self.test_runner.partner_client.update_instance = mock.AsyncMock(
            return_value=BoltState(DummyJointStageFlow.JOINT_STAGE.failed_status)
        )
        mock_partner_run_stage = mock.AsyncMock()
        self.test_runner.partner_client.run_stage = mock_partner_run_stage
        await self.test_runner.run_next_stage(
            publisher_id="publisher_id",
            partner_id="partner_id",
            stage=DummyJointStageFlow.JOINT_STAGE,
            poll_interval=5,
        )
        mock_partner_run_stage.assert_called_once_with(
            instance_id="partner_id",
            stage=DummyJointStageFlow.JOINT_STAGE,
            server_ips=server_ips,
        )

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch(
        "fbpcs.bolt.bolt_runner.BoltRunner.run_next_stage", new_callable=mock.AsyncMock
    )
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_next_valid_stage")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_stage_flow")
    async def test_auto_stage_retry(
        self,
        mock_get_stage_flow,
        mock_next_stage,
        mock_run_next_stage,
        mock_publisher_args,
        mock_partner_args,
        mock_sleep,
    ) -> None:
        for is_retryable in (True, False):
            with self.subTest(is_retryable=is_retryable):
                # mock runner has default num_tries = 2
                mock_get_stage_flow.reset_mock()
                mock_run_next_stage.reset_mock()
                mock_run_next_stage.side_effect = [Exception(1), Exception(2)]
                test_job = BoltJob(
                    job_name="test",
                    publisher_bolt_args=mock_publisher_args,
                    partner_bolt_args=mock_partner_args,
                )
                mock_next_stage.return_value = (
                    DummyRetryableStageFlow.RETRYABLE_STAGE
                    if is_retryable
                    else DummyNonRetryableStageFlow.NON_RETRYABLE_STAGE
                )
                mock_get_stage_flow.return_value = (
                    DummyRetryableStageFlow
                    if is_retryable
                    else DummyNonRetryableStageFlow
                )
                await self.test_runner.run_async([test_job])
                if is_retryable:
                    self.assertEqual(mock_run_next_stage.call_count, DEFAULT_NUM_TRIES)
                else:
                    self.assertEqual(mock_run_next_stage.call_count, 1)

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_next_valid_stage")
    @mock.patch("fbpcs.bolt.bolt_runner.BoltRunner.get_stage_flow")
    async def test_auto_stage_retry_one_sided_failure(
        self, mock_get_stage_flow, mock_next_stage, mock_sleep
    ) -> None:
        stage = DummyNonJointStageFlow.NON_JOINT_STAGE
        for publisher_fails in (True, False):
            with self.subTest(publisher_fails=publisher_fails):
                mock_publisher_run_stage = mock.AsyncMock()
                mock_partner_run_stage = mock.AsyncMock()
                self.test_runner.publisher_client.run_stage = mock_publisher_run_stage
                self.test_runner.partner_client.run_stage = mock_partner_run_stage
                self.test_runner.publisher_client.should_invoke_stage = mock.AsyncMock(
                    return_value=publisher_fails
                )
                self.test_runner.partner_client.should_invoke_stage = mock.AsyncMock(
                    return_value=not publisher_fails
                )
                # if one side fails but the other doesn't and it's not a joint stage,
                # only the failing side should retry. The joint stage case involves cancelling,
                # which is tested separately
                await self.test_runner.run_next_stage(
                    "publisher_id", "partner_id", stage, poll_interval=5
                )
                if publisher_fails:
                    mock_publisher_run_stage.assert_called_once()
                    mock_partner_run_stage.assert_not_called()
                else:
                    mock_publisher_run_stage.assert_not_called()
                    mock_partner_run_stage.assert_called_once()

    @mock.patch("fbpcs.bolt.bolt_runner.asyncio.sleep")
    @mock.patch("fbpcs.bolt.bolt_client.BoltClient.has_feature")
    async def test_wait_stage_complete(self, mock_sleep, mock_has_feature) -> None:
        for (
            stage,
            publisher_statuses,
            partner_statuses,
            result,
        ) in self._get_wait_stage_complete_data():
            self.test_runner.partner_client.cancel_current_stage = mock.AsyncMock()
            mock_has_feature.return_value = True
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
                        self.test_runner.publisher_client.cancel_current_stage.assert_called_once_with(
                            instance_id="test_pub_id"
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
    async def test_get_stage_flow(self, mock_publisher_args, mock_partner_args) -> None:
        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
            final_stage=PrivateComputationStageFlow.AGGREGATE,
        )
        for (
            publisher_stage_flow,
            partner_stage_flow,
            expect_exception,
            expect_final_stage_flow,
        ) in (
            (  # happy case
                PrivateComputationStageFlow,
                PrivateComputationStageFlow,
                None,
                PrivateComputationStageFlow,
            ),
            (  # publisher-partner stageflow inconsistency
                PrivateComputationStageFlow,
                DummyNonJointStageFlow,
                IncompatibleStageError,
                None,
            ),
            (PrivateComputationStageFlow, None, None, PrivateComputationStageFlow),
            (None, PrivateComputationStageFlow, None, PrivateComputationStageFlow),
            (None, None, IncompatibleStageError, PrivateComputationStageFlow),
        ):
            with self.subTest(
                publisher_stage_flow=publisher_stage_flow,
                partner_stage_flow=partner_stage_flow,
                expect_exception=expect_exception,
                expect_final_stage_flow=expect_final_stage_flow,
            ):
                self.test_runner.publisher_client.get_stage_flow = mock.AsyncMock(
                    return_value=publisher_stage_flow,
                )
                self.test_runner.partner_client.get_stage_flow = mock.AsyncMock(
                    return_value=partner_stage_flow,
                )
                if expect_exception is not None:
                    with self.assertRaises(IncompatibleStageError):
                        await self.test_runner.get_stage_flow(job=test_job)

                else:
                    stage_flow = await self.test_runner.get_stage_flow(job=test_job)
                    self.assertEqual(stage_flow, expect_final_stage_flow)

    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    async def test_get_next_valid_stage(
        self, mock_publisher_args, mock_partner_args
    ) -> None:
        test_job = BoltJob(
            job_name="test",
            publisher_bolt_args=mock_publisher_args,
            partner_bolt_args=mock_partner_args,
            final_stage=PrivateComputationStageFlow.AGGREGATE,
        )
        for (
            publisher_status,
            publisher_next_stage,
            partner_status,
            partner_next_stage,
            expected_next_stage,
        ) in self._get_valid_stage_data():
            with self.subTest(
                publisher_status=publisher_status,
                partner_status=partner_status,
                expected_next_stage=expected_next_stage,
            ):
                self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(publisher_status)
                )
                self.test_runner.partner_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(partner_status)
                )
                self.test_runner.publisher_client.get_valid_stage = mock.AsyncMock(
                    return_value=publisher_next_stage
                )
                self.test_runner.partner_client.get_valid_stage = mock.AsyncMock(
                    return_value=partner_next_stage
                )
                next_valid_stage = await self.test_runner.get_next_valid_stage(
                    job=test_job,
                    stage_flow=PrivateComputationStageFlow,
                )
                self.assertEqual(next_valid_stage, expected_next_stage)
        for (
            publisher_status,
            publisher_next_stage,
            partner_status,
            partner_next_stage,
        ) in self._get_incompatible_stage_data():
            with self.subTest(
                "Testing incompatible stages",
                publisher_status=publisher_status,
                partner_status=partner_status,
            ):
                self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(publisher_status)
                )
                self.test_runner.partner_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(partner_status)
                )
                self.test_runner.publisher_client.get_valid_stage = mock.AsyncMock(
                    return_value=publisher_next_stage
                )
                self.test_runner.partner_client.get_valid_stage = mock.AsyncMock(
                    return_value=partner_next_stage
                )
                with self.assertRaises(IncompatibleStageError):
                    next_valid_stage = await self.test_runner.get_next_valid_stage(
                        job=test_job,
                        stage_flow=PrivateComputationStageFlow,
                    )

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
        test_previous_completed_state = BoltState(
            pc_instance_status=PrivateComputationInstanceStatus.CREATED
        )
        test_start_state = BoltState(
            pc_instance_status=stage.started_status, server_ips=server_ips
        )
        test_completed_state = BoltState(pc_instance_status=stage.completed_status)
        if server_ips:
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[
                    test_previous_completed_state,
                    test_start_state,
                    test_completed_state,
                ]
            )
        else:
            self.test_runner.publisher_client.update_instance = mock.AsyncMock(
                side_effect=[test_previous_completed_state, test_completed_state]
            )
        self.test_runner.partner_client.update_instance = mock.AsyncMock(
            side_effect=[test_previous_completed_state, test_completed_state]
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

    def _get_valid_stage_data(
        self,
    ) -> List[
        Tuple[
            PrivateComputationInstanceStatus,
            Optional[PrivateComputationBaseStageFlow],
            PrivateComputationInstanceStatus,
            Optional[PrivateComputationBaseStageFlow],
            Optional[PrivateComputationBaseStageFlow],
        ]
    ]:
        """
        Tuple represents:
            * publisher status
            * next valid publisher stage
            * partner status
            * next valid partner stage
            * next expected valid stage
        """
        return [
            (
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationStageFlow.CREATED.next_stage,
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationStageFlow.CREATED.next_stage,
                PrivateComputationStageFlow.CREATED.next_stage,
            ),
            (
                PrivateComputationStageFlow.ID_MATCH.started_status,
                PrivateComputationStageFlow.ID_MATCH,
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_MATCH.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_MATCH,
                PrivateComputationStageFlow.ID_MATCH,
            ),
            (
                PrivateComputationStageFlow.ID_MATCH.started_status,
                PrivateComputationStageFlow.ID_MATCH,
                PrivateComputationStageFlow.ID_MATCH.started_status,
                PrivateComputationStageFlow.ID_MATCH,
                PrivateComputationStageFlow.ID_MATCH,
            ),
            (
                PrivateComputationStageFlow.ID_MATCH.failed_status,
                PrivateComputationStageFlow.ID_MATCH,
                PrivateComputationStageFlow.ID_MATCH.failed_status,
                PrivateComputationStageFlow.ID_MATCH,
                PrivateComputationStageFlow.ID_MATCH,
            ),
            (
                PrivateComputationStageFlow.ID_MATCH.completed_status,
                PrivateComputationStageFlow.ID_MATCH.next_stage,
                PrivateComputationStageFlow.ID_MATCH.completed_status,
                PrivateComputationStageFlow.ID_MATCH.next_stage,
                PrivateComputationStageFlow.ID_MATCH.next_stage,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.started_status,
                PrivateComputationStageFlow.COMPUTE,
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.COMPUTE.previous_stage.completed_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.started_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE.started_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.failed_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE.failed_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.COMPUTE.started_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.started_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.COMPUTE.next_stage,
            ),
            (
                PrivateComputationStageFlow.AGGREGATE.started_status,
                PrivateComputationStageFlow.AGGREGATE,
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.AGGREGATE.previous_stage.completed_status,
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE,
            ),
            (
                PrivateComputationStageFlow.AGGREGATE.started_status,
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE.started_status,
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE,
            ),
            (
                PrivateComputationStageFlow.AGGREGATE.failed_status,
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE.failed_status,
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE,
            ),
            (
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                None,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                None,
                None,
            ),
            ####################### NON JOINT STAGE TEST #################################3
            (
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.previous_stage.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.failed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.started_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.failed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
                PrivateComputationStageFlow.ID_SPINE_COMBINER,
            ),
            (
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.completed_status,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
                PrivateComputationStageFlow.ID_SPINE_COMBINER.next_stage,
            ),
        ]

    def _get_incompatible_stage_data(
        self,
    ) -> List[
        Tuple[
            PrivateComputationInstanceStatus,
            Optional[PrivateComputationBaseStageFlow],
            PrivateComputationInstanceStatus,
            Optional[PrivateComputationBaseStageFlow],
        ]
    ]:
        return [
            (
                PrivateComputationStageFlow.PID_PREPARE.completed_status,
                PrivateComputationStageFlow.PID_PREPARE.next_stage,
                PrivateComputationInstanceStatus.CREATED,
                PrivateComputationStageFlow.CREATED.next_stage,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.RESHARD.completed_status,
                PrivateComputationStageFlow.RESHARD.next_stage,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
                PrivateComputationStageFlow.COMPUTE.failed_status,
                PrivateComputationStageFlow.COMPUTE,
            ),
            (
                PrivateComputationStageFlow.COMPUTE.failed_status,
                PrivateComputationStageFlow.COMPUTE,
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow.COMPUTE.next_stage,
            ),
            (
                PrivateComputationStageFlow.CREATED.completed_status,
                PrivateComputationStageFlow.CREATED.next_stage,
                PrivateComputationStageFlow.PID_SHARD.failed_status,
                PrivateComputationStageFlow.PID_SHARD,
            ),
            (
                PrivateComputationStageFlow.PID_SHARD.failed_status,
                PrivateComputationStageFlow.PID_SHARD,
                PrivateComputationStageFlow.CREATED.completed_status,
                PrivateComputationStageFlow.CREATED.next_stage,
            ),
        ]


class DummyJointStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.CREATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.CREATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.CREATED,
        failed_status=PrivateComputationInstanceStatus.CREATION_FAILED,
        is_joint_stage=False,
    )

    JOINT_STAGE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.ID_MATCHING_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        completed_status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        is_joint_stage=True,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()


class DummyNonJointStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.CREATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.CREATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.CREATED,
        failed_status=PrivateComputationInstanceStatus.CREATION_FAILED,
        is_joint_stage=False,
    )

    NON_JOINT_STAGE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.PID_SHARD_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.PID_SHARD_STARTED,
        completed_status=PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.PID_SHARD_FAILED,
        is_joint_stage=False,
    )


class DummyRetryableStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.CREATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.CREATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.CREATED,
        failed_status=PrivateComputationInstanceStatus.CREATION_FAILED,
        is_joint_stage=False,
    )

    RETRYABLE_STAGE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.PC_PRE_VALIDATION_FAILED,
        is_joint_stage=False,
        is_retryable=True,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()


class DummyNonRetryableStageFlow(PrivateComputationBaseStageFlow):
    CREATED = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.CREATION_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.CREATION_STARTED,
        completed_status=PrivateComputationInstanceStatus.CREATED,
        failed_status=PrivateComputationInstanceStatus.CREATION_FAILED,
        is_joint_stage=False,
    )

    NON_RETRYABLE_STAGE = PrivateComputationStageFlowData(
        initialized_status=PrivateComputationInstanceStatus.ID_MATCHING_INITIALIZED,
        started_status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        completed_status=PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
        failed_status=PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
        is_joint_stage=True,
        is_retryable=False,
    )

    def get_stage_service(
        self, args: PrivateComputationStageServiceArgs
    ) -> PrivateComputationStageService:
        raise NotImplementedError()
