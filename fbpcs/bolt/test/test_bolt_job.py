#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest import mock

from fbpcs.bolt.bolt_job import BoltJob
from fbpcs.bolt.exceptions import IncompatibleStageError
from fbpcs.private_computation.stage_flows.private_computation_decoupled_stage_flow import (
    PrivateComputationDecoupledStageFlow,
)

from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)


class TestBoltJob(unittest.TestCase):
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    @mock.patch("fbpcs.bolt.bolt_job.BoltPlayerArgs")
    def test_job_is_finished(
        self,
        mock_publisher_args,
        mock_partner_args,
    ) -> None:
        for (
            job_final_stage,
            publisher_status,
            partner_status,
            stage_flow,
            expected_result,
            expected_side_effect,
        ) in self._get_test_data():
            with self.subTest(
                job_final_stage=job_final_stage,
                publisher_status=publisher_status,
                partner_status=partner_status,
                stage_flow=stage_flow,
                expected_result=expected_result,
                expected_side_effect=expected_side_effect,
            ):
                job = BoltJob(
                    job_name="test",
                    publisher_bolt_args=mock_publisher_args,
                    partner_bolt_args=mock_partner_args,
                    final_stage=job_final_stage,
                )
                if expected_side_effect:
                    with self.assertRaises(IncompatibleStageError):
                        job.is_finished(
                            publisher_status=publisher_status,
                            partner_status=partner_status,
                            stage_flow=stage_flow,
                        )
                else:
                    result = job.is_finished(
                        publisher_status=publisher_status,
                        partner_status=partner_status,
                        stage_flow=stage_flow,
                    )
                    self.assertEqual(expected_result, result)

    def _get_test_data(self):
        # job_final_stage, publisher_status, partner_status, stage_flow, expected_result, expected_side_effect,
        return (
            (
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                PrivateComputationStageFlow,
                True,
                False,
            ),
            (
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                PrivateComputationStageFlow.COMPUTE.completed_status,
                PrivateComputationStageFlow,
                False,
                False,
            ),
            (  # final_stage is None, taking stage flow
                None,
                PrivateComputationStageFlow.get_last_stage().completed_status,
                PrivateComputationStageFlow.get_last_stage().completed_status,
                PrivateComputationStageFlow,
                True,
                False,
            ),
            (  # Expect exception
                PrivateComputationStageFlow.AGGREGATE,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                PrivateComputationStageFlow.AGGREGATE.completed_status,
                PrivateComputationDecoupledStageFlow,
                False,
                True,
            ),
        )
