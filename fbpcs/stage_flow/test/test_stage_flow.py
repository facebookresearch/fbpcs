#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase

from fbpcs.stage_flow.exceptions import StageFlowStageNotFoundError
from fbpcs.stage_flow.test.dummy_stage_flow import (
    DummyStageFlow,
    DummyStageFlowStatus,
)


class TestStageFlow(TestCase):
    def test_get_first_stage(self):
        self.assertEqual(DummyStageFlow.STAGE_1, DummyStageFlow.get_first_stage())

    def test_get_last_stage(self):
        self.assertEqual(DummyStageFlow.STAGE_3, DummyStageFlow.get_last_stage())

    def test_move_forward(self):
        stage = DummyStageFlow.get_first_stage()
        self.assertEqual(DummyStageFlow.STAGE_1, stage)
        self.assertEqual(DummyStageFlow.STAGE_2, stage.next_stage)
        self.assertEqual(DummyStageFlow.STAGE_3, stage.next_stage.next_stage)
        self.assertEqual(None, stage.next_stage.next_stage.next_stage)

    def test_move_backwards(self):
        stage = DummyStageFlow.get_last_stage()
        self.assertEqual(DummyStageFlow.STAGE_3, stage)
        self.assertEqual(DummyStageFlow.STAGE_2, stage.previous_stage)
        self.assertEqual(DummyStageFlow.STAGE_1, stage.previous_stage.previous_stage)
        self.assertEqual(None, stage.previous_stage.previous_stage.previous_stage)

    def test_is_started_status(self):
        start_statuses = [
            DummyStageFlowStatus.STAGE_1_STARTED,
            DummyStageFlowStatus.STAGE_2_STARTED,
            DummyStageFlowStatus.STAGE_3_STARTED,
        ]
        other_statuses = [
            DummyStageFlowStatus.STAGE_1_FAILED,
            DummyStageFlowStatus.STAGE_1_COMPLETED,
            DummyStageFlowStatus.STAGE_2_FAILED,
            DummyStageFlowStatus.STAGE_2_COMPLETED,
            DummyStageFlowStatus.STAGE_3_FAILED,
            DummyStageFlowStatus.STAGE_3_COMPLETED,
        ]

        self.assertTrue(
            all(DummyStageFlow.is_started_status(status) for status in start_statuses)
        )
        self.assertTrue(
            all(
                not DummyStageFlow.is_started_status(status)
                for status in other_statuses
            )
        )

    def test_get_stage_from_status(self):
        stage_1_statuses = [
            DummyStageFlowStatus.STAGE_1_COMPLETED,
            DummyStageFlowStatus.STAGE_1_FAILED,
            DummyStageFlowStatus.STAGE_1_STARTED,
        ]
        stage_2_statuses = [
            DummyStageFlowStatus.STAGE_2_COMPLETED,
            DummyStageFlowStatus.STAGE_2_FAILED,
            DummyStageFlowStatus.STAGE_2_STARTED,
        ]
        stage_3_statuses = [
            DummyStageFlowStatus.STAGE_3_COMPLETED,
            DummyStageFlowStatus.STAGE_3_FAILED,
            DummyStageFlowStatus.STAGE_3_STARTED,
        ]

        for stage, statuses in zip(
            (DummyStageFlow.STAGE_1, DummyStageFlow.STAGE_2, DummyStageFlow.STAGE_3),
            (stage_1_statuses, stage_2_statuses, stage_3_statuses),
        ):
            for status in statuses:
                self.assertIs(stage, DummyStageFlow.get_stage_from_status(status))

    def test_get_next_runnable_stage_from_status(self):
        stage_1_is_next = [DummyStageFlowStatus.STAGE_1_FAILED]
        stage_2_is_next = [
            DummyStageFlowStatus.STAGE_1_COMPLETED,
            DummyStageFlowStatus.STAGE_2_FAILED,
        ]
        stage_3_is_next = [
            DummyStageFlowStatus.STAGE_2_COMPLETED,
            DummyStageFlowStatus.STAGE_3_FAILED,
        ]
        nothing_is_next = [
            DummyStageFlowStatus.STAGE_1_STARTED,
            DummyStageFlowStatus.STAGE_2_STARTED,
            DummyStageFlowStatus.STAGE_3_STARTED,
            DummyStageFlowStatus.STAGE_3_COMPLETED,
        ]

        for stage, statuses in zip(
            (
                DummyStageFlow.STAGE_1,
                DummyStageFlow.STAGE_2,
                DummyStageFlow.STAGE_3,
                None,
            ),
            (stage_1_is_next, stage_2_is_next, stage_3_is_next, nothing_is_next),
        ):
            for status in statuses:
                self.assertIs(
                    stage, DummyStageFlow.get_next_runnable_stage_from_status(status)
                )

    def test_get_stage_from_name(self):
        # setup
        expected_stage = DummyStageFlow.get_first_stage()

        # test that lower case works
        actual_stage = DummyStageFlow.get_stage_from_str(expected_stage.name.lower())
        self.assertEqual(expected_stage, actual_stage)

        # test that uppercase works
        actual_stage = DummyStageFlow.get_stage_from_str(expected_stage.name.upper())
        self.assertEqual(expected_stage, actual_stage)

        # test that non existent stages raise error
        with self.assertRaises(StageFlowStageNotFoundError):
            DummyStageFlow.get_stage_from_str(
                "do not name your stage this or you will be fired"
            )
