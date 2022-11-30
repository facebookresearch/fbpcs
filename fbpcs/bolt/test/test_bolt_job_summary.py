#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from unittest import TestCase

from fbpcs.bolt.bolt_job_summary import BoltJobSummary, BoltMetric, BoltMetricType
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole
from fbpcs.private_computation.test.service.dummy_stage_flow import DummyStageFlow


class TestBoltJobSummary(TestCase):
    def setUp(self) -> None:
        self.job_metrics = [
            BoltMetric(BoltMetricType.JOB_QUEUE_TIME, 1),
            BoltMetric(BoltMetricType.JOB_RUN_TIME, 11),
        ]
        self.stage_metrics = [
            BoltMetric(BoltMetricType.STAGE_START_UP_TIME, 2, DummyStageFlow.STAGE_1),
            BoltMetric(
                BoltMetricType.STAGE_WAIT_FOR_COMPLETED, 22, DummyStageFlow.STAGE_1
            ),
            BoltMetric(BoltMetricType.STAGE_TOTAL_RUNTIME, 222, DummyStageFlow.STAGE_1),
        ]
        self.partner_metrics = [
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                4,
                DummyStageFlow.STAGE_1,
                PrivateComputationRole.PARTNER,
            ),
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                44,
                DummyStageFlow.STAGE_1,
                PrivateComputationRole.PARTNER,
            ),
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                444,
                DummyStageFlow.STAGE_2,
                PrivateComputationRole.PARTNER,
            ),
        ]
        self.publisher_metrics = [
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                3,
                DummyStageFlow.STAGE_1,
                PrivateComputationRole.PUBLISHER,
            ),
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                33,
                DummyStageFlow.STAGE_1,
                PrivateComputationRole.PUBLISHER,
            ),
            BoltMetric(
                BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                333,
                DummyStageFlow.STAGE_2,
                PrivateComputationRole.PUBLISHER,
            ),
        ]

        self.job_summary_with_all_metrics = BoltJobSummary(
            job_name="job_summary_with_all_metrics",
            publisher_instance_id="publisher_failed",
            partner_instance_id="partner_failed",
            is_success=False,
            bolt_metrics=self.job_metrics
            + self.stage_metrics
            + self.partner_metrics
            + self.publisher_metrics,
        )
        self.job_summary_with_job_metrics = BoltJobSummary(
            job_name="job_summary_with_job_metrics",
            publisher_instance_id="publisher_failed",
            partner_instance_id="partner_failed",
            is_success=False,
            bolt_metrics=self.job_metrics,
        )
        self.job_summary_with_stage_metrics = BoltJobSummary(
            job_name="job_summary_with_stage_metrics",
            publisher_instance_id="publisher_failed",
            partner_instance_id="partner_failed",
            is_success=False,
            bolt_metrics=self.stage_metrics
            + self.partner_metrics
            + self.publisher_metrics,
        )
        self.job_summary_with_partner_metrics = BoltJobSummary(
            job_name="job_summary_with_partner_metrics",
            publisher_instance_id="publisher_failed",
            partner_instance_id="partner_failed",
            is_success=False,
            bolt_metrics=self.partner_metrics,
        )
        self.job_summary_with_publisher_metrics = BoltJobSummary(
            job_name="job_summary_with_publisher_metrics",
            publisher_instance_id="publisher_failed",
            partner_instance_id="partner_failed",
            is_success=False,
            bolt_metrics=self.publisher_metrics,
        )

    def test_job_metrics(self) -> None:
        with self.subTest("job_summary_with_all_metrics"):
            self.assertListEqual(
                self.job_summary_with_all_metrics.job_metrics, self.job_metrics
            )

        with self.subTest("job_summary_with_job_metrics"):
            self.assertListEqual(
                self.job_summary_with_job_metrics.job_metrics, self.job_metrics
            )

        with self.subTest("job_summary_with_stage_metrics"):
            self.assertListEqual(self.job_summary_with_stage_metrics.job_metrics, [])

        with self.subTest("job_summary_with_partner_metrics"):
            self.assertListEqual(self.job_summary_with_partner_metrics.job_metrics, [])

        with self.subTest("job_summary_with_publisher_metrics"):
            self.assertListEqual(
                self.job_summary_with_publisher_metrics.job_metrics, []
            )

    def test_stage_metrics(self) -> None:
        with self.subTest("job_summary_with_all_metrics"):
            self.assertListEqual(
                self.job_summary_with_all_metrics.stage_metrics,
                self.stage_metrics + self.partner_metrics + self.publisher_metrics,
            )

        with self.subTest("job_summary_with_job_metrics"):
            self.assertListEqual(self.job_summary_with_job_metrics.stage_metrics, [])

        with self.subTest("job_summary_with_stage_metrics"):
            self.assertListEqual(
                self.job_summary_with_stage_metrics.stage_metrics,
                self.stage_metrics + self.partner_metrics + self.publisher_metrics,
            )

        with self.subTest("job_summary_with_partner_metrics"):
            self.assertListEqual(
                self.job_summary_with_partner_metrics.stage_metrics,
                self.partner_metrics,
            )

        with self.subTest("job_summary_with_publisher_metrics"):
            self.assertListEqual(
                self.job_summary_with_publisher_metrics.stage_metrics,
                self.publisher_metrics,
            )

    def test_partner_metrics(self) -> None:
        with self.subTest("job_summary_with_all_metrics"):
            self.assertListEqual(
                self.job_summary_with_all_metrics.partner_metrics,
                self.partner_metrics,
            )

        with self.subTest("job_summary_with_job_metrics"):
            self.assertListEqual(self.job_summary_with_job_metrics.partner_metrics, [])

        with self.subTest("job_summary_with_stage_metrics"):
            self.assertListEqual(
                self.job_summary_with_stage_metrics.partner_metrics,
                self.partner_metrics,
            )

        with self.subTest("job_summary_with_partner_metrics"):
            self.assertListEqual(
                self.job_summary_with_partner_metrics.partner_metrics,
                self.partner_metrics,
            )

        with self.subTest("job_summary_with_publisher_metrics"):
            self.assertListEqual(
                self.job_summary_with_publisher_metrics.partner_metrics, []
            )

    def test_publisher_metrics(self) -> None:
        with self.subTest("job_summary_with_all_metrics"):
            self.assertListEqual(
                self.job_summary_with_all_metrics.publisher_metrics,
                self.publisher_metrics,
            )

        with self.subTest("job_summary_with_job_metrics"):
            self.assertListEqual(
                self.job_summary_with_job_metrics.publisher_metrics, []
            )

        with self.subTest("job_summary_with_stage_metrics"):
            self.assertListEqual(
                self.job_summary_with_stage_metrics.publisher_metrics,
                self.publisher_metrics,
            )

        with self.subTest("job_summary_with_partner_metrics"):
            self.assertListEqual(
                self.job_summary_with_partner_metrics.publisher_metrics,
                [],
            )

        with self.subTest("job_summary_with_publisher_metrics"):
            self.assertListEqual(
                self.job_summary_with_publisher_metrics.publisher_metrics,
                self.publisher_metrics,
            )

    def test_get_stage_metrics(self) -> None:
        with self.subTest("job_summary_with_all_metrics"):
            self.assertListEqual(
                self.job_summary_with_all_metrics.get_stage_metrics(
                    DummyStageFlow.STAGE_1
                ),
                self.stage_metrics
                + self.partner_metrics[0:2]
                + self.publisher_metrics[0:2],
            )

        with self.subTest("job_summary_with_job_metrics"):
            self.assertListEqual(
                self.job_summary_with_job_metrics.get_stage_metrics(
                    DummyStageFlow.STAGE_1
                ),
                [],
            )

        with self.subTest("job_summary_with_stage_metrics"):
            self.assertListEqual(
                self.job_summary_with_stage_metrics.get_stage_metrics(
                    DummyStageFlow.STAGE_1
                ),
                self.stage_metrics
                + self.partner_metrics[0:2]
                + self.publisher_metrics[0:2],
            )

        with self.subTest("job_summary_with_partner_metrics"):
            self.assertListEqual(
                self.job_summary_with_partner_metrics.get_stage_metrics(
                    DummyStageFlow.STAGE_1
                ),
                self.partner_metrics[0:2],
            )

        with self.subTest("job_summary_with_publisher_metrics"):
            self.assertListEqual(
                self.job_summary_with_publisher_metrics.get_stage_metrics(
                    DummyStageFlow.STAGE_1
                ),
                self.publisher_metrics[0:2],
            )
