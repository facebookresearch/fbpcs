#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from unittest import TestCase

from fbpcs.bolt.bolt_job_summary import BoltJobSummary

from fbpcs.bolt.bolt_summary import BoltSummary


class TestBoltSummary(TestCase):
    def setUp(self) -> None:
        self.only_failed = BoltSummary(
            job_summaries=[
                BoltJobSummary(
                    job_name="failed_1",
                    publisher_instance_id="publisher_failed_1",
                    partner_instance_id="partner_failed_1",
                    is_success=False,
                ),
                BoltJobSummary(
                    job_name="failed_2",
                    publisher_instance_id="publisher_failed_2",
                    partner_instance_id="partner_failed_2",
                    is_success=False,
                ),
            ]
        )
        self.only_success = BoltSummary(
            job_summaries=[
                BoltJobSummary(
                    job_name="success_1",
                    publisher_instance_id="publisher_success_1",
                    partner_instance_id="partner_success_1",
                    is_success=True,
                ),
                BoltJobSummary(
                    job_name="success_2",
                    publisher_instance_id="publisher_success_2",
                    partner_instance_id="partner_success_2",
                    is_success=True,
                ),
            ]
        )
        self.mixed_results = BoltSummary(
            job_summaries=[
                BoltJobSummary(
                    job_name="success_1",
                    publisher_instance_id="publisher_success_1",
                    partner_instance_id="partner_success_1",
                    is_success=True,
                ),
                BoltJobSummary(
                    job_name="failed_2",
                    publisher_instance_id="publisher_failed_2",
                    partner_instance_id="partner_failed_2",
                    is_success=False,
                ),
            ]
        )

    def test_dunder_bool(self) -> None:
        with self.subTest("only_failed"):
            self.assertFalse(bool(self.only_failed))

        with self.subTest("mixed_results"):
            self.assertFalse(bool(self.mixed_results))

        with self.subTest("only_success"):
            self.assertTrue(bool(self.only_success))

    def test_is_success(self) -> None:
        with self.subTest("only_failed"):
            self.assertFalse(self.only_failed.is_success)

        with self.subTest("mixed_results"):
            self.assertFalse(self.mixed_results.is_success)

        with self.subTest("only_success"):
            self.assertTrue(self.only_success.is_success)

    def test_is_failure(self) -> None:
        with self.subTest("only_failed"):
            self.assertTrue(self.only_failed.is_failure)

        with self.subTest("mixed_results"):
            self.assertTrue(self.mixed_results.is_failure)

        with self.subTest("only_success"):
            self.assertFalse(self.only_success.is_failure)

    def test_num_jobs(self) -> None:
        with self.subTest("only_failed"):
            self.assertEqual(2, self.only_failed.num_jobs)

        with self.subTest("mixed_results"):
            self.assertEqual(2, self.mixed_results.num_jobs)

        with self.subTest("only_success"):
            self.assertEqual(2, self.only_success.num_jobs)

    def test_num_successes(self) -> None:
        with self.subTest("only_failed"):
            self.assertEqual(0, self.only_failed.num_successes)

        with self.subTest("mixed_results"):
            self.assertEqual(1, self.mixed_results.num_successes)

        with self.subTest("only_success"):
            self.assertEqual(2, self.only_success.num_successes)

    def test_num_failures(self) -> None:
        with self.subTest("only_failed"):
            self.assertEqual(2, self.only_failed.num_failures)

        with self.subTest("mixed_results"):
            self.assertEqual(1, self.mixed_results.num_failures)

        with self.subTest("only_success"):
            self.assertEqual(0, self.only_success.num_failures)

    def test_failed_job_summaries(self) -> None:
        with self.subTest("only_failed"):
            self.assertEqual(
                self.only_failed.job_summaries, self.only_failed.failed_job_summaries
            )

        with self.subTest("mixed_results"):
            self.assertEqual(
                [self.mixed_results.job_summaries[1]],
                self.mixed_results.failed_job_summaries,
            )

        with self.subTest("only_success"):
            self.assertEqual([], self.only_success.failed_job_summaries)

    def test_failed_job_names(self) -> None:
        with self.subTest("only_failed"):
            self.assertEqual(
                [s.job_name for s in self.only_failed.job_summaries],
                self.only_failed.failed_job_names,
            )

        with self.subTest("mixed_results"):
            self.assertEqual(
                [self.mixed_results.job_summaries[1].job_name],
                self.mixed_results.failed_job_names,
            )

        with self.subTest("only_success"):
            self.assertEqual([], self.only_success.failed_job_names)
