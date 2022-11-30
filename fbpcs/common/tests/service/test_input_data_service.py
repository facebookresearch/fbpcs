#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch

from fbpcs.common.service.input_data_service import InputDataService


class TestInputDataService(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

    @patch("fbpcs.common.service.input_data_service.logging.getLogger")
    def test_get_lift_study_timestamps_returns_none_when_unparsable(
        self, _mock_logger
    ) -> None:
        result = InputDataService.get_lift_study_timestamps("not", "valid")

        self.assertIsNone(result.start_timestamp)
        self.assertIsNone(result.end_timestamp)

    def test_get_lift_study_timestamps(self) -> None:
        start_timestamp = "1657090807"
        end_timestamp = "1658000000"

        result = InputDataService.get_lift_study_timestamps(
            start_timestamp, end_timestamp
        )

        self.assertEqual(result.start_timestamp, start_timestamp)
        self.assertEqual(result.end_timestamp, end_timestamp)

    @patch("fbpcs.common.service.input_data_service.logging.getLogger")
    def test_get_attribution_timestamps_returns_none_when_unparsable(
        self, _mock_logger
    ) -> None:
        result = InputDataService.get_attribution_timestamps("not-valid")

        self.assertIsNone(result.start_timestamp)
        self.assertIsNone(result.end_timestamp)

    def test_get_attribution_timestamps(self) -> None:
        expected_start = "1651327200"
        # Start + 2 days
        expected_end = str(int(expected_start) + (3600 * 48))

        result = InputDataService.get_attribution_timestamps("2022-05-01T14:00:00+0000")

        self.assertEqual(result.start_timestamp, expected_start)
        self.assertEqual(result.end_timestamp, expected_end)
