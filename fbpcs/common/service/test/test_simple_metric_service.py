#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from unittest import mock, TestCase
from unittest.mock import call

from fbpcs.common.service.simple_metric_service import SimpleMetricService


class TestSimpleMetricService(TestCase):
    def setUp(self) -> None:
        self.logger = mock.create_autospec(logging.Logger)
        self.svc = SimpleMetricService(category="default")
        self.svc.logger = self.logger

    def test_bump_entity_key_simple(self) -> None:
        # Arrange
        expected_dump = json.dumps(
            {
                "operation": "bump_entity_key",
                "entity": "default.entity",
                "key": "key",
                "value": 1,
            }
        )

        # Act
        self.svc.bump_entity_key("entity", "key")

        # Assert
        self.logger.info.assert_called_once_with(expected_dump)

    def test_bump_entity_key_custom_value(self) -> None:
        # Arrange
        expected_dump = json.dumps(
            {
                "operation": "bump_entity_key",
                "entity": "default.entity",
                "key": "key",
                "value": 123,
            }
        )

        # Act
        self.svc.bump_entity_key("entity", "key", 123)

        # Assert
        self.logger.info.assert_called_once_with(expected_dump)

    def test_bump_entity_key_avg_custom_value(self) -> None:
        # Arrange
        expected_dump = json.dumps(
            {
                "operation": "bump_entity_key_avg",
                "entity": "default.entity",
                "key": "key",
                "value": 123,
            }
        )

        # Act
        self.svc.bump_entity_key_avg("entity", "key", 123)

        # Assert
        self.logger.info.assert_called_once_with(expected_dump)

    @mock.patch(
        "time.perf_counter_ns",
        new=mock.MagicMock(side_effect=[28000000000000, 29000000000000]),
    )
    def test_timer(self) -> None:
        # Arrange
        expected_dump = json.dumps(
            {
                "operation": "bump_entity_key_avg",
                "entity": "default.entity",
                "key": "prefix.time_ms",
                "value": 1000000,
            }
        )

        # Act
        with self.svc.timer("entity", "prefix"):
            pass

        # Assert
        self.logger.info.assert_called_once_with(expected_dump)

    def test_bump_num_times_called_and_error_count(self) -> None:
        # Arrange
        expected_err_dump = json.dumps(
            {
                "operation": "bump_entity_key",
                "entity": "default.entity",
                "key": "prefix.num_errors",
                "value": 1,
            }
        )
        expected_dump = json.dumps(
            {
                "operation": "bump_entity_key",
                "entity": "default.entity",
                "key": "prefix.num_calls",
                "value": 1,
            }
        )

        # Act
        try:
            with self.svc.bump_num_times_called_and_error_count("entity", "prefix"):
                raise RuntimeError("Force Fail")
        except Exception:
            pass

        # Assert
        self.assertEqual(2, self.logger.info.call_count)
        self.logger.info.assert_has_calls(
            [call(expected_err_dump), call(expected_dump)]
        )
