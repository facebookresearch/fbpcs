#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from unittest import mock, TestCase

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
