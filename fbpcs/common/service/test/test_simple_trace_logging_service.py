#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from unittest import mock, TestCase

from fbpcs.common.service.simple_trace_logging_service import SimpleTraceLoggingService

from fbpcs.common.service.trace_logging_service import CheckpointStatus


class TestSimpleTraceLoggingService(TestCase):
    def setUp(self) -> None:
        self.logger = mock.create_autospec(logging.Logger)
        self.svc = SimpleTraceLoggingService()
        self.svc.logger = self.logger

    def test_write_checkpoint_simple(self) -> None:
        # Act
        self.svc.write_checkpoint(
            run_id="run123",
            instance_id="instance456",
            checkpoint_name="foo",
            status=CheckpointStatus.STARTED,
        )

        # Assert
        self.logger.info.assert_called_once()
        # TODO(T131856635): Check actual logger output
        # Ideally we should check the contents more closely, but since
        # we augment the data with a filepath (which can change in our test context),
        # it's *really* annoying to figure out what exactly it should look like here.

    def test_write_checkpoint_custom_data(self) -> None:
        # Arrange
        data = {"bar": "baz", "quux": "quuz"}

        # Act
        self.svc.write_checkpoint(
            run_id="run123",
            instance_id="instance456",
            checkpoint_name="foo",
            status=CheckpointStatus.STARTED,
            checkpoint_data=data,
        )

        # Assert
        self.logger.info.assert_called_once()
        # TODO(T131856635): Check actual logger output
        # Ideally we should check the contents more closely, but since
        # we augment the data with a filepath (which can change in our test context),
        # it's *really* annoying to figure out what exactly it should look like here.
        self.assertIn("quux", self.logger.info.call_args_list[0][0][0])
