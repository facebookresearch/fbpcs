#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from unittest import mock, TestCase

import requests

from fbpcs.common.service.graphapi_trace_logging_service import (
    GraphApiTraceLoggingService,
)
from fbpcs.common.service.trace_logging_service import CheckpointStatus


TEST_ENDPOINT_URL = "localhost"


class TestGraphApiTraceLoggingService(TestCase):
    def setUp(self) -> None:
        self.logger = mock.create_autospec(logging.Logger)
        self.mock_requests = mock.create_autospec(requests)
        self.svc = GraphApiTraceLoggingService(TEST_ENDPOINT_URL)
        self.svc.logger = self.logger

    def test_write_checkpoint_no_run_id(self) -> None:
        # Act
        self.svc.write_checkpoint(
            run_id=None,
            instance_id="instance456",
            checkpoint_name="foo",
            status=CheckpointStatus.STARTED,
        )

        # Assert
        self.logger.debug.assert_called_once()
        self.logger.info.assert_not_called()

    def test_write_checkpoint_simple(self) -> None:
        # Arrange
        form_data = {
            "operation": "write_checkpoint",
            "run_id": "run123",
            "instance_id": "instance456",
            "checkpoint_name": "foo",
            "status": str(CheckpointStatus.STARTED),
        }

        # Act
        with mock.patch(
            "fbpcs.common.service.graphapi_trace_logging_service.requests",
            self.mock_requests,
        ):
            self.svc.write_checkpoint(
                run_id="run123",
                instance_id="instance456",
                checkpoint_name="foo",
                status=CheckpointStatus.STARTED,
            )

        # Assert
        self.logger.info.assert_called_once()
        self.mock_requests.post.assert_called_once_with(
            TEST_ENDPOINT_URL, json=form_data
        )

    def test_write_checkpoint_custom_data(self) -> None:
        # Arrange
        data = {"bar": "baz", "quux": "quuz"}
        form_data = {
            "operation": "write_checkpoint",
            "run_id": "run123",
            "instance_id": "instance456",
            "checkpoint_name": "foo",
            "status": str(CheckpointStatus.STARTED),
            "checkpoint_data": json.dumps(data),
        }

        # Act
        with mock.patch(
            "fbpcs.common.service.graphapi_trace_logging_service.requests",
            self.mock_requests,
        ):
            self.svc.write_checkpoint(
                run_id="run123",
                instance_id="instance456",
                checkpoint_name="foo",
                status=CheckpointStatus.STARTED,
                checkpoint_data=data,
            )

        # Assert
        self.logger.info.assert_called_once()
        self.mock_requests.post.assert_called_once_with(
            TEST_ENDPOINT_URL, json=form_data
        )
