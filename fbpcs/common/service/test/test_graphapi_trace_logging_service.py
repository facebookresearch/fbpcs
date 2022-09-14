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
    RESPONSE_TIMEOUT,
)
from fbpcs.common.service.trace_logging_service import CheckpointStatus


TEST_ENDPOINT_URL = "localhost"


class TestGraphApiTraceLoggingService(TestCase):
    def setUp(self) -> None:
        self.logger = mock.create_autospec(logging.Logger)
        self.mock_requests = mock.create_autospec(requests)
        # "wtf is this line?"
        # Well, we've mocked out the entire requests lib in the line above.
        # If we don't *reset* the exceptions module to point to the *real*
        # module, we'll get a bizarre error when running unit tests:
        #     except requests.exceptions.Timeout:
        # TypeError: catching classes that do not inherit from BaseException is not allowed
        # This is because after mocking, Timeout is a *MagicMock*, not an Exception.
        # The line below will fix that oddity.
        self.mock_requests.exceptions = requests.exceptions
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
        self.mock_requests.post.assert_called_once()

    def test_write_checkpoint_request_timeout(self) -> None:
        # Arrange
        self.mock_requests.post.side_effect = requests.exceptions.Timeout()

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
        # TODO(T131856635): Check actual logger output
        # Ideally we should check the contents more closely, but since
        # we augment the data with a filepath (which can change in our test context),
        # it's *really* annoying to figure out what exactly it should look like here.
        self.assertIn("Timeout", self.logger.info.call_args_list[0][0][0])

    def test_write_checkpoint_other_exception(self) -> None:
        # Arrange
        self.mock_requests.post.side_effect = Exception("Foobar")

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
        # TODO(T131856635): Check actual logger output
        # Ideally we should check the contents more closely, but since
        # we augment the data with a filepath (which can change in our test context),
        # it's *really* annoying to figure out what exactly it should look like here.
        self.assertIn("Foobar", self.logger.info.call_args_list[0][0][0])

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
        self.mock_requests.post.assert_called_once()
