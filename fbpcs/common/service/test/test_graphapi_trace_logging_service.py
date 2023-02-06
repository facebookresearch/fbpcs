#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from queue import SimpleQueue
from unittest import mock, TestCase
from unittest.mock import call

import requests

from fbpcs.common.service.graphapi_trace_logging_service import (
    GraphApiTraceLoggingService,
)
from fbpcs.common.service.trace_logging_service import CheckpointStatus


TEST_ACCESS_TOKEN = "access_token"
TEST_ENDPOINT_URL = "localhost"


class TestGraphApiTraceLoggingService(TestCase):
    def setUp(self) -> None:
        self.logger = mock.create_autospec(logging.Logger)
        self.mock_requests = mock.create_autospec(requests)
        self.mock_msg_queue = mock.create_autospec(SimpleQueue)
        # "wtf is this line?"
        # Well, we've mocked out the entire requests lib in the line above.
        # If we don't *reset* the exceptions module to point to the *real*
        # module, we'll get a bizarre error when running unit tests:
        #     except requests.exceptions.Timeout:
        # TypeError: catching classes that do not inherit from BaseException is not allowed
        # This is because after mocking, Timeout is a *MagicMock*, not an Exception.
        # The line below will fix that oddity.
        self.mock_requests.exceptions = requests.exceptions
        self.svc = GraphApiTraceLoggingService(
            access_token=TEST_ACCESS_TOKEN,
            endpoint_url=TEST_ENDPOINT_URL,
        )
        self.svc.logger = self.logger
        self.svc.msg_queue = self.mock_msg_queue

    def test_write_checkpoint_simple(self) -> None:
        # Act
        self.svc.write_checkpoint(
            run_id="run123",
            instance_id="instance456",
            checkpoint_name="foo",
            status=CheckpointStatus.STARTED,
        )

        # Assert
        self.mock_msg_queue.put.assert_called_once()
        self.logger.debug.assert_called_once()

    def test_post_request_timeout(self) -> None:
        # Arrange
        self.mock_requests.post.side_effect = requests.exceptions.Timeout()

        # Act
        with mock.patch(
            "fbpcs.common.service.graphapi_trace_logging_service.requests",
            self.mock_requests,
        ):
            self.svc._post_request(params={})

        # Assert
        self.logger.info.assert_called_once()
        # TODO(T131856635): Check actual logger output
        # Ideally we should check the contents more closely, but since
        # we augment the data with a filepath (which can change in our test context),
        # it's *really* annoying to figure out what exactly it should look like here.
        self.assertIn("Timeout", self.logger.info.call_args_list[0][0][0])

    def test_post_request_other_exception(self) -> None:
        # Arrange
        self.mock_requests.post.side_effect = Exception("Foobar")

        # Act
        with mock.patch(
            "fbpcs.common.service.graphapi_trace_logging_service.requests",
            self.mock_requests,
        ):
            self.svc._post_request(params={})

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

        # Act
        self.svc.write_checkpoint(
            run_id="run123",
            instance_id="instance456",
            checkpoint_name="foo",
            status=CheckpointStatus.STARTED,
            checkpoint_data=data,
        )

        # Assert
        self.mock_msg_queue.put.assert_called_once()
        self.logger.debug.assert_called_once()

    def test_flush_msg_queue(self) -> None:
        # Arrange
        msg_lists = [
            {
                "instance_id": "instance456",
                "component": "component1",
                "checkpoint_name": "foo1",
                "checkpoint_data": "data1",
            },
            {
                "instance_id": "instance456",
                "component": "component2",
                "checkpoint_name": "foo2",
                "checkpoint_data": "data2",
            },
            {
                "instance_id": "instance789",
                "component": "component3",
                "checkpoint_name": "foo3",
                "checkpoint_data": "data3",
            },
        ]
        self.mock_msg_queue.get.side_effect = msg_lists

        # Act
        with mock.patch(
            "fbpcs.common.service.graphapi_trace_logging_service.requests",
            self.mock_requests,
        ):
            self.svc._flush_msg_queue(
                msg_queue=self.mock_msg_queue, flush_size=len(msg_lists)
            )

        # Assert
        # group by instance id - having two distince instances
        self.assertEqual(2, self.mock_requests.post.call_count)
        self.assertEqual(2, self.logger.info.call_count)
        # assert post group by instance id
        self.mock_requests.post.assert_has_calls(
            [
                call(
                    "localhost",
                    params={
                        "instance_id": "instance456",
                        "component": "component1\\001component2",
                        "checkpoint_name": "foo1\\001foo2",
                        "checkpoint_data": "data1\\001data2",
                    },
                    timeout=3.05,
                ),
                call(
                    "localhost",
                    params={
                        "instance_id": "instance789",
                        "component": "component3",
                        "checkpoint_name": "foo3",
                        "checkpoint_data": "data3",
                    },
                    timeout=3.05,
                ),
            ],
            any_order=True,
        )
