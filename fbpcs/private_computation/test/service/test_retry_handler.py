#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, create_autospec

from fbpcs.private_computation.service.retry_handler import BackoffType, RetryHandler


class DummyExceptionType(Exception):
    pass


class TestRetryHandler(IsolatedAsyncioTestCase):
    async def test_execute(self) -> None:
        with self.subTest("first_attempt"):
            # Arrange
            foo = AsyncMock(return_value=123)
            # Act
            with RetryHandler() as handler:
                actual = await handler.execute(foo)
            # Assert
            self.assertEqual(123, actual)

        with self.subTest("second_attempt"):
            # Arrange
            foo = AsyncMock(side_effect=[DummyExceptionType(), 123])
            # Act
            with RetryHandler() as handler:
                actual = await handler.execute(foo)
            # Assert
            self.assertEqual(123, actual)

        with self.subTest("out_of_attempts"):
            # Arrange
            foo = AsyncMock(side_effect=DummyExceptionType())
            # Act & Assert
            with self.assertRaises(DummyExceptionType):
                with RetryHandler() as handler:
                    await handler.execute(foo)

    async def test_execute_logging(self) -> None:
        with self.subTest("first_attempt"):
            # Arrange
            foo = AsyncMock(return_value=123)
            logger = create_autospec(logging.Logger)
            # Act
            with RetryHandler(logger=logger) as handler:
                await handler.execute(foo)
            # Assert
            logger.assert_not_called()

        with self.subTest("second_attempt"):
            # Arrange
            logger = create_autospec(logging.Logger)
            foo = AsyncMock(side_effect=[DummyExceptionType(), 123])
            # Act
            with RetryHandler(logger=logger) as handler:
                await handler.execute(foo)
            # Assert
            logger.warning.assert_called_once()

        with self.subTest("out_of_attempts"):
            # Arrange
            logger = create_autospec(logging.Logger)
            foo = AsyncMock(side_effect=DummyExceptionType())
            # Act & Assert
            with self.assertRaises(DummyExceptionType):
                with RetryHandler(logger=logger) as handler:
                    await handler.execute(foo)
            logger.error.assert_called_once()

    #############################
    # Logically private methods #
    #############################
    def test_get_backoff_time(self) -> None:
        with self.subTest("constant_call1"):
            # Arrange
            handler = RetryHandler(
                backoff_type=BackoffType.CONSTANT, backoff_seconds=123
            )
            expected = 123
            # Act
            actual = handler._get_backoff_time(attempt=1)
            # Assert
            self.assertEqual(expected, actual)

        with self.subTest("constant_call2"):
            # Arrange
            handler = RetryHandler(
                backoff_type=BackoffType.CONSTANT, backoff_seconds=123
            )
            expected = 123
            # Act
            actual = handler._get_backoff_time(attempt=2)
            # Assert
            self.assertEqual(expected, actual)

        with self.subTest("linear_call1"):
            # Arrange
            handler = RetryHandler(backoff_type=BackoffType.LINEAR, backoff_seconds=123)
            expected = 123
            # Act
            actual = handler._get_backoff_time(attempt=1)
            # Assert
            self.assertEqual(expected, actual)

        with self.subTest("linear_call2"):
            # Arrange
            handler = RetryHandler(backoff_type=BackoffType.LINEAR, backoff_seconds=123)
            expected = 123 * 2
            # Act
            actual = handler._get_backoff_time(attempt=2)
            # Assert
            self.assertEqual(expected, actual)

        with self.subTest("exponential_call1"):
            # Arrange
            handler = RetryHandler(
                backoff_type=BackoffType.EXPONENTIAL, backoff_seconds=123
            )
            expected = 123
            # Act
            actual = handler._get_backoff_time(attempt=1)
            # Assert
            self.assertEqual(expected, actual)

        with self.subTest("exponential_call2"):
            # Arrange
            handler = RetryHandler(
                backoff_type=BackoffType.EXPONENTIAL, backoff_seconds=123
            )
            expected = 123**2
            # Act
            actual = handler._get_backoff_time(attempt=2)
            # Assert
            self.assertEqual(expected, actual)
