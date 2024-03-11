#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe


from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

from fbpcs.bolt.hooks.exception_thrower import (
    BoltExceptionThrowerHook,
    BoltExceptionThrowerHookArgs,
)


class TestException(Exception):
    pass


class TestBoltExceptionThrowerHook(IsolatedAsyncioTestCase):
    async def test_throw_exception(self) -> None:
        exception = TestException()
        test_hook = BoltExceptionThrowerHook(
            hook_args=BoltExceptionThrowerHookArgs(exception=exception)
        )
        with self.assertRaises(TestException):
            await test_hook._inject(injection_args=Mock())
