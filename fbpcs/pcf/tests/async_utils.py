#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import functools
import unittest
from typing import Any


def AsyncMock(*args, **kwargs):
    m = unittest.mock.MagicMock(*args, **kwargs)

    async def mock_future(*args, **kwargs):
        return m(*args, **kwargs)

    mock_future.mock = m
    return mock_future


async def awaitable(v: Any) -> Any:
    return v


def wait(f: asyncio.Future) -> Any:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(f)


def to_sync(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        asyncio.run(f(*args, **kwargs))

    return wrapper
