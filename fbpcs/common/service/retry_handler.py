#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from enum import auto, Enum
from types import TracebackType
from typing import Any, Awaitable, Callable, Dict, Optional, Type, TypeVar


T = TypeVar("T")


DEFAULT_MAX_ATTEMPTS = 3


class BackoffType(Enum):
    CONSTANT = auto()
    LINEAR = auto()
    EXPONENTIAL = auto()


class RetryHandler:
    """
    A class that can act as a context manager to help retry a segment of code.
    Parameters:
        - exc_type: The type of exception to handle
            NOTE: Does not support handling multiple exception types at once
        - max_attempts: number of attempts to try before re-raising the
            underlying exception that most recently occurred
        - logger: an optional logger to log.warning on each retry and log.error
            after the last attepmt
        - backoff_type: whether to back off in a constant, linear, or
            exponential amount of time after each failed `execute`

    Usage:
    with RetryHandler(MyException, max_attempts=3) as retry_handler:
        await retry_handler.execute(func_we_want_to_retry, arg1, arg2, arg3=999)
    """

    def __init__(
        self,
        exc_type: Type[BaseException] = Exception,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        logger: Optional[logging.Logger] = None,
        backoff_type: BackoffType = BackoffType.CONSTANT,
        backoff_seconds: int = 1,
    ) -> None:
        self.exc_type = exc_type
        self.max_attempts = max_attempts
        self.logger: logging.Logger = logger or logging.getLogger(__name__)
        self.backoff_type = backoff_type
        self.backoff_seconds = backoff_seconds

    def __enter__(self) -> "RetryHandler":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        pass

    def _get_backoff_time(self, attempt: int) -> int:
        """
        Determine how much time we should sleep after this
        attempt failure
        """
        if self.backoff_type is BackoffType.CONSTANT:
            return self.backoff_seconds
        elif self.backoff_type is BackoffType.LINEAR:
            return self.backoff_seconds * attempt
        elif self.backoff_type is BackoffType.EXPONENTIAL:
            return self.backoff_seconds**attempt
        raise NotImplementedError(f"Unhandled backoff type: {self.backoff_type}")

    async def execute(
        self, f: Callable[..., Awaitable[T]], *args: Any, **kwargs: Dict[str, Any]
    ) -> T:
        """
        Execute an awaitable function with retries
        """
        saved_err: BaseException
        # Use 1-indexing for better human-readable logs
        for attempt in range(1, self.max_attempts + 1):
            try:
                return await f(*args, **kwargs)
            except self.exc_type as e:
                self.logger.warning(
                    f"Caught exception during attempt [{attempt} / {self.max_attempts}]"
                )
                await asyncio.sleep(self._get_backoff_time(attempt))
                saved_err = e
        self.logger.error("Out of retry attempts. Raising last error.")
        raise saved_err
