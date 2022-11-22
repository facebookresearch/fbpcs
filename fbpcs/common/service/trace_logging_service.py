#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import functools
import inspect
import logging
import os
import sys
import time
import traceback
from contextlib import contextmanager, suppress
from enum import auto, Enum
from typing import Dict, Iterator, Optional


class CheckpointStatus(Enum):
    STARTED = auto()
    COMPLETED = auto()
    FAILED = auto()

    def __str__(self) -> str:
        return self.name


class TraceLoggingService(abc.ABC):
    def __init__(self) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)

    def write_checkpoint(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
        extract_caller_info: bool = True,
    ) -> None:
        # since we want write_checkpoint to be an infallible operation,
        # all changes to this method should be within the try/except block
        try:
            checkpoint_data = checkpoint_data or {}
            if extract_caller_info:
                checkpoint_data.update(self._extract_caller_info())
            if status is CheckpointStatus.FAILED:
                checkpoint_data.update(self._extract_error_info())
            if bundle_id := os.getenv("FBPCS_BUNDLE_ID"):
                checkpoint_data["FBPCS_BUNDLE_ID"] = bundle_id

            self._write_checkpoint_impl(
                run_id=run_id,
                instance_id=instance_id,
                checkpoint_name=checkpoint_name,
                status=status,
                checkpoint_data=checkpoint_data,
            )
        except Exception as e:
            self.logger.error(f"Failed to write checkpoint: {e}")

    @contextmanager
    def write_checkpoint_cm(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> Iterator[Dict[str, str]]:
        checkpoint_data = checkpoint_data or {}
        try:
            write_checkpoint = functools.partial(
                self.write_checkpoint,
                run_id=run_id,
                instance_id=instance_id,
                checkpoint_name=checkpoint_name,
                # this gets weird in a decorator / context manager
                extract_caller_info=False,
            )
            start_ns = time.perf_counter_ns()
        except Exception:
            self.logger.debug(
                "Could not instantiate checkpoint writer context manager", exc_info=True
            )
            yield checkpoint_data
        else:
            try:
                with suppress(BaseException):
                    write_checkpoint(
                        status=CheckpointStatus.STARTED,
                        checkpoint_data=checkpoint_data.copy(),
                    )
                # allow caller to further mutate checkpoint data as they see fit
                yield checkpoint_data
            except BaseException:
                with suppress(BaseException):
                    elapsed_ms = int((time.perf_counter_ns() - start_ns) / 1e6)
                    write_checkpoint(
                        status=CheckpointStatus.FAILED,
                        checkpoint_data={
                            "runtime_ms": str(elapsed_ms),
                            **checkpoint_data,
                        },
                    )
                raise
            else:
                with suppress(BaseException):
                    elapsed_ms = int((time.perf_counter_ns() - start_ns) / 1e6)
                    write_checkpoint(
                        status=CheckpointStatus.COMPLETED,
                        checkpoint_data={
                            "runtime_ms": str(elapsed_ms),
                            **checkpoint_data,
                        },
                    )

    @abc.abstractmethod
    def _write_checkpoint_impl(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:
        pass

    def _extract_caller_info(self) -> Dict[str, str]:
        res = {}
        try:
            frame = inspect.stack()[2]
            res["filepath"] = f"{frame.filename}:{frame.lineno}"
        except Exception as e:
            logging.warning(f"Failed to extract caller info: {e}")

        return res

    def _extract_error_info(self) -> Dict[str, str]:
        res = {}
        exception_type, exception, exception_trace = sys.exc_info()
        if not exception_type:
            return res
        res.update(
            {
                "exception_type": str(exception_type),
                "exception": str(exception),
                "exception_trace": "\n".join(
                    traceback.format_exception(
                        exception_type, exception, exception_trace
                    )
                ),
            }
        )
        return res
