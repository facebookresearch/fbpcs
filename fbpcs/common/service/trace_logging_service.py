#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import inspect
import logging
import sys
import traceback
from enum import auto, Enum
from typing import Dict, Optional


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
    ) -> None:
        checkpoint_data = checkpoint_data or {}
        checkpoint_data.update(self._extract_caller_info())
        if status is CheckpointStatus.FAILED:
            checkpoint_data.update(self._extract_error_info())

        self._write_checkpoint_impl(
            run_id=run_id,
            instance_id=instance_id,
            checkpoint_name=checkpoint_name,
            status=status,
            checkpoint_data=checkpoint_data,
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
            frame = inspect.stack()[1]
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
