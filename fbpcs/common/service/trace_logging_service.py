#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import logging
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

    @abc.abstractmethod
    def write_checkpoint(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:
        pass
