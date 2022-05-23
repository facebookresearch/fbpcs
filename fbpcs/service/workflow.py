#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from enum import Enum
from typing import Dict, Optional


class WorkflowStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowService(abc.ABC):
    @abc.abstractmethod
    def start_workflow(
        self,
        workflow_conf: Dict[str, str],
        run_id: str,
        run_conf: Optional[Dict[str, str]] = None,
    ) -> str:
        pass

    @abc.abstractmethod
    def get_workflow_status(
        self, workflow_conf: Dict[str, str], run_id: str
    ) -> WorkflowStatus:
        pass
