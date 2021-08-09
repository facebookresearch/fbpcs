#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from fbpcs.entity.instance_base import InstanceBase
from fbpmp.post_processing_handler.post_processing_handler import (
    PostProcessingHandler,
    PostProcessingHandlerStatus,
)


class PostProcessingInstanceStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class PostProcessingInstance(InstanceBase):
    instance_id: str
    handler_statuses: Dict[str, PostProcessingHandlerStatus]
    status: PostProcessingInstanceStatus

    @classmethod
    def create_instance(
        cls,
        instance_id: str,
        handlers: Optional[Dict[str, PostProcessingHandler]] = None,
        handler_statuses: Optional[Dict[str, PostProcessingHandlerStatus]] = None,
        status: PostProcessingInstanceStatus = PostProcessingInstanceStatus.UNKNOWN,
    ) -> "PostProcessingInstance":
        if handlers and not handler_statuses:
            handler_statuses = {
                name: PostProcessingHandlerStatus.UNKNOWN for name in handlers.keys()
            }
        else:
            handler_statuses = handler_statuses or {}
        return cls(instance_id, handler_statuses, status)

    def get_instance_id(self) -> str:
        return self.instance_id
