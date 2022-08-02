#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
from typing import Dict, Optional

from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)


class SimpleTraceLoggingService(TraceLoggingService):
    def write_checkpoint(
        self,
        run_id: str,
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:
        result = {
            "operation": "write_checkpoint",
            "run_id": run_id,
            "instance_id": instance_id,
            "checkpoint_name": checkpoint_name,
            "status": str(status),
        }
        if checkpoint_data:
            result["checkpoint_data"] = json.dumps(checkpoint_data)
        self.logger.info(json.dumps(result))
