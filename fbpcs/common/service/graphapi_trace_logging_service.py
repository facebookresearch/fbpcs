#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
from typing import Dict, Optional

import requests

from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)


class GraphApiTraceLoggingService(TraceLoggingService):
    def __init__(self, endpoint_url: str) -> None:
        self.endpoint_url = endpoint_url

    def write_checkpoint(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:
        if run_id is None:
            self.logger.debug("No run_id provided - skipping write_checkpoint")
            return

        form_data = {
            "operation": "write_checkpoint",
            "run_id": run_id,
            "instance_id": instance_id,
            "checkpoint_name": checkpoint_name,
            "status": str(status),
        }
        if checkpoint_data:
            form_data["checkpoint_data"] = json.dumps(checkpoint_data)

        r = requests.post(self.endpoint_url, json=form_data)

        log_data = form_data.copy()
        log_data["requests_post_status_code"] = str(r.status_code)
        self.logger.info(json.dumps(log_data))
