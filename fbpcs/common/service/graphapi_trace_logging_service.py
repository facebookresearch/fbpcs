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


# Allow a request to be open for this many seconds before
# raising a requests.exceptions.Timeout exception.
# From the requests documentation: It’s a good practice to set
# connect timeouts to slightly larger than a multiple of 3,
# which is the default TCP packet retransmission window.
RESPONSE_TIMEOUT: float = 3.05


class GraphApiTraceLoggingService(TraceLoggingService):
    def __init__(self, endpoint_url: str) -> None:
        self.endpoint_url = endpoint_url

    def _write_checkpoint_impl(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:
        form_data = {
            "operation": "write_checkpoint",
            "run_id": run_id,
            "instance_id": instance_id,
            "checkpoint_name": checkpoint_name,
            "status": str(status),
        }
        if checkpoint_data:
            form_data["checkpoint_data"] = json.dumps(checkpoint_data)

        log_data = form_data.copy()

        try:
            r = requests.post(
                self.endpoint_url, json=form_data, timeout=RESPONSE_TIMEOUT
            )
            log_data["requests_post_status_code"] = str(r.status_code)
            log_data["requests_post_reason"] = str(r.reason)
        except requests.exceptions.Timeout:
            log_data["extra_info"] = "Timeout reaching endpoint"
        except Exception as e:
            log_data["extra_info"] = f"Unexpected error: {e}"

        self.logger.info(json.dumps(log_data))
