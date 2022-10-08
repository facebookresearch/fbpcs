#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
from typing import Dict, Optional

import requests

from fbpcs.common.service.secret_scrubber import SecretScrubber
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

DEFAULT_RUN_ID = "anonymous_run_id"
DEFAULT_COMPONENT_NAME = "pcservice"


class GraphApiTraceLoggingService(TraceLoggingService):
    def __init__(self, access_token: str, endpoint_url: str) -> None:
        super().__init__()
        self.access_token = access_token
        self.endpoint_url = endpoint_url
        self.scrubber = SecretScrubber()

    def _write_checkpoint_impl(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:

        checkpoint_data = checkpoint_data or {}
        component = checkpoint_data.pop("component", DEFAULT_COMPONENT_NAME)
        scrubbed_checkpoint_data = {}
        for k, v in checkpoint_data.items():
            scrubbed_key = self.scrubber.scrub(k).scrubbed_output
            scrubbed_val = self.scrubber.scrub(v).scrubbed_output
            scrubbed_checkpoint_data[scrubbed_key] = scrubbed_val

        params = {
            "run_id": run_id or DEFAULT_RUN_ID,
            "instance_id": instance_id,
            "checkpoint_name": f"{checkpoint_name}_{status}",
            "access_token": self.access_token,
            "component": component,
            "checkpoint_data": json.dumps(checkpoint_data),
        }

        log_data = params.copy()
        del log_data["access_token"]

        try:
            r = requests.post(
                self.endpoint_url, params=params, timeout=RESPONSE_TIMEOUT
            )
            log_data["requests_post_status_code"] = str(r.status_code)
            log_data["requests_post_reason"] = str(r.reason)
        except requests.exceptions.Timeout:
            log_data["extra_info"] = "Timeout reaching endpoint"
        except Exception as e:
            log_data["extra_info"] = f"Unexpected error: {e}"

        # We run the secret scrubber since we want to be completely
        # sure we don't accidentally log an access token
        log_dump = json.dumps(log_data)
        scrubbed_log_dump = self.scrubber.scrub(log_dump).scrubbed_output
        self.logger.info(scrubbed_log_dump)
