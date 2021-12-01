#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
from typing import Dict, Any

from fbpcs.pl_coordinator.pl_graphapi_utils import (
    PLGraphAPIClient,
)


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, prefix: str):
        super(LoggerAdapter, self).__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return "[%s] %s" % (self.prefix, msg), kwargs


# dataset information fields
AD_OBJECT_ID = "ad_object_id"
TARGET_OBJECT_TYPE = "target_object_type"
DATASETS_INFORMATION = "datasets_information"


POLL_INTERVAL = 60
WAIT_VALID_STATUS_TIMEOUT = 600
WAIT_VALID_STAGE_TIMEOUT = 300
OPERATION_REQUEST_TIMEOUT = 1200
CANCEL_STAGE_TIMEOUT = POLL_INTERVAL * 5

MIN_TRIES = 1
MAX_TRIES = 2
RETRY_INTERVAL = 60

MIN_NUM_INSTANCES = 1
MAX_NUM_INSTANCES = 5
PROCESS_WAIT = 1  # interval between starting processes.
INSTANCE_SLA = 14400  # 2 hr instance sla, 2 tries per stage, total 4 hrs.


def get_attribution_dataset_info(
    config: Dict[str, Any], dataset_id: str, logger: logging.Logger
) -> str:
    client = PLGraphAPIClient(config["graphapi"]["access_token"], logger)

    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [AD_OBJECT_ID, TARGET_OBJECT_TYPE, DATASETS_INFORMATION],
        ).text
    )
