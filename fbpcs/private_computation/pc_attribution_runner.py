#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Type

import dateutil.parser
import pytz
from fbpcs.pl_coordinator.pl_graphapi_utils import PCGraphAPIClient
from fbpcs.pl_coordinator.pl_instance_runner import run_instance
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, prefix: str) -> None:
        super(LoggerAdapter, self).__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return "[%s] %s" % (self.prefix, msg), kwargs


# dataset information fields
DATASETS_INFORMATION = "datasets_information"
INSTANCES = "instances"
NUM_SHARDS = "num_shards"
NUM_CONTAINERS = "num_containers"

# instance fields
TIMESTAMP = "timestamp"
ATTRIBUTION_RULE = "attribution_rule"
STATUS = "status"
CREATED_TIME = "created_time"

TERMINAL_STATUSES = [
    "POST_PROCESSING_HANDLERS_COMPLETED",
    "RESULT_READY",
    "INSTANCE_FAILURE",
]

"""
The input to this function will be the input path, the dataset_id as well as the following params to choose
a specific dataset range to create and run a PA instance on
1) timestamp - timestamp of the day(0AM) describing the data uploaded from the Meta side
2) attribution_rule - attribution rule for the selected data
3) result_type - result type for the selected data
"""


def run_attribution(
    config: Dict[str, Any],
    dataset_id: str,
    input_path: str,
    timestamp: str,
    attribution_rule: AttributionRule,
    aggregation_type: AggregationType,
    concurrency: int,
    num_files_per_mpc_container: int,
    k_anonymity_threshold: int,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
) -> None:

    ## Step 1: Validation. Function arguments and  for private attribution run.
    # obtain the values in the dataset info vector.
    client = PCGraphAPIClient(config, logger)
    datasets_info = _get_attribution_dataset_info(client, dataset_id, logger)
    datasets = datasets_info[DATASETS_INFORMATION]
    matched_data = {}
    attribution_rule_str = attribution_rule.name
    attribution_rule_val = attribution_rule.value
    instance_id = None
    pacific_timezone = pytz.timezone("US/Pacific")
    # Validate if input is datetime or timestamp
    is_date_format = _iso_date_validator(timestamp)
    if is_date_format:
        dt = pacific_timezone.localize(datetime.strptime(timestamp, "%Y-%m-%d"))
    else:
        dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)

    # Compute the argument after the timestamp has been input
    dt_arg = int(datetime.timestamp(dt))

    # Verify that input has matching dataset info:
    # a. attribution rule
    # b. timestamp
    if len(datasets) == 0:
        raise ValueError("Dataset for given parameters and dataset invalid")
    for data in datasets:
        if data["key"] == attribution_rule_str:
            matched_attr = data["value"]

    for m_data in matched_attr:
        m_time = dateutil.parser.parse(m_data[TIMESTAMP])
        if m_time == dt:
            matched_data = m_data
            break
    if len(matched_data) == 0:
        raise ValueError("No dataset matching to the information provided")
    # Step 2: Validate what instances need to be created vs what already exist
    # Conditions for retry:
    # 1. Not in a terminal status
    # 2. Instance has been created > 1d ago
    dataset_instance_data = _get_existing_pa_instances(client, dataset_id)
    existing_instances = dataset_instance_data["data"]
    for inst in existing_instances:
        inst_time = dateutil.parser.parse(inst[TIMESTAMP])
        creation_time = dateutil.parser.parse(inst[CREATED_TIME])
        exp_time = datetime.now(tz=timezone.utc) - timedelta(days=1)
        expired = exp_time > creation_time
        if (
            inst[ATTRIBUTION_RULE] == attribution_rule_val
            and inst_time == dt
            and inst[STATUS] not in TERMINAL_STATUSES
            and not expired
        ):
            instance_id = inst["id"]
            break

    if instance_id is None:
        instance_id = _create_new_instance(
            dataset_id,
            int(dt_arg),
            attribution_rule_val,
            client,
            logger,
        )
    instance_data = _get_pa_instance_info(client, instance_id, logger)
    num_pid_containers = instance_data[NUM_SHARDS]
    num_mpc_containers = instance_data[NUM_CONTAINERS]

    ## Step 3. Run Instances. Run maximum number of instances in parallel
    logger.info(f"Start running instance {instance_id}.")
    run_instance(
        config,
        instance_id,
        input_path,
        num_pid_containers,
        num_mpc_containers,
        stage_flow,
        logger,
        PrivateComputationGameType.ATTRIBUTION,
        attribution_rule,
        AggregationType.MEASUREMENT,
        concurrency,
        num_files_per_mpc_container,
        k_anonymity_threshold,
        num_tries,
    )
    logger.info(f"Finished running instances {instance_id}.")


def _create_new_instance(
    dataset_id: str,
    timestamp: int,
    attribution_rule: str,
    client: PCGraphAPIClient,
    logger: logging.Logger,
) -> str:
    instance_id = json.loads(
        client.create_pa_instance(
            dataset_id,
            timestamp,
            attribution_rule,
            2,
        ).text
    )["id"]
    logger.info(
        f"Created instance {instance_id} for dataset {dataset_id} and attribution rule {attribution_rule}"
    )
    return instance_id


def get_attribution_dataset_info(
    config: Dict[str, Any], dataset_id: str, logger: logging.Logger
) -> str:
    client = PCGraphAPIClient(config, logger)

    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [DATASETS_INFORMATION],
        ).text
    )


def _get_pa_instance_info(
    client: PCGraphAPIClient, instance_id: str, logger: logging.Logger
) -> Any:
    return json.loads(client.get_instance(instance_id).text)


def _iso_date_validator(timestamp: str) -> Any:
    try:
        datetime.strptime(timestamp, "%Y-%m-%d")
        return True
    except Exception:
        pass
    else:
        return False


def _get_attribution_dataset_info(
    client: PCGraphAPIClient, dataset_id: str, logger: logging.Logger
) -> Any:
    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [DATASETS_INFORMATION],
        ).text
    )


def _get_existing_pa_instances(client: PCGraphAPIClient, dataset_id: str) -> Any:
    return json.loads(client.get_existing_pa_instances(dataset_id).text)
