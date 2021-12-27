#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
from typing import Type, Optional, Dict, Any

from fbpcs.pl_coordinator.pl_graphapi_utils import (
    PLGraphAPIClient,
)
from fbpcs.pl_coordinator.pl_instance_runner import (
    run_instance,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    AttributionRule,
    AggregationType,
    PrivateComputationGameType,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
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
INSTANCES = "instances"
NUM_SHARDS = "num_shards"
NUM_CONTAINERS = "num_containers"


POLL_INTERVAL = 60
WAIT_VALID_STATUS_TIMEOUT = 600
WAIT_VALID_STAGE_TIMEOUT = 300
OPERATION_REQUEST_TIMEOUT = 1200
CANCEL_STAGE_TIMEOUT = POLL_INTERVAL * 5

MIN_TRIES = 1
MAX_TRIES = 2
RETRY_INTERVAL = 60

MIN_NUM_INSTANCES = 1
PROCESS_WAIT = 1  # interval between starting processes.
INSTANCE_SLA = 14400  # 2 hr instance sla, 2 tries per stage, total 4 hrs.

"""
The input to this function will be the input path, the dataset_id as well as the following params to choose
a specific dataset range to create and run a PA instance on
1) start_date - start date of the FB Opportunity data
2) end_date - end date of the FB Opportunity data
3) attribution_rule - attribution rule for the selected data
4) result_type - result type for the selected data
"""


def run_attribution(
    config: Dict[str, Any],
    dataset_id: str,
    input_path: str,
    start_date: str,
    end_date: str,
    attribution_rule: AttributionRule,
    aggregation_type: AggregationType,
    concurrency: int,
    num_files_per_mpc_container: int,
    k_anonymity_threshold: int,
    result_type: str,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
) -> None:

    ## Step 1: Validation. Function arguments and  for private lift run.
    # obtain the values in the dataset info vector.
    client = PLGraphAPIClient(config["graphapi"]["access_token"], logger)
    datasets_info = _get_attribution_dataset_info(client, dataset_id, logger)
    datasets = datasets_info[DATASETS_INFORMATION]
    matched_data = {}
    attribution_rule_str = attribution_rule.value
    # Verify that input has matching dataset info:
    # a. appropriate date range
    # b. attribution rule
    # c. result type
    for data in datasets:
        unmatched_start_date = data["start_date"] != start_date
        unmatched_end_date = data["end_date"] != end_date
        unmatched_attribution_rule = data["attribution_rule"] != attribution_rule_str
        unmatched_result_type = data["result_type"] != result_type

        if (
            unmatched_start_date
            or unmatched_end_date
            or unmatched_result_type
            or unmatched_attribution_rule
        ):
            continue
        matched_data = data
        break

    if len(matched_data) == 0:
        raise ValueError("No dataset matching to the information provided")

    # Step 2: Validate what instances need to be created vs what already exist
    dataset_instance_data = _get_existing_pa_instances(client, dataset_id)
    existing_instances = dataset_instance_data["data"]
    if len(existing_instances) == 0:
        instance_id = _create_new_instance(
            dataset_id,
            start_date,
            end_date,
            attribution_rule_str,
            result_type,
            client,
            logger,
        )
    else:
        instance_id = existing_instances[0]["id"]

    instance_data = _get_pa_instance_info(client, instance_id, logger)
    num_pid_containers = instance_data[NUM_CONTAINERS]
    num_mpc_containers = instance_data[NUM_SHARDS]

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
    start_date: str,
    end_date: str,
    attribution_rule: str,
    result_type: str,
    client: PLGraphAPIClient,
    logger: logging.Logger,
) -> str:
    instance_id = json.loads(
        client.create_pa_instance(
            dataset_id,
            start_date,
            end_date,
            attribution_rule,
            2,
            result_type,
        ).text
    )["id"]
    logger.info(
        f"Created instance {instance_id} for dataset {dataset_id} and attribution rule {attribution_rule}"
    )
    return instance_id


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


def _get_pa_instance_info(
    client: PLGraphAPIClient, instance_id: str, logger: logging.Logger
) -> Any:
    return json.loads(client.get_instance(instance_id).text)


def _get_attribution_dataset_info(
    client: PLGraphAPIClient, dataset_id: str, logger: logging.Logger
) -> Any:
    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [AD_OBJECT_ID, TARGET_OBJECT_TYPE, DATASETS_INFORMATION],
        ).text
    )


def _get_existing_pa_instances(client: PLGraphAPIClient, dataset_id: str) -> Any:
    return json.loads(client.get_existing_pa_instances(dataset_id).text)
