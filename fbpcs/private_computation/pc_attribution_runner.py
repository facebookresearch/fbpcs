#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Type

import dateutil.parser
import pytz
from fbpcs.bolt.bolt_job import BoltJob, BoltPlayerArgs
from fbpcs.bolt.bolt_runner import BoltRunner
from fbpcs.bolt.oss_bolt_pcs import BoltPCSClient, BoltPCSCreateInstanceArgs
from fbpcs.common.feature.pcs_feature_gate_utils import get_stage_flow
from fbpcs.pl_coordinator.bolt_graphapi_client import (
    BoltGraphAPIClient,
    BoltPAGraphAPICreateInstanceArgs,
)
from fbpcs.pl_coordinator.constants import MAX_NUM_INSTANCES
from fbpcs.pl_coordinator.exceptions import (
    GraphAPIGenericException,
    IncorrectVersionError,
    OneCommandRunnerBaseException,
    OneCommandRunnerExitCode,
    PCAttributionValidationException,
    sys_exit_after,
)
from fbpcs.private_computation.entity.infra_config import (
    PrivateComputationGameType,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.pcs_tier import PCSTier
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionRule,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    _build_private_computation_service,
    get_tier,
)


# dataset information fields
DATASETS_INFORMATION = "datasets_information"
TARGET_ID = "target_id"
INSTANCES = "instances"
NUM_SHARDS = "num_shards"
NUM_CONTAINERS = "num_containers"

# instance fields
TIMESTAMP = "timestamp"
ATTRIBUTION_RULE = "attribution_rule"
STATUS = "status"
CREATED_TIME = "created_time"
TIER = "tier"
FEATURE_LIST = "feature_list"

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


@sys_exit_after
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
    num_tries: Optional[int] = None,  # this is number of tries per stage
    final_stage: Optional[PrivateComputationBaseStageFlow] = None,
    run_id: Optional[str] = None,
) -> None:

    ## Step 1: Validation. Function arguments and  for private attribution run.
    # obtain the values in the dataset info vector.
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs] = BoltGraphAPIClient(
        config["graphapi"], logger
    )
    try:
        datasets_info = _get_attribution_dataset_info(client, dataset_id, logger)
    except GraphAPIGenericException as err:
        logger.error(err)
        raise PCAttributionValidationException(
            cause=f"Read attribution dataset {dataset_id} data failed.",
            remediation=f"Check access token has permission to read dataset {dataset_id}",
            exit_code=OneCommandRunnerExitCode.ERROR_READ_DATASET,
        )

    datasets = datasets_info[DATASETS_INFORMATION]
    target_id = datasets_info[TARGET_ID]
    # Verify adspixel
    _verify_adspixel(target_id, client)
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
    try:
        dataset_instance_data = _get_existing_pa_instances(client, dataset_id)
    except GraphAPIGenericException as err:
        logger.error(err)
        raise PCAttributionValidationException(
            cause=f"Read dataset instance {dataset_id} failed.",
            remediation=f"Check access token has permission to read dataset instance {dataset_id}",
            exit_code=OneCommandRunnerExitCode.ERROR_READ_PA_INSTANCE,
        )

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
        try:
            instance_id = _create_new_instance(
                dataset_id,
                int(dt_arg),
                attribution_rule_val,
                client,
                logger,
            )
        except GraphAPIGenericException as err:
            logger.error(err)
            raise PCAttributionValidationException(
                cause=f"Create dataset instance {dataset_id} failed.",
                remediation=f"Check access token has permission to create dataset instance {dataset_id}",
                exit_code=OneCommandRunnerExitCode.ERROR_CREATE_PA_INSTANCE,
            )

    instance_data = _get_pa_instance_info(client, instance_id, logger)
    _check_version(instance_data, config)
    # override stage flow based on pcs feature gate. Please contact PSI team to have a similar adoption
    stage_flow_override = stage_flow
    # get the enabled features
    pcs_features = _get_pcs_features(instance_data)
    pcs_feature_enums = []
    if pcs_features:
        logger.info(f"Enabled features: {pcs_features}")
        pcs_feature_enums = [PCSFeature.from_str(feature) for feature in pcs_features]
        stage_flow_override = get_stage_flow(
            game_type=PrivateComputationGameType.ATTRIBUTION,
            pcs_feature_enums=set(pcs_feature_enums),
            stage_flow_cls=stage_flow,
        )
    num_pid_containers = instance_data[NUM_SHARDS]
    num_mpc_containers = instance_data[NUM_CONTAINERS]

    ## Step 3. Populate instance args and create Bolt jobs
    publisher_args = BoltPlayerArgs(
        create_instance_args=BoltPAGraphAPICreateInstanceArgs(
            instance_id=instance_id,
            dataset_id=dataset_id,
            timestamp=str(dt_arg),
            attribution_rule=attribution_rule.name,
        )
    )
    partner_args = BoltPlayerArgs(
        create_instance_args=BoltPCSCreateInstanceArgs(
            instance_id=instance_id,
            role=PrivateComputationRole.PARTNER,
            game_type=PrivateComputationGameType.ATTRIBUTION,
            input_path=input_path,
            num_pid_containers=num_pid_containers,
            num_mpc_containers=num_mpc_containers,
            stage_flow_cls=stage_flow_override,
            concurrency=concurrency,
            attribution_rule=attribution_rule,
            aggregation_type=aggregation_type,
            num_files_per_mpc_container=num_files_per_mpc_container,
            k_anonymity_threshold=k_anonymity_threshold,
            pcs_features=pcs_features,
            run_id=run_id,
        )
    )
    job = BoltJob(
        job_name=f"Job [dataset_id: {dataset_id}][timestamp: {dt_arg}",
        publisher_bolt_args=publisher_args,
        partner_bolt_args=partner_args,
        num_tries=num_tries,
        final_stage=stage_flow_override.get_last_stage().previous_stage,
        poll_interval=60,
    )
    # Step 4. Run instances async

    logger.info(f"Started running instance {instance_id}.")
    all_run_success = asyncio.run(run_bolt(config, logger, [job]))
    logger.info(f"Finished running instance {instance_id}.")
    if not all(all_run_success):
        sys.exit(1)


async def run_bolt(
    config: Dict[str, Any],
    logger: logging.Logger,
    job_list: List[
        BoltJob[BoltPAGraphAPICreateInstanceArgs, BoltPCSCreateInstanceArgs]
    ],
) -> List[bool]:
    """Run private attribution with the BoltRunner in a dedicated function to ensure that
    the BoltRunner semaphore and runner.run_async share the same event loop.

    Arguments:
        config: The dict representation of a config.yml file
        logger: logger client
        job_list: The BoltJobs to execute
    """
    if not job_list:
        raise OneCommandRunnerBaseException(
            "Expected at least one job",
            "len(job_list) == 0",
            "Submit at least one job to call this API",
        )

    runner = BoltRunner(
        publisher_client=BoltGraphAPIClient(config=config["graphapi"], logger=logger),
        partner_client=BoltPCSClient(
            _build_private_computation_service(
                config["private_computation"],
                config["mpc"],
                config["pid"],
                config.get("post_processing_handlers", {}),
                config.get("pid_post_processing_handlers", {}),
            )
        ),
        logger=logger,
        max_parallel_runs=MAX_NUM_INSTANCES,
    )

    # run all jobs
    return await runner.run_async(job_list)


def _create_new_instance(
    dataset_id: str,
    timestamp: int,
    attribution_rule: str,
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs],
    logger: logging.Logger,
) -> str:
    instance_id = asyncio.run(
        client.create_instance(
            BoltPAGraphAPICreateInstanceArgs(
                instance_id="",
                dataset_id=dataset_id,
                timestamp=str(timestamp),
                attribution_rule=attribution_rule,
            )
        )
    )
    logger.info(
        f"Created instance {instance_id} for dataset {dataset_id} and attribution rule {attribution_rule}"
    )
    return instance_id


def _check_version(
    instance: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    """Checks that the publisher version (graph api) and the partner version (config.yml) are the same

    Arguments:
        instances: theoretically is dict representing the PA instance fields.
        config: The dict representation of a config.yml file

    Raises:
        IncorrectVersionError: the publisher and partner are running with different versions
    """

    instance_tier_str = instance.get(TIER)
    # if there is no tier for some reason, let's just assume
    # the tier is correct
    if not instance_tier_str:
        return

    config_tier = get_tier(config)
    expected_tier = PCSTier.from_str(instance_tier_str)
    if expected_tier is not config_tier:
        raise IncorrectVersionError.make_error(
            instance["id"], expected_tier, config_tier
        )


def _get_pcs_features(instance: Dict[str, Any]) -> Optional[List[str]]:
    return instance.get(FEATURE_LIST)


def _verify_adspixel(
    adspixels_id: str, client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs]
) -> None:
    try:
        client.get_adspixels(adspixels_id=adspixels_id, fields=["id"])
    except GraphAPIGenericException:
        raise PCAttributionValidationException(
            cause=f"Read adspixel {adspixels_id} failed.",
            remediation="Check access token has permission to read adspixel",
            exit_code=OneCommandRunnerExitCode.ERROR_READ_ADSPIXELS,
        )


def get_attribution_dataset_info(
    config: Dict[str, Any], dataset_id: str, logger: logging.Logger
) -> str:
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs] = BoltGraphAPIClient(
        config["graphapi"], logger
    )

    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [DATASETS_INFORMATION, TARGET_ID],
        ).text
    )


def _get_pa_instance_info(
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs],
    instance_id: str,
    logger: logging.Logger,
) -> Any:
    return json.loads(asyncio.run(client.get_instance(instance_id)).text)


def _iso_date_validator(timestamp: str) -> Any:
    try:
        datetime.strptime(timestamp, "%Y-%m-%d")
        return True
    except Exception:
        pass
    else:
        return False


def _get_attribution_dataset_info(
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs],
    dataset_id: str,
    logger: logging.Logger,
) -> Any:
    return json.loads(
        client.get_attribution_dataset_info(
            dataset_id,
            [DATASETS_INFORMATION, TARGET_ID],
        ).text
    )


def _get_existing_pa_instances(
    client: BoltGraphAPIClient[BoltPAGraphAPICreateInstanceArgs], dataset_id: str
) -> Any:
    return json.loads(client.get_existing_pa_instances(dataset_id).text)
