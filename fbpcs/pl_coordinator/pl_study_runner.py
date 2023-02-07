#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import asyncio
import calendar
import json
import logging
import time
from typing import Any, Dict, List, Optional, Type

from fbpcs.bolt.bolt_checkpoint import bolt_checkpoint
from fbpcs.bolt.bolt_hook import BoltHook, BoltHookArgs, BoltHookKey

from fbpcs.bolt.bolt_job import BoltJob, BoltPlayerArgs
from fbpcs.bolt.bolt_runner import BoltRunner
from fbpcs.bolt.bolt_summary import BoltSummary
from fbpcs.bolt.oss_bolt_pcs import BoltPCSClient, BoltPCSCreateInstanceArgs
from fbpcs.common.feature.pcs_feature_gate_utils import get_stage_flow
from fbpcs.common.service.graphapi_trace_logging_service import (
    GraphApiTraceLoggingService,
)
from fbpcs.common.service.input_data_service import InputDataService, TimestampValues
from fbpcs.common.service.retry_handler import BackoffType, RetryHandler
from fbpcs.common.service.trace_logging_service import TraceLoggingService
from fbpcs.pl_coordinator.bolt_graphapi_client import (
    BoltGraphAPIClient,
    BoltPLGraphAPICreateInstanceArgs,
    GRAPHAPI_INSTANCE_STATUSES,
)
from fbpcs.pl_coordinator.constants import MAX_NUM_INSTANCES
from fbpcs.pl_coordinator.exceptions import (
    GraphAPIGenericException,
    IncorrectVersionError,
    OneCommandRunnerBaseException,
    OneCommandRunnerExitCode,
    PCStudyValidationException,
    sys_exit_after,
)
from fbpcs.private_computation.entity.infra_config import (
    PrivateComputationGameType,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.pcs_tier import PCSTier
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import ResultVisibility
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    build_private_computation_service,
    get_instance,
    get_tier,
    get_trace_logging_service,
)

# study information fields
TYPE = "type"
STATUS = "status"
RUN_ID = "run_id"
START_TIME = "start_time"
OBSERVATION_END_TIME = "observation_end_time"
OBJECTIVES = "objectives"
OPP_DATA_INFORMATION = "opp_data_information"
INSTANCES = "instances"

# constants
LIFT = "LIFT"
ON = "ON"
MPC_CONVERSION = "MPC_CONVERSION"
SEC_IN_HOUR = 3600
SEC_IN_DAY = 86400
INSTANCE_LIFESPAN: int = SEC_IN_DAY
STUDY_EXPIRE_TIME: int = 90 * SEC_IN_DAY
CREATE_INSTANCE_TRIES = 3

LOG_COMPONENT = "pl_study_runner"


# TODO(T116497329): don't use unstructured entities in pl_study_runner.py


@sys_exit_after
def run_study(
    config: Dict[str, Any],
    study_id: str,
    objective_ids: List[str],
    input_paths: List[str],
    logger: logging.Logger,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    num_tries: Optional[int] = None,  # this is number of tries per stage
    dry_run: Optional[bool] = False,  # if set to true, it will only run one stage
    result_visibility: Optional[ResultVisibility] = None,
    final_stage: Optional[PrivateComputationBaseStageFlow] = None,
    run_id: Optional[str] = None,
    graphapi_version: Optional[str] = None,
    output_dir: Optional[str] = None,
    graphapi_domain: Optional[str] = None,
) -> None:
    bolt_summary = asyncio.run(
        run_study_async(
            config,
            study_id,
            objective_ids,
            input_paths,
            logger,
            stage_flow,
            num_tries,
            dry_run,
            result_visibility,
            final_stage,
            run_id,
            graphapi_version,
            output_dir,
            graphapi_domain,
        )
    )

    # TODO(T136692970): [BE] use is_success instead of hacky status check
    for s in bolt_summary.job_summaries:
        instance_id = s.partner_instance_id
        if (
            get_instance(config, instance_id, logger).infra_config.status
            is not PrivateComputationInstanceStatus.AGGREGATION_COMPLETED
        ):
            raise OneCommandRunnerBaseException(
                f"{instance_id=} FAILED.",
                "Status is not aggregation completed",
                "Check logs for more information",
            )


async def run_study_async(
    config: Dict[str, Any],
    study_id: str,
    objective_ids: List[str],
    input_paths: List[str],
    logger: logging.Logger,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    num_tries: Optional[int] = None,  # this is number of tries per stage
    dry_run: Optional[bool] = False,  # if set to true, it will only run one stage
    result_visibility: Optional[ResultVisibility] = None,
    final_stage: Optional[PrivateComputationBaseStageFlow] = None,
    run_id: Optional[str] = None,
    graphapi_version: Optional[str] = None,
    output_dir: Optional[str] = None,
    graphapi_domain: Optional[str] = None,
    bolt_hooks: Optional[Dict[BoltHookKey, List[BoltHook[BoltHookArgs]]]] = None,
) -> BoltSummary:

    # Create a GraphApiTraceLoggingService specific for this study_id
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs] = BoltGraphAPIClient(
        config=config,
        logger=logger,
        graphapi_version=graphapi_version,
        graphapi_domain=graphapi_domain,
    )
    endpoint_url = f"{client.graphapi_url}/{study_id}/checkpoint"
    default_trace_logger = GraphApiTraceLoggingService(
        access_token=client.access_token,
        endpoint_url=endpoint_url,
    )
    # if the user configured a trace logging service via the config.yml file, use that
    # instead of the default trace logger
    trace_logging_svc = get_trace_logging_service(
        config, default_trace_logger=default_trace_logger
    )
    # register the trace_logging_svc as a Bolt global default
    bolt_checkpoint.register_trace_logger(trace_logging_svc)
    # register the run id as a Bolt global default
    # sets a unique default run id if run_id was None
    run_id = bolt_checkpoint.register_run_id(run_id)

    return await _run_study_async_helper(
        client=client,
        trace_logging_svc=trace_logging_svc,
        config=config,
        study_id=study_id,
        objective_ids=objective_ids,
        input_paths=input_paths,
        logger=logger,
        stage_flow=stage_flow,
        num_tries=num_tries,
        dry_run=dry_run,
        result_visibility=result_visibility,
        final_stage=final_stage,
        run_id=run_id,
        graphapi_version=graphapi_version,
        output_dir=output_dir,
        graphapi_domain=graphapi_domain,
        bolt_hooks=bolt_hooks,
    )


@bolt_checkpoint(
    dump_params=True,
    include=["study_id", "objective_ids", "input_paths", "result_visibility"],
    dump_return_val=True,
    checkpoint_name="RUN_STUDY",
    component=LOG_COMPONENT,
)
async def _run_study_async_helper(
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
    trace_logging_svc: TraceLoggingService,
    *,
    config: Dict[str, Any],
    study_id: str,
    objective_ids: List[str],
    input_paths: List[str],
    logger: logging.Logger,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    num_tries: Optional[int],
    dry_run: Optional[bool],
    result_visibility: Optional[ResultVisibility],
    final_stage: Optional[PrivateComputationBaseStageFlow],
    run_id: Optional[str],
    graphapi_version: Optional[str],
    output_dir: Optional[str],
    graphapi_domain: Optional[str],
    bolt_hooks: Optional[Dict[BoltHookKey, List[BoltHook[BoltHookArgs]]]],
) -> BoltSummary:
    ## Step 1: Validation. Function arguments and study metadata must be valid for private lift run.
    _validate_input(objective_ids, input_paths)

    # obtain study information
    try:
        study_data = _get_study_data(study_id, client)
    except GraphAPIGenericException as err:
        logger.error(err)
        raise PCStudyValidationException(
            cause=f"Read study {study_id} data failed.",
            remediation=f"Check access token has permission to read study {study_id}",
            exit_code=OneCommandRunnerExitCode.ERROR_READ_STUDY,
        )

    # Verify study can run private lift:
    _verify_study_type(study_data)

    # verify mpc objectives
    _verify_mpc_objs(study_data, objective_ids, client)

    # verify study opp_data_information is non-empty
    if OPP_DATA_INFORMATION not in study_data:
        raise PCStudyValidationException(
            f"Study {study_id} has no opportunity datasets.",
            f"Check {study_id} study data to include {OPP_DATA_INFORMATION}",
        )

    ## Step 2. Preparation. Find which cell-obj pairs should have new instances created for and which should use existing
    ## valid ones. If a valid instance exists for a particular cell-obj pair, use it. Otherwise, try to create one.

    cell_obj_instance = _get_cell_obj_instance(
        study_data,
        objective_ids,
        input_paths,
    )
    _print_json(
        "Existing valid instances for cell-obj pairs", cell_obj_instance, logger
    )

    instance_ids_to_timestamps: Dict[str, TimestampValues] = {}

    # create new instances
    try:
        study_start_time = str(_date_to_timestamp(study_data[START_TIME]))
        observation_end_time = str(_date_to_timestamp(study_data[OBSERVATION_END_TIME]))
        await _create_new_instances(
            cell_obj_instance,
            study_id,
            client,
            logger,
            instance_ids_to_timestamps,
            study_start_time,
            observation_end_time,
            run_id,
        )
    except GraphAPIGenericException as err:
        logger.error(err)
        raise PCStudyValidationException(
            cause=f"Create PL instance on study {study_id} and cell-obj pairs {cell_obj_instance} failed.",
            remediation="Check access token has permission to create instance",
            exit_code=OneCommandRunnerExitCode.ERROR_CREATE_PL_INSTANCE,
        )

    _print_json("Instances to run for cell-obj pairs", cell_obj_instance, logger)
    # create a dict with {instance_id, input_path} pairs
    instances_input_path = _instance_to_input_path(cell_obj_instance)
    _print_json(
        "Instances will be calculated with corresponding input paths",
        instances_input_path,
        logger,
    )

    # check that the version in config.yml is same as from graph api
    try:
        await _check_versions(cell_obj_instance, config, client)
    except GraphAPIGenericException as err:
        logger.error(err)
        raise PCStudyValidationException(
            cause=f"Read PL instance on cell-obj pairs {cell_obj_instance} failed.",
            remediation="Check access token has permission to read instance",
            exit_code=OneCommandRunnerExitCode.ERROR_READ_PL_INSTANCE,
        )

    # override stage flow based on pcs feature gate. Please contact PSI team to have a similar adoption
    stage_flow_override = stage_flow
    # get the enabled features
    pcs_features = await _get_pcs_features(cell_obj_instance, client)
    pcs_feature_enums = set()
    if pcs_features:
        logger.info(f"Enabled features: {pcs_features}")
        pcs_feature_enums = {PCSFeature.from_str(feature) for feature in pcs_features}
        stage_flow_override = get_stage_flow(
            game_type=PrivateComputationGameType.LIFT,
            pcs_feature_enums=pcs_feature_enums,
            stage_flow_cls=stage_flow,
        )

    ## Step 3. Run Instances. Run maximum number of instances in parallel

    # using bolt runner
    # create the jobs
    all_instance_ids = []
    job_list = []

    for instance_id in instances_input_path.keys():
        timestamps = instance_ids_to_timestamps[instance_id]
        all_instance_ids.append(instance_id)
        data = instances_input_path[instance_id]
        input_path = data["input_path"]
        num_shards = data["num_shards"]
        cell_id = data["cell_id"]
        obj_id = data["objective_id"]
        publisher_args = BoltPlayerArgs(
            create_instance_args=BoltPLGraphAPICreateInstanceArgs(
                instance_id=instance_id,
                study_id=study_id,
                breakdown_key={
                    "cell_id": cell_id,
                    "objective_id": obj_id,
                },
                run_id=run_id,
            )
        )
        partner_args = BoltPlayerArgs(
            create_instance_args=BoltPCSCreateInstanceArgs(
                instance_id=instance_id,
                role=PrivateComputationRole.PARTNER,
                game_type=PrivateComputationGameType.LIFT,
                input_path=input_path,
                output_dir=output_dir if output_dir else "",
                num_pid_containers=int(num_shards),
                num_mpc_containers=int(num_shards),
                stage_flow_cls=stage_flow_override,
                result_visibility=result_visibility or ResultVisibility.PUBLIC,
                pcs_features=pcs_features,
                pid_configs=config["pid"],
                run_id=run_id,
                input_path_start_ts=timestamps.start_timestamp,
                input_path_end_ts=timestamps.end_timestamp,
            )
        )
        job = BoltJob(
            job_name=f"Job [cell_id: {cell_id}][obj_id: {obj_id}]",
            publisher_bolt_args=publisher_args,
            partner_bolt_args=partner_args,
            num_tries=num_tries,
            final_stage=stage_flow_override.get_last_stage().previous_stage,
            poll_interval=60,
            hooks=bolt_hooks or {},
        )
        job_list.append(job)

    bolt_summary = await run_bolt(
        client,
        trace_logging_svc,
        config,
        logger,
        job_list,
        graphapi_version=graphapi_version,
        graphapi_domain=graphapi_domain,
    )

    ## Step 4: Print out the initial and end states

    # Wait to resolve throttling issue
    # - Re-attempts every 5 minutes for 75 minutes total before failing
    # - https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
    with RetryHandler(
        logger=logger,
        backoff_seconds=300,
        backoff_type=BackoffType.CONSTANT,
        max_attempts=15,
    ) as retry_handler:
        end_state_study_data = await retry_handler.execute_sync(
            _get_study_data, study_id, client
        )

    new_cell_obj_instances = _get_cell_obj_instance(
        end_state_study_data,
        objective_ids,
        input_paths,
    )

    _print_json(
        "Pre-run statuses for instance of each cell-objective pair",
        cell_obj_instance,
        logger,
    )
    _print_json(
        "Post-run statuses for instance of each cell-objective pair",
        new_cell_obj_instances,
        logger,
    )

    logger.info(bolt_summary)
    return bolt_summary


@bolt_checkpoint(component=LOG_COMPONENT)
async def run_bolt(
    publisher_client: BoltGraphAPIClient,
    trace_logging_svc: TraceLoggingService,
    config: Dict[str, Any],
    logger: logging.Logger,
    job_list: List[
        BoltJob[BoltPLGraphAPICreateInstanceArgs, BoltPCSCreateInstanceArgs]
    ],
    graphapi_version: Optional[str] = None,
    graphapi_domain: Optional[str] = None,
) -> BoltSummary:
    """Run private lift with the BoltRunner in a dedicated function to ensure that
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

    # create the runner
    runner = BoltRunner(
        publisher_client=publisher_client,
        partner_client=BoltPCSClient(
            build_private_computation_service(
                pc_config=config["private_computation"],
                mpc_config=config["mpc"],
                pid_config=config["pid"],
                pph_config=config.get("post_processing_handlers", {}),
                pid_pph_config=config.get("pid_post_processing_handlers", {}),
                trace_logging_svc=trace_logging_svc,
            ),
        ),
        logger=logger,
        max_parallel_runs=MAX_NUM_INSTANCES,
    )

    # run all jobs
    return await runner.run_async(job_list)


@bolt_checkpoint(component=LOG_COMPONENT)
def _validate_input(objective_ids: List[str], input_paths: List[str]) -> None:
    err_msgs = []
    # verify that input is valid.
    # Deny if
    #   a. objective_ids have duplicate
    #   b. input_paths have duplicate
    #   c. their lengths don't match
    if _has_duplicates(objective_ids):
        err_msgs.append("objective_ids have duplicates")
    if _has_duplicates(input_paths):
        err_msgs.append("input_paths have duplicates")

    if len(objective_ids) != len(input_paths):
        err_msgs.append(
            "Number of objective_ids and number of input_paths don't match."
        )
    if err_msgs:
        raise PCStudyValidationException(
            _join_err_msgs(err_msgs),
            "ensure objective_ids,input_paths have no duplicate and should be same 1-1 mapping",
        )


@bolt_checkpoint(component=LOG_COMPONENT)
def _verify_study_type(study_data: Dict[str, Any]) -> None:
    # Deny if study is
    #   a. not LIFT
    #   b. has not started yet
    #   c. finished more than 90 days
    #
    # This logic should be in sync with the logic here https://fburl.com/diffusion/qyjl89qn
    err_msgs = []
    current_time = int(time.time())
    if study_data[TYPE] != LIFT:
        err_msgs.append(f"Expected study type: {LIFT}. Study type: {study_data[TYPE]}.")
    study_start_time = _date_to_timestamp(study_data[START_TIME])
    if study_start_time > current_time:
        err_msgs.append(
            f"Study must have started. Study start time: {study_start_time}. Current time: {current_time}."
        )
    observation_end_time = _date_to_timestamp(study_data[OBSERVATION_END_TIME])
    if observation_end_time + STUDY_EXPIRE_TIME < current_time:
        err_msgs.append("Cannot run for study that finished more than 90 days ago.")
    if err_msgs:
        raise PCStudyValidationException(
            _join_err_msgs(err_msgs),
            f"ensure {study_data['id']} study is LIFT, must have started, finished less than 90 days ago.",
        )


@bolt_checkpoint(dump_params=True, include=["adpixels_ids"], component=LOG_COMPONENT)
def _verify_adspixels_if_exist(
    adspixels_ids: List[str],
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
) -> None:
    if adspixels_ids:
        try:
            for pixel_id in adspixels_ids:
                client.get_adspixels(adspixels_id=pixel_id, fields=["id"])
        except GraphAPIGenericException:
            raise PCStudyValidationException(
                cause=f"Read adspixel {adspixels_ids} failed.",
                remediation="Check access token has permission to read adspixel",
                exit_code=OneCommandRunnerExitCode.ERROR_READ_ADSPIXELS,
            )


@bolt_checkpoint(component=LOG_COMPONENT)
def _verify_mpc_objs(
    study_data: Dict[str, Any],
    objective_ids: List[str],
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
) -> None:
    # verify study has mpc objectives
    mpc_objectives = list(
        filter(
            lambda obj: obj["type"] == MPC_CONVERSION,
            study_data["objectives"]["data"],
        )
    )

    if not mpc_objectives:
        raise PCStudyValidationException(
            f"Study {study_data['id']} has no MPC objectives",
            "check study data that need to have MPC objectives",
        )

    # verify adspixels read if exist
    adspixels_ids = []
    for obj in mpc_objectives:
        if "adspixels" in obj:
            adspixels = obj["adspixels"]["data"]
            for pixel in adspixels:
                adspixels_ids.append(pixel["id"])

    _verify_adspixels_if_exist(adspixels_ids, client)

    mpc_objectives_ids = [obj["id"] for obj in mpc_objectives]
    # verify input objs are MPC objs of this study.
    for obj_id in objective_ids:
        if obj_id not in mpc_objectives_ids:
            raise PCStudyValidationException(
                f"Objective id {obj_id} invalid. Valid MPC objective ids for study {study_data['id']}: {','.join(mpc_objectives_ids)}",
                "input objs are MPC objs of this study.",
            )


@bolt_checkpoint(
    dump_params=True,
    include=["study_id"],
    component=LOG_COMPONENT,
)
def _get_study_data(
    study_id: str, client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs]
) -> Any:
    return json.loads(
        client.get_study_data(
            study_id,
            [
                TYPE,
                START_TIME,
                OBSERVATION_END_TIME,
                OBJECTIVES,
                OPP_DATA_INFORMATION,
                INSTANCES,
            ],
        ).text
    )


def get_runnable_objectives(
    study_id: str,
    config: Dict[str, Any],
    logger: logging.Logger,
    graphapi_version: Optional[str] = None,
    graphapi_domain: Optional[str] = None,
) -> List[str]:
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs] = BoltGraphAPIClient(
        config=config,
        logger=logger,
        graphapi_version=graphapi_version,
        graphapi_domain=graphapi_domain,
    )

    study_data = _get_study_data(study_id, client)
    mpc_objective_ids = [
        objective_dict["id"]
        for objective_dict in study_data["objectives"]["data"]
        if objective_dict["type"] == MPC_CONVERSION
    ]
    cell_obj_instances = _get_cell_obj_instance(
        study_data, mpc_objective_ids, ["fake_input_path.csv"] * len(mpc_objective_ids)
    )

    runnable_objective_ids = [
        objective_id
        for cell_id in cell_obj_instances
        for objective_id in cell_obj_instances[cell_id]
        # if instance_id *is* in the dict, it means there is either an ongoing run
        # or a completed run within 24 hours
        if "instance_id" not in cell_obj_instances[cell_id][objective_id]
    ]

    logger.info(f"MPC objectives: {mpc_objective_ids}")
    logger.info(f"Runnable objectives: {runnable_objective_ids}")

    return runnable_objective_ids


@bolt_checkpoint(component=LOG_COMPONENT)
def _get_cell_obj_instance(
    study_data: Dict[str, Any],
    objective_ids: List[str],
    input_paths: List[str],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    # only consider cells in OPP_DATA_INFORMATION (opportunity datasets available).
    cells_data: List[str] = study_data[OPP_DATA_INFORMATION]
    # only consider objective_ids from function arguments (conversion datasets available).
    objectives_data: Dict[str, str] = dict(zip(objective_ids, input_paths))
    # for some cell_obj pairs, valid instances already exist
    instances_data: List[Dict[str, Any]] = (
        study_data[INSTANCES]["data"] if INSTANCES in study_data else []
    )
    current_time = int(time.time())
    cell_obj_instance = {}
    # find the latest_data_ts and input_path for all cell-obj pairs
    for cell_data in cells_data:
        cell_data = json.loads(cell_data)
        cell_id = str(cell_data["breakdowns"]["cell_id"])
        latest_data_ts = cell_data["latest_data_ts"]
        num_shards = cell_data["num_shards"]
        cell_obj_instance[cell_id] = {}
        for objective_id in objectives_data:
            cell_obj_instance[cell_id][objective_id] = {
                "latest_data_ts": latest_data_ts,
                "input_path": objectives_data[objective_id],
                "num_shards": num_shards,
            }
    # for these cell-obj pairs, find those with valid instances
    for instance_data in instances_data:
        breakdown_key = json.loads(instance_data["breakdown_key"])
        cell_id = str(breakdown_key["cell_id"])
        objective_id = str(breakdown_key["objective_id"])

        # If to-be-calculated cell-obj pairs does not include this instance's
        # cell-obj pair, skip.
        if (
            cell_id not in cell_obj_instance
            or objective_id not in cell_obj_instance[cell_id]
        ):
            continue
        created_time = _date_to_timestamp(instance_data["created_time"])
        status = GRAPHAPI_INSTANCE_STATUSES.get(instance_data[STATUS])
        if not status:
            logging.warning(
                f"Status in {instance_data} could not be mapped to a PrivateComputationInstanceStatus"
            )

        # Instance is valid if it has not expired and it was created after opp_data upload time
        # Duplicates shouldn't occur if all instances of this study were created by partner. If
        # they do, select a random one.
        if (
            status
            and created_time
            > cell_obj_instance[cell_id][objective_id]["latest_data_ts"]
            and (created_time > current_time - INSTANCE_LIFESPAN)
        ):
            cell_obj_instance[cell_id][objective_id]["instance_id"] = instance_data[
                "id"
            ]
            cell_obj_instance[cell_id][objective_id][STATUS] = status.value

    return cell_obj_instance


@bolt_checkpoint(dump_params=True, include=["study_id"], component=LOG_COMPONENT)
async def _create_new_instances(
    cell_obj_instances: Dict[str, Dict[str, Any]],
    study_id: str,
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
    logger: logging.Logger,
    instance_ids_to_timestamps: Dict[str, TimestampValues],
    study_start_time: str,
    observation_end_time: str,
    run_id: Optional[str] = None,
) -> None:
    for cell_id in cell_obj_instances:
        for objective_id in cell_obj_instances[cell_id]:
            # Create new instance for cell_obj pairs which has no valid instance.
            if "instance_id" not in cell_obj_instances[cell_id][objective_id]:
                cell_obj_instances[cell_id][objective_id][
                    "instance_id"
                ] = await _create_instance_retry(
                    client, study_id, cell_id, objective_id, run_id, logger
                )
                cell_obj_instances[cell_id][objective_id][
                    STATUS
                ] = PrivateComputationInstanceStatus.CREATED.value

            instance_id = cell_obj_instances[cell_id][objective_id]["instance_id"]
            is_pl_timestamp_validation_enabled = await client.has_feature(
                instance_id, PCSFeature.PL_TIMESTAMP_VALIDATION
            )
            timestamps = InputDataService.get_lift_study_timestamps(
                study_start_time,
                observation_end_time,
                is_pl_timestamp_validation_enabled,
            )
            instance_ids_to_timestamps[instance_id] = timestamps


@bolt_checkpoint(
    dump_params=True,
    include=["study_id", "cell_id", "objective_id"],
    dump_return_val=True,
    component=LOG_COMPONENT,
)
async def _create_instance_retry(
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
    study_id: str,
    cell_id: str,
    objective_id: str,
    run_id: Optional[str],
    logger: logging.Logger,
) -> str:
    tries = 0
    while tries < CREATE_INSTANCE_TRIES:
        tries += 1
        try:
            instance_id = await client.create_instance(
                BoltPLGraphAPICreateInstanceArgs(
                    instance_id="",
                    study_id=study_id,
                    breakdown_key={
                        "cell_id": cell_id,
                        "objective_id": objective_id,
                    },
                    run_id=run_id,
                )
            )
            logger.info(
                f"Created instance {instance_id} for cell {cell_id} and objective {objective_id}"
            )
            return instance_id
        except GraphAPIGenericException as err:
            if tries >= CREATE_INSTANCE_TRIES:
                logger.error(
                    f"Error: Instance not created for cell {cell_id} and {objective_id}"
                )
                raise err
            logger.info(
                f"Instance not created for cell {cell_id} and {objective_id}. Retrying:"
            )
    return ""  # this is to make pyre happy


@bolt_checkpoint(dump_return_val=True, component=LOG_COMPONENT)
def _instance_to_input_path(
    cell_obj_instance: Dict[str, Dict[str, Dict[str, Any]]]
) -> Dict[str, Dict[str, str]]:
    instance_input_path = {}
    for cell_id in cell_obj_instance:
        for objective_id in cell_obj_instance[cell_id]:
            data = cell_obj_instance[cell_id][objective_id]
            if (
                "instance_id" in data
                and STATUS in data
                and data[STATUS]
                is not PrivateComputationInstanceStatus.AGGREGATION_COMPLETED.value
            ):
                instance_input_path[data["instance_id"]] = {
                    "cell_id": cell_id,
                    "objective_id": objective_id,
                    "input_path": data["input_path"],
                    "num_shards": data["num_shards"],
                }
    return instance_input_path


@bolt_checkpoint(component=LOG_COMPONENT)
async def _check_versions(
    cell_obj_instances: Dict[str, Dict[str, Dict[str, Any]]],
    config: Dict[str, Any],
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
) -> None:
    """Checks that the publisher version (graph api) and the partner version (config.yml) are the same

    Arguments:
        cell_obj_instances: theoretically is dict mapping cell->obj->instance.
        config: The dict representation of a config.yml file
        client: Interface for submitting graph API requests

    Raises:
        IncorrectVersionError: the publisher and partner are running with different versions
    """

    config_tier = get_tier(config)

    for cell_id in cell_obj_instances:
        for objective_id in cell_obj_instances[cell_id]:
            instance_data = cell_obj_instances[cell_id][objective_id]
            instance_id = instance_data["instance_id"]
            # if there is no tier for some reason (e.g. old study?), let's just assume
            # the tier is correct
            tier_str = json.loads((await client.get_instance(instance_id)).text).get(
                "tier"
            )
            if tier_str:
                expected_tier = PCSTier.from_str(tier_str)
                if expected_tier is not config_tier:
                    raise IncorrectVersionError.make_error(
                        instance_id, expected_tier, config_tier
                    )


@bolt_checkpoint(dump_return_val=True, component=LOG_COMPONENT)
async def _get_pcs_features(
    cell_obj_instances: Dict[str, Dict[str, Dict[str, Any]]],
    client: BoltGraphAPIClient[BoltPLGraphAPICreateInstanceArgs],
) -> Optional[List[str]]:
    for cell_id in cell_obj_instances:
        for objective_id in cell_obj_instances[cell_id]:
            instance_data = cell_obj_instances[cell_id][objective_id]
            instance_id = instance_data["instance_id"]
            feature_list = json.loads(
                (await client.get_instance(instance_id)).text
            ).get("feature_list")
            if feature_list:
                return feature_list


def _date_to_timestamp(time_str: str) -> int:
    return calendar.timegm(time.strptime(time_str, "%Y-%m-%dT%H:%M:%S+0000"))


def _has_duplicates(str_list: List[str]) -> bool:
    return len(str_list) is not len(set(str_list))


def _join_err_msgs(err_msgs: List[str]) -> str:
    err_msgs = [f"Error: {msg}" for msg in err_msgs]
    return "\n".join(err_msgs)


def _print_json(msg: str, data: Dict[str, Any], logger: logging.Logger) -> None:
    logger.info(f"{msg}:\n{json.dumps(data, indent=4, sort_keys=True)}")
