#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import logging
from multiprocessing import Process
from time import sleep, time
from typing import Any, Dict, List, Optional, Type

from fbpcs.pl_coordinator.constants import (
    INSTANCE_SLA,
    MAX_NUM_INSTANCES,
    MAX_TRIES,
    MIN_NUM_INSTANCES,
    MIN_TRIES,
    POLL_INTERVAL,
    PROCESS_WAIT,
    RETRY_INTERVAL,
    WAIT_VALID_STAGE_TIMEOUT,
    WAIT_VALID_STATUS_TIMEOUT,
)
from fbpcs.pl_coordinator.exceptions import (
    IncompatibleStageError,
    PCInstanceCalculationException,
    PCStudyValidationException,
)
from fbpcs.pl_coordinator.pc_graphapi_utils import PCGraphAPIClient
from fbpcs.pl_coordinator.pc_partner_instance import PrivateComputationPartnerInstance
from fbpcs.pl_coordinator.pc_publisher_instance import (
    PrivateComputationPublisherInstance,
)
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionRule,
    ResultVisibility,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.utils.logger_adapter import LoggerAdapter
from termcolor import colored


def run_instance(
    *,
    config: Dict[str, Any],
    instance_id: str,
    input_path: str,
    num_mpc_containers: int,
    num_pid_containers: int,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    logger: logging.Logger,
    game_type: PrivateComputationGameType,
    attribution_rule: Optional[AttributionRule] = None,
    aggregation_type: Optional[AggregationType] = None,
    concurrency: Optional[int] = None,
    num_files_per_mpc_container: Optional[int] = None,
    k_anonymity_threshold: Optional[int] = None,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
    dry_run: Optional[bool] = False,
    result_visibility: Optional[ResultVisibility] = None,
    pcs_features: Optional[List[str]] = None,
) -> None:
    num_tries = num_tries if num_tries is not None else MAX_TRIES
    if num_tries < MIN_TRIES or num_tries > MAX_TRIES:
        raise PCStudyValidationException(
            "Number of retries not allowed",
            f"num_tries must be between {MIN_TRIES} and {MAX_TRIES}.",
        )
    client = PCGraphAPIClient(config, logger)
    instance_runner = PLInstanceRunner(
        config=config,
        instance_id=instance_id,
        input_path=input_path,
        num_mpc_containers=num_mpc_containers,
        num_pid_containers=num_pid_containers,
        logger=logger,
        client=client,
        num_tries=num_tries,
        game_type=game_type,
        dry_run=dry_run,
        stage_flow=stage_flow,
        attribution_rule=attribution_rule,
        aggregation_type=aggregation_type,
        concurrency=concurrency,
        num_files_per_mpc_container=num_files_per_mpc_container,
        k_anonymity_threshold=k_anonymity_threshold,
        result_visibility=result_visibility,
        pcs_features=pcs_features,
    )
    logger.info(f"Running private {game_type.name.lower()} for instance {instance_id}")
    instance_runner.run()


def run_instances(
    config: Dict[str, Any],
    instance_ids: List[str],
    input_paths: List[str],
    num_shards_list: List[str],
    stage_flow: Type[PrivateComputationBaseStageFlow],
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
    dry_run: Optional[bool] = False,
    result_visibility: Optional[ResultVisibility] = None,
    pcs_features: Optional[List[str]] = None,
) -> None:
    if len(instance_ids) is not len(input_paths):
        raise PCStudyValidationException(
            f"# instances: {len(instance_ids)} != # input paths: {len(input_paths)}",
            "Number of instances and number of input paths must be the same",
        )
    if len(input_paths) is not len(num_shards_list):
        raise PCStudyValidationException(
            f"# input paths: {len(input_paths)} != # shards: {len(num_shards_list)}",
            "Number of input paths and number of num_shards must be the same",
        )
    if not MIN_NUM_INSTANCES <= len(instance_ids) <= MAX_NUM_INSTANCES:
        raise PCStudyValidationException(
            "Number of instances not allowed",
            f"Number of instances must be between {MIN_NUM_INSTANCES} and {MAX_NUM_INSTANCES}",
        )
    processes = list(
        map(
            lambda instance_id, input_path, num_shards: Process(
                target=run_instance,
                kwargs={
                    "config": config,
                    "instance_id": instance_id,
                    "input_path": input_path,
                    "num_mpc_containers": num_shards,  # Currently ignored due to D35852672.
                    "num_pid_containers": num_shards,
                    "stage_flow": stage_flow,
                    "logger": LoggerAdapter(logger=logger, prefix=instance_id),
                    "game_type": PrivateComputationGameType.LIFT,
                    "num_tries": num_tries,
                    "dry_run": dry_run,
                    "result_visibility": result_visibility,
                    "pcs_features": pcs_features,
                },
            ),
            instance_ids,
            input_paths,
            num_shards_list,
        )
    )
    for process in processes:
        process.start()
        sleep(PROCESS_WAIT)
    for process in processes:
        process.join(INSTANCE_SLA)


class PLInstanceRunner:
    """
    Private Lift Partner-Publisher computation for an instance.
    """

    # TODO(T124214185): [BE] make PLInstanceRunner args keyword only
    def __init__(
        self,
        config: Dict[str, Any],
        instance_id: str,
        input_path: str,
        num_mpc_containers: int,
        num_pid_containers: int,
        logger: logging.Logger,
        client: PCGraphAPIClient,
        num_tries: int,
        game_type: PrivateComputationGameType,
        dry_run: Optional[bool],
        stage_flow: Type[PrivateComputationBaseStageFlow],
        attribution_rule: Optional[AttributionRule] = None,
        aggregation_type: Optional[AggregationType] = None,
        concurrency: Optional[int] = None,
        num_files_per_mpc_container: Optional[int] = None,
        k_anonymity_threshold: Optional[int] = None,
        result_visibility: Optional[ResultVisibility] = None,
        pcs_features: Optional[List[str]] = None,
    ) -> None:
        self.logger = logger
        self.instance_id = instance_id
        self.publisher = PrivateComputationPublisherInstance(
            instance_id, logger, client
        )
        self.game_type = game_type
        self.partner = PrivateComputationPartnerInstance(
            instance_id=instance_id,
            config=config,
            input_path=input_path,
            game_type=game_type,
            attribution_rule=attribution_rule,
            aggregation_type=aggregation_type,
            concurrency=concurrency,
            num_files_per_mpc_container=num_files_per_mpc_container,
            k_anonymity_threshold=k_anonymity_threshold,
            num_mpc_containers=num_mpc_containers,
            num_pid_containers=num_pid_containers,
            logger=logger,
            result_visibility=result_visibility,
            pcs_features=pcs_features,
        )
        self.num_tries = num_tries
        self.dry_run = dry_run
        self.stage_flow = stage_flow

    def get_valid_stage(self) -> Optional[PrivateComputationBaseStageFlow]:
        if not self.is_finished():
            publisher_stage = self.publisher.get_valid_stage(self.stage_flow)
            partner_stage = self.partner.get_valid_stage(self.stage_flow)

            # expected for all joint stages
            if publisher_stage is partner_stage:
                return publisher_stage

            elif publisher_stage is None:
                return partner_stage
            elif partner_stage is None:
                return publisher_stage

            elif publisher_stage is partner_stage.previous_stage:
                # if it's not a joint stage, the statuses don't matter at all since
                # each party operates independently
                # Example: publisher is PREPARE_DATA_FAILED, partner is PREPARE_DATA_COMPLETED
                if not publisher_stage.is_joint_stage or (
                    # it's fine if one party is completed and the other is started
                    # because the one with the started status just needs to call
                    # update_instance one more time
                    # Example: publisher is COMPUTATION_STARTED, partner is COMPUTATION_COMPLETED
                    self.stage_flow.is_started_status(self.publisher.status)
                    and self.stage_flow.is_completed_status(self.partner.status)
                ):
                    return publisher_stage
            elif partner_stage is publisher_stage.previous_stage:
                # Example: publisher is PREPARE_DATA_COMPLETED, partner is PREPARE_DATA_FAILED
                if not partner_stage.is_joint_stage or (
                    # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_STARTED
                    self.stage_flow.is_started_status(self.partner.status)
                    and self.stage_flow.is_completed_status(self.publisher.status)
                ):
                    return partner_stage

            # Example: partner is CREATED, publisher is PID_PREPARE_COMPLETED
            # Example: publisher is COMPUTATION COMPLETED, partner is PREPARE_COMPLETED
            # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_FAILED
            raise IncompatibleStageError.make_error(
                publisher_stage.name, partner_stage.name
            )
        return None

    def wait_valid_stage(self, timeout: int) -> PrivateComputationBaseStageFlow:
        self.logger.info("Polling instances expecting valid stage.")
        if timeout <= 0:
            raise ValueError(f"Timeout must be > 0, not {timeout}")
        start_time = time()
        while time() < start_time + timeout:
            valid_stage = self.get_valid_stage()
            if valid_stage is None:
                self.logger.info(
                    f"Valid stage not found. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}"
                )
                sleep(POLL_INTERVAL)
            else:
                self.logger.info(f"Valid stage found: {valid_stage}")
                return valid_stage
        raise PCInstanceCalculationException(
            "Timeout error",
            f"Waiting for valid stage timed out after {timeout}s.",
            "Try running again",
        )

    def is_finished(self) -> bool:
        return self.publisher.is_finished() and self.partner.is_finished()

    def run(self) -> None:
        tries = 0
        while tries < self.num_tries:
            tries += 1
            try:
                if self.is_finished():
                    self.logger.info(
                        f"Private {self.game_type.name.title()} run completed for instance {self.instance_id}. View results at {self.partner.output_dir}"
                    )
                    return
                # in case the publisher has a status of TIMEOUT
                self.publisher.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)
                valid_stage = self.wait_valid_stage(WAIT_VALID_STAGE_TIMEOUT)
                if valid_stage is not None:
                    # disable retries by MAX_TRIES+1 to prevent retries if stage is not retryable
                    if not valid_stage.is_retryable:
                        tries = MAX_TRIES + 1

                    self.run_stage(valid_stage)
                    # run the next stage
                    if not self.dry_run:
                        self.run()
                break
            except Exception as e:
                if tries >= self.num_tries:
                    raise e
                self.logger.error(
                    f"Error: type: {type(e)}, message: {e}. Retries left: {self.num_tries - tries}."
                )
                sleep(RETRY_INTERVAL)

    def run_stage(self, stage: PrivateComputationBaseStageFlow) -> None:
        self.logger.info(
            colored(
                f"Running publisher-partner {stage.name}",
                "green",
                attrs=[
                    "bold",
                ],
            )
        )
        # call publisher <STAGE>
        self.logger.info(
            colored(
                f"Invoking publisher {stage.name}.",
                "blue",
                attrs=[
                    "bold",
                ],
            )
        )
        self.publisher.run_stage(stage)
        server_ips = None
        # if it's a joint stage, it means partner must wait for publisher to provide server ips.
        # if it is not a joint stage, publisher and partner can run in parallel
        if stage.is_joint_stage:
            # keep polling graphapi until publisher status is <STAGE>_STARTED and server_ips are available
            self.publisher.wait_stage_start(stage)
            server_ips = self.publisher.server_ips
            if server_ips is None:
                raise ValueError(f"{stage.name} requires server ips but got none.")
        self.logger.info(
            colored(
                f"Starting partner {stage.name}:",
                "blue",
                attrs=[
                    "bold",
                ],
            )
        )
        self.partner.run_stage(stage, server_ips)
        self.wait_stage_complete(stage)

    def wait_stage_complete(self, stage: PrivateComputationBaseStageFlow) -> None:
        complete_status = stage.completed_status
        fail_status = stage.failed_status
        timeout = stage.timeout

        start_time = time()
        while time() < start_time + timeout:
            self.publisher.update_instance()
            self.partner.update_instance()
            self.logger.info(
                f"Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
            )
            # stages are completed
            if (
                self.publisher.status is complete_status
                and self.partner.status is complete_status
            ):
                self.logger.info(
                    colored(
                        f"Stage {stage.name} is complete.",
                        "green",
                        attrs=[
                            "bold",
                        ],
                    )
                )
                return

            # stage fail, tear down partner-side service
            if (
                self.publisher.status
                in [fail_status, PrivateComputationInstanceStatus.TIMEOUT]
                or self.partner.status is fail_status
            ):
                # trying to cancel partner stage only in joint stage (even it's fail status)
                if stage.is_joint_stage:
                    try:
                        self.logger.error(
                            f"Publisher status: {self.publisher.status}. Canceling partner stage {stage.name}."
                        )
                        self.partner.cancel_current_stage()
                    except Exception as e:
                        self.logger.error(
                            f"Unable to cancal current stage {stage.name}. Error: type: {type(e)}, message: {e}."
                        )

                raise PCInstanceCalculationException(
                    f"Stage {stage.name} failed.",
                    f"Publisher status: {self.publisher.status}. Partner status: {self.partner.status}.",
                    "Try running again",
                )

            # keep polling
            sleep(POLL_INTERVAL)

        raise PCInstanceCalculationException(
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}.",
            "unknown",
            "Try running again",
        )
