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
    CANCEL_STAGE_TIMEOUT,
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
from fbpcs.pl_coordinator.pc_partner_instance import PrivateComputationPartnerInstance
from fbpcs.pl_coordinator.pc_publisher_instance import (
    PrivateComputationPublisherInstance,
)
from fbpcs.pl_coordinator.pl_graphapi_utils import PLGraphAPIClient
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
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


def run_instance(
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
) -> None:
    num_tries = num_tries if num_tries is not None else MAX_TRIES
    if num_tries < MIN_TRIES or num_tries > MAX_TRIES:
        raise PCStudyValidationException(
            "Number of retries not allowed",
            f"num_tries must be between {MIN_TRIES} and {MAX_TRIES}.",
        )
    client = PLGraphAPIClient(config, logger)
    instance_runner = PLInstanceRunner(
        config,
        instance_id,
        input_path,
        num_mpc_containers,
        num_pid_containers,
        logger,
        client,
        num_tries,
        game_type,
        dry_run,
        stage_flow,
        attribution_rule,
        aggregation_type,
        concurrency,
        num_files_per_mpc_container,
        k_anonymity_threshold,
    )
    logger.info(f"Running private lift for instance {instance_id}")
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
                    "num_mpc_containers": num_shards,
                    "num_pid_containers": num_shards,
                    "stage_flow": stage_flow,
                    "logger": LoggerAdapter(logger=logger, prefix=instance_id),
                    "game_type": PrivateComputationGameType.LIFT,
                    "num_tries": num_tries,
                    "dry_run": dry_run,
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

    def __init__(
        self,
        config: Dict[str, Any],
        instance_id: str,
        input_path: str,
        num_mpc_containers: int,
        num_pid_containers: int,
        logger: logging.Logger,
        client: PLGraphAPIClient,
        num_tries: int,
        game_type: PrivateComputationGameType,
        dry_run: Optional[bool],
        stage_flow: Type[PrivateComputationBaseStageFlow],
        attribution_rule: Optional[AttributionRule] = None,
        aggregation_type: Optional[AggregationType] = None,
        concurrency: Optional[int] = None,
        num_files_per_mpc_container: Optional[int] = None,
        k_anonymity_threshold: Optional[int] = None,
    ) -> None:
        self.logger = logger
        self.instance_id = instance_id
        self.publisher = PrivateComputationPublisherInstance(
            instance_id, logger, client
        )
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
                        f"Private Lift run completed for instance {self.instance_id}. View results at {self.partner.output_dir}"
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
        self.logger.info(f"Running publisher-partner {stage.name}")
        # call publisher <STAGE>
        self.logger.info(f"Invoking publisher {stage.name}.")
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
        self.logger.info(f"Starting partner {stage.name}:")
        self.partner.run_stage(stage, server_ips)
        self.wait_stage_complete(stage)

    def wait_stage_complete(self, stage: PrivateComputationBaseStageFlow) -> None:
        start_status = stage.started_status
        complete_status = stage.completed_status
        fail_status = stage.failed_status
        timeout = stage.timeout

        start_time = time()
        cancel_time = 0
        while time() < start_time + timeout:
            self.publisher.update_instance()
            self.partner.update_instance()
            self.logger.info(
                f"Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
            )
            if (
                self.publisher.status is complete_status
                and self.partner.status is complete_status
            ):
                self.logger.info(f"Stage {stage.name} is complete.")
                return
            if (
                self.publisher.status
                in [fail_status, PrivateComputationInstanceStatus.TIMEOUT]
                or self.partner.status is fail_status
            ):
                if (
                    self.publisher.status
                    in [fail_status, PrivateComputationInstanceStatus.TIMEOUT]
                    and self.partner.status is start_status
                    and cancel_time <= CANCEL_STAGE_TIMEOUT
                ):
                    # wait 5 minutes for partner to become fail status on its own
                    # if not, only perform 'cancel_stage' one time
                    if cancel_time == CANCEL_STAGE_TIMEOUT:
                        self.logger.error(f"Canceling partner stage {stage.name}.")
                        self.partner.cancel_current_stage()
                    else:
                        self.logger.info(
                            f"Waiting to cancel partner stage {stage.name}."
                        )
                    # only cancel once
                    cancel_time += POLL_INTERVAL
                else:
                    raise PCInstanceCalculationException(
                        f"Stage {stage.name} failed.",
                        f"Publisher status: {self.publisher.status}. Partner status: {self.partner.status}.",
                        "Try running again",
                    )
            sleep(POLL_INTERVAL)
        raise PCInstanceCalculationException(
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}.",
            "unknown",
            "Try running again",
        )
