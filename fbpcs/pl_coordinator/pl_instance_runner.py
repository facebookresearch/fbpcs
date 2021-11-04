#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
from multiprocessing import Process
from time import sleep, time
from typing import Any, Dict, List, Optional, Type

from fbpcs.pl_coordinator.pl_graphapi_utils import (
    GraphAPIGenericException,
    PLGraphAPIClient,
    GRAPHAPI_INSTANCE_STATUSES,
)
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_legacy_stage_flow import (
    PrivateComputationLegacyStageFlow,
)
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    aggregate_shards,
    cancel_current_stage,
    compute_metrics,
    create_instance,
    get_instance,
    id_match,
    prepare_compute_input,
    run_stage,
)


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, prefix: str):
        super(LoggerAdapter, self).__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return "[%s] %s" % (self.prefix, msg), kwargs


INVALID_STATUS_LIST = [
    PrivateComputationInstanceStatus.UNKNOWN,
    PrivateComputationInstanceStatus.PROCESSING_REQUEST,
    PrivateComputationInstanceStatus.TIMEOUT,
]

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


class PLInstanceCalculationException(RuntimeError):
    pass


def run_instance(
    config: Dict[str, Any],
    instance_id: str,
    input_path: str,
    num_shards: int,
    stage_flow: Type[PrivateComputationBaseStageFlow],
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
    dry_run: Optional[bool] = False,
) -> None:
    num_tries = num_tries if num_tries is not None else MAX_TRIES
    if num_tries < MIN_TRIES or num_tries > MAX_TRIES:
        raise ValueError(f"num_tries must be between {MIN_TRIES} and {MAX_TRIES}.")
    client = PLGraphAPIClient(config["graphapi"]["access_token"], logger)
    instance_runner = PLInstanceRunner(
        config,
        instance_id,
        input_path,
        num_shards,
        logger,
        client,
        num_tries,
        dry_run,
        stage_flow,
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
        raise ValueError(
            "Number of instances and number of input paths must be the same"
        )
    if len(input_paths) is not len(num_shards_list):
        raise ValueError(
            "Number of input paths and number of num_shards must be the same"
        )
    if not MIN_NUM_INSTANCES <= len(instance_ids) <= MAX_NUM_INSTANCES:
        raise ValueError(
            f"Number of instances must be between {MIN_NUM_INSTANCES} and {MAX_NUM_INSTANCES}"
        )
    processes = list(
        map(
            lambda instance_id, input_path, num_shards: Process(
                target=run_instance,
                kwargs={
                    "config": config,
                    "instance_id": instance_id,
                    "input_path": input_path,
                    "num_shards": num_shards,
                    "stage_flow": stage_flow,
                    "logger": LoggerAdapter(logger=logger, prefix=instance_id),
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


class PrivateLiftCalcInstance:
    """
    Representation of a publisher or partner instance being calculated.
    """

    def __init__(
        self, instance_id: str, logger: logging.Logger, role: PrivateComputationRole
    ) -> None:
        self.instance_id: str = instance_id
        self.logger: logging.Logger = logger
        self.role: PrivateComputationRole = role
        self.status: PrivateComputationInstanceStatus = (
            PrivateComputationInstanceStatus.UNKNOWN
        )

    def update_instance(self) -> None:
        raise NotImplementedError(
            "This is a parent method to be overrided and should not be called."
        )

    def status_ready(self, status: PrivateComputationInstanceStatus) -> bool:
        self.logger.info(f"{self.role} instance status: {self.status}.")
        return self.status is status

    def wait_valid_status(
        self,
        timeout: int,
    ) -> None:
        self.update_instance()
        if self.status in INVALID_STATUS_LIST:
            self.logger.info(
                f"{self.role} instance status {self.status} invalid for calculation."
            )
            self.logger.info(f"Poll {self.role} instance expecting valid status.")
            if timeout <= 0:
                raise ValueError(f"Timeout must be > 0, not {timeout}")
            start_time = time()
            while time() < start_time + timeout:
                self.update_instance()
                self.logger.info(f"{self.role} instance status: {self.status}.")
                if self.status not in INVALID_STATUS_LIST:
                    self.logger.info(
                        f"{self.role} instance has valid status: {self.status}."
                    )
                    return
                sleep(POLL_INTERVAL)
            raise PLInstanceCalculationException(
                f"Poll {self.role} status timed out after {timeout}s expecting valid status."
            )

    def wait_instance_status(
        self,
        status: PrivateComputationInstanceStatus,
        fail_status: PrivateComputationInstanceStatus,
        timeout: int,
    ) -> None:
        self.logger.info(f"Poll {self.role} instance expecting status: {status}.")
        if timeout <= 0:
            raise ValueError(f"Timeout must be > 0, not {timeout}")
        start_time = time()
        while time() < start_time + timeout:
            self.update_instance()
            if self.status_ready(status):
                self.logger.info(f"{self.role} instance has expected status: {status}.")
                return
            if status not in [
                fail_status,
                PrivateComputationInstanceStatus.TIMEOUT,
            ] and self.status in [
                fail_status,
                PrivateComputationInstanceStatus.TIMEOUT,
            ]:
                raise PLInstanceCalculationException(
                    f"{self.role} failed with status {self.status}. Expecting status {status}."
                )
            sleep(POLL_INTERVAL)
        raise PLInstanceCalculationException(
            f"Poll {self.role} status timed out after {timeout}s expecting status: {status}."
        )

    def ready_for_stage(self, stage: PrivateComputationBaseStageFlow) -> bool:
        # This function checks whether the instance is ready for the publisher-partner
        # <stage> (PLInstanceCalculation.run_stage(<stage>)). Suppose user first
        # invokes publisher <stage> through GraphAPI, now publisher status is
        # '<STAGE>_STARTED`. Then, user runs pl-coordinator 'run_instance' command,
        # we would want to still allow <stage> to run.
        self.update_instance()
        previous_stage = stage.previous_stage
        return self.status in [
            previous_stage.completed_status if previous_stage else None,
            stage.started_status,
            stage.failed_status,
        ]

    def get_valid_stage(
        self, stage_flow: Type[PrivateComputationBaseStageFlow]
    ) -> Optional[PrivateComputationBaseStageFlow]:
        if not self.is_finished():
            for stage in list(stage_flow):
                if self.ready_for_stage(stage):
                    return stage
        return None

    def should_invoke_operation(self, stage: PrivateComputationBaseStageFlow) -> bool:
        # Once the the publisher-partner <stage> is called, this function
        # determines if <stage> operation should be invoked at publisher/partner end. If
        # the status is already <STAGE>_STARTED, then there's no need to invoke it
        # a second time.
        return self.ready_for_stage(stage) and self.status is not stage.started_status

    def wait_stage_start(self, stage: PrivateComputationBaseStageFlow) -> None:
        self.wait_instance_status(
            stage.started_status,
            stage.failed_status,
            OPERATION_REQUEST_TIMEOUT,
        )

    def is_finished(self):
        finished_status = GRAPHAPI_INSTANCE_STATUSES["RESULT_READY"]
        return self.status is finished_status


class PrivateLiftPublisherInstance(PrivateLiftCalcInstance):
    """
    Representation of a publisher instance.
    """

    def __init__(
        self, instance_id: str, logger: logging.Logger, client: PLGraphAPIClient
    ) -> None:
        super().__init__(instance_id, logger, PrivateComputationRole.PUBLISHER)
        self.client: PLGraphAPIClient = client
        self.server_ips: Optional[List[str]] = None
        self.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)

    def update_instance(self) -> None:
        response = json.loads(self.client.get_instance(self.instance_id).text)
        status = response.get("status")
        try:
            self.status = GRAPHAPI_INSTANCE_STATUSES[status]
        except KeyError:
            raise GraphAPIGenericException(
                f"Error getting Publisher instance status: Unexpected value {status}."
            )
        self.server_ips = response.get("server_ips")

    def wait_valid_status(
        self,
        timeout: int,
    ) -> None:
        self.update_instance()
        if self.status is PrivateComputationInstanceStatus.TIMEOUT:
            self.client.invoke_operation(self.instance_id, "NEXT")
        super().wait_valid_status(timeout)

    def status_ready(self, status: PrivateComputationInstanceStatus) -> bool:
        self.logger.info(
            f"{self.role} instance status: {self.status}, server ips: {self.server_ips}."
        )
        return self.status is status and self.server_ips is not None

    def run_stage(self, stage: PrivateComputationBaseStageFlow) -> None:
        if self.should_invoke_operation(stage):
            operation = (
                stage.name
                if isinstance(stage, PrivateComputationLegacyStageFlow)
                else "NEXT"
            )
            self.client.invoke_operation(self.instance_id, operation)


class PrivateLiftPartnerInstance(PrivateLiftCalcInstance):
    """
    Representation of a partner instance.
    """

    def __init__(
        self,
        instance_id: str,
        config: Dict[str, Any],
        input_path: str,
        num_shards: int,
        logger: logging.Logger,
    ) -> None:
        super().__init__(instance_id, logger, PrivateComputationRole.PARTNER)
        self.config: Dict[str, Any] = config
        self.input_path: str = input_path
        self.output_dir: str = self.get_output_dir_from_input_path(input_path)
        try:
            self.status = get_instance(
                self.config, self.instance_id, self.logger
            ).status
        except RuntimeError:
            self.logger.info(f"Creating new partner instance {self.instance_id}")
            self.status = create_instance(
                config=self.config,
                instance_id=self.instance_id,
                role=PrivateComputationRole.PARTNER,
                game_type=PrivateComputationGameType.LIFT,
                logger=self.logger,
                input_path=self.input_path,
                output_dir=self.output_dir,
                num_pid_containers=num_shards,
                num_mpc_containers=num_shards,
            ).status
        self.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)

    def update_instance(self) -> None:
        self.status = get_instance(self.config, self.instance_id, self.logger).status

    def cancel_current_stage(self) -> None:
        cancel_current_stage(self.config, self.instance_id, self.logger)

    def get_output_dir_from_input_path(self, input_path: str) -> str:
        return input_path[: input_path.rfind("/")]

    def run_stage(
        self,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        if self.should_invoke_operation(stage):
            try:
                if stage is PrivateComputationLegacyStageFlow.ID_MATCH:
                    id_match(
                        config=self.config,
                        instance_id=self.instance_id,
                        server_ips=server_ips,
                        logger=self.logger,
                        dry_run=None,
                    )
                elif stage is PrivateComputationLegacyStageFlow.COMPUTE:
                    prepare_compute_input(
                        config=self.config,
                        instance_id=self.instance_id,
                        logger=self.logger,
                        dry_run=None,
                    )
                    compute_metrics(
                        config=self.config,
                        instance_id=self.instance_id,
                        logger=self.logger,
                        server_ips=server_ips,
                        dry_run=None,
                    )
                elif stage is PrivateComputationLegacyStageFlow.AGGREGATE:
                    aggregate_shards(
                        config=self.config,
                        instance_id=self.instance_id,
                        logger=self.logger,
                        server_ips=server_ips,
                        dry_run=None,
                    )
                else:
                    run_stage(
                        config=self.config,
                        instance_id=self.instance_id,
                        stage=stage,
                        logger=self.logger,
                        server_ips=server_ips,
                    )
            except Exception as error:
                self.logger.exception(f"Error running partner {stage.name} {error}")


class PLInstanceRunner:
    """
    Private Lift Partner-Publisher computation for an instance.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        instance_id: str,
        input_path: str,
        num_shards: int,
        logger: logging.Logger,
        client: PLGraphAPIClient,
        num_tries: int,
        dry_run: Optional[bool],
        stage_flow: Type[PrivateComputationBaseStageFlow],
    ) -> None:
        self.logger = logger
        self.instance_id = instance_id
        self.publisher = PrivateLiftPublisherInstance(instance_id, logger, client)
        self.partner = PrivateLiftPartnerInstance(
            instance_id=instance_id,
            config=config,
            input_path=input_path,
            num_shards=num_shards,
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
                if not publisher_stage.is_joint_stage:
                    # Example: publisher is PREPARE_DATA_FAILED, partner is PREPARE_DATA_COMPLETED
                    return publisher_stage
            elif partner_stage is publisher_stage.previous_stage:
                if not partner_stage.is_joint_stage:
                    # Example: publisher is PREPARE_DATA_COMPLETED, partner is PREPARE_DATA_FAILED
                    return partner_stage
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
        raise PLInstanceCalculationException(
            f"Waiting for valid stage timed out after {timeout}s."
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
                    raise PLInstanceCalculationException(
                        f"Stage {stage.name} failed. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
                    )
            sleep(POLL_INTERVAL)
        raise PLInstanceCalculationException(
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
        )
