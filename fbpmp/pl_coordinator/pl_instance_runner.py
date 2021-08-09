#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
from enum import Enum
from multiprocessing import Process
from time import sleep, time
from typing import Any, Dict, List, Optional

from fbpmp.pl_coordinator.pl_graphapi_utils import (
    GraphAPIGenericException,
    PLGraphAPIClient,
    GRAPHAPI_INSTANCE_STATUSES,
)
from fbpmp.pl_coordinator.pl_service_wrapper import (
    get,
    create_instance,
    id_match,
    compute,
    aggregate,
    cancel_current_stage,
)
from fbpmp.private_lift.entity.privatelift_instance import (
    PrivateLiftRole,
    PrivateLiftInstanceStatus,
)


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, prefix: str):
        super(LoggerAdapter, self).__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return "[%s] %s" % (self.prefix, msg), kwargs


class PrivateLiftStage(Enum):
    ID_MATCH = "ID_MATCH"
    COMPUTE = "COMPUTE"
    AGGREGATE = "AGGREGATE"


PRIVATE_LIFT_STAGES = [
    PrivateLiftStage.ID_MATCH,
    PrivateLiftStage.COMPUTE,
    PrivateLiftStage.AGGREGATE,
]
STAGE_OUTPUT_SUFFIX = {
    PrivateLiftStage.ID_MATCH: "_pid_out.csv",
    PrivateLiftStage.COMPUTE: "_mpc_computed.csv",
    PrivateLiftStage.AGGREGATE: "_mpc_aggregated.json",
}
READY_STATUS = {
    PrivateLiftStage.ID_MATCH: PrivateLiftInstanceStatus.CREATED,
    PrivateLiftStage.COMPUTE: PrivateLiftInstanceStatus.ID_MATCHING_COMPLETED,
    PrivateLiftStage.AGGREGATE: PrivateLiftInstanceStatus.COMPUTATION_COMPLETED,
}
STARTED_STATUS = {
    PrivateLiftStage.ID_MATCH: PrivateLiftInstanceStatus.ID_MATCHING_STARTED,
    PrivateLiftStage.COMPUTE: PrivateLiftInstanceStatus.COMPUTATION_STARTED,
    PrivateLiftStage.AGGREGATE: PrivateLiftInstanceStatus.AGGREGATION_STARTED,
}
FAILED_STATUS = {
    PrivateLiftStage.ID_MATCH: PrivateLiftInstanceStatus.ID_MATCHING_FAILED,
    PrivateLiftStage.COMPUTE: PrivateLiftInstanceStatus.COMPUTATION_FAILED,
    PrivateLiftStage.AGGREGATE: PrivateLiftInstanceStatus.AGGREGATION_FAILED,
}
COMPLETED_STATUS = {
    PrivateLiftStage.ID_MATCH: PrivateLiftInstanceStatus.ID_MATCHING_COMPLETED,
    PrivateLiftStage.COMPUTE: PrivateLiftInstanceStatus.COMPUTATION_COMPLETED,
    PrivateLiftStage.AGGREGATE: PrivateLiftInstanceStatus.AGGREGATION_COMPLETED,
}
INVALID_STATUS_LIST = [
    PrivateLiftInstanceStatus.UNKNOWN,
    PrivateLiftInstanceStatus.PROCESSING_REQUEST,
]

STAGE_TIMEOUT = {
    PrivateLiftStage.ID_MATCH: 3600,
    PrivateLiftStage.COMPUTE: 3600,
    PrivateLiftStage.AGGREGATE: 1800,
}
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
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
    dry_run: Optional[bool] = False,
) -> None:
    num_tries = num_tries if num_tries is not None else MAX_TRIES
    if num_tries < MIN_TRIES or num_tries > MAX_TRIES:
        raise ValueError(f"num_tries must be between {MIN_TRIES} and {MAX_TRIES}.")
    client = PLGraphAPIClient(config["graphapi"]["access_token"], logger)
    instance_runner = PLInstanceRunner(
        config, instance_id, input_path, logger, client, num_tries, dry_run
    )
    logger.info(f"Running private lift for instance {instance_id}")
    instance_runner.run()


def run_instances(
    config: Dict[str, Any],
    instance_ids: List[str],
    input_paths: List[str],
    logger: logging.Logger,
    num_tries: Optional[int] = 2,  # this is number of tries per stage
    dry_run: Optional[bool] = False,
) -> None:
    if len(instance_ids) is not len(input_paths):
        raise ValueError(
            "Number of instances and number of input paths must be the same"
        )
    if not MIN_NUM_INSTANCES <= len(instance_ids) <= MAX_NUM_INSTANCES:
        raise ValueError(
            f"Number of instances must be between {MIN_NUM_INSTANCES} and {MAX_NUM_INSTANCES}"
        )
    processes = list(
        map(
            lambda instance_id, input_path: Process(
                target=run_instance,
                kwargs={
                    "config": config,
                    "instance_id": instance_id,
                    "input_path": input_path,
                    "logger": LoggerAdapter(logger=logger, prefix=instance_id),
                    "num_tries": num_tries,
                    "dry_run": dry_run,
                },
            ),
            instance_ids,
            input_paths,
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
        self, instance_id: str, logger: logging.Logger, role: PrivateLiftRole
    ) -> None:
        self.instance_id: str = instance_id
        self.logger: logging.Logger = logger
        self.role: PrivateLiftRole = role
        self.status: PrivateLiftInstanceStatus = PrivateLiftInstanceStatus.UNKNOWN

    def update_instance(self) -> None:
        raise NotImplementedError(
            "This is a parent method to be overrided and should not be called."
        )

    def status_ready(self, status: PrivateLiftInstanceStatus) -> bool:
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
        status: PrivateLiftInstanceStatus,
        fail_status: PrivateLiftInstanceStatus,
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
            if status is not fail_status and self.status is fail_status:
                raise PLInstanceCalculationException(
                    f"{self.role} failed expecting status {status}. Status: {fail_status}."
                )
            sleep(POLL_INTERVAL)
        raise PLInstanceCalculationException(
            f"Poll {self.role} status timed out after {timeout}s expecting status: {status}."
        )

    def ready_for_stage(self, stage: PrivateLiftStage) -> bool:
        # This function checks whether the instance is ready for the publisher-partner
        # <stage> (PLInstanceCalculation.run_stage(<stage>)). Suppose user first
        # invokes publisher <stage> through GraphAPI, now publisher status is
        # '<STAGE>_STARTED`. Then, user runs pl-coordinator 'run_instance' command,
        # we would want to still allow <stage> to run.
        self.update_instance()
        return self.status in [
            READY_STATUS[stage],
            STARTED_STATUS[stage],
            FAILED_STATUS[stage],
        ]

    def should_invoke_operation(self, stage: PrivateLiftStage) -> bool:
        # Once the the publisher-partner <stage> is called, this function
        # determines if <stage> operation should be invoked at publisher/partner end. If
        # the status is already <STAGE>_STARTED, then there's no need to invoke it
        # a second time.
        return self.ready_for_stage(stage) and self.status is not STARTED_STATUS[stage]

    def wait_stage_start(self, stage: PrivateLiftStage) -> None:
        self.wait_instance_status(
            STARTED_STATUS[stage],
            FAILED_STATUS[stage],
            OPERATION_REQUEST_TIMEOUT,
        )


class PrivateLiftPublisherInstance(PrivateLiftCalcInstance):
    """
    Representation of a publisher instance.
    """

    def __init__(
        self, instance_id: str, logger: logging.Logger, client: PLGraphAPIClient
    ) -> None:
        super().__init__(instance_id, logger, PrivateLiftRole.PUBLISHER)
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

    def status_ready(self, status: PrivateLiftInstanceStatus) -> bool:
        self.logger.info(
            f"{self.role} instance status: {self.status}, server ips: {self.server_ips}."
        )
        return self.status is status and self.server_ips is not None

    def run_stage(self, stage: PrivateLiftStage) -> None:
        if self.should_invoke_operation(stage):
            self.client.invoke_operation(self.instance_id, stage.value)


class PrivateLiftPartnerInstance(PrivateLiftCalcInstance):
    """
    Representation of a partner instance.
    """

    def __init__(
        self,
        instance_id: str,
        config: Dict[str, Any],
        input_path: str,
        logger: logging.Logger,
    ) -> None:
        super().__init__(instance_id, logger, PrivateLiftRole.PARTNER)
        self.config: Dict[str, Any] = config
        self.input_path: str = input_path
        try:
            self.status = get(self.config, self.instance_id, self.logger).status
        except RuntimeError:
            self.logger.info(f"Creating new partner instance {self.instance_id}")
            self.status = create_instance(
                self.config, self.instance_id, PrivateLiftRole.PARTNER, self.logger
            ).status
        self.wait_valid_status(WAIT_VALID_STATUS_TIMEOUT)

    def update_instance(self) -> None:
        self.status = get(self.config, self.instance_id, self.logger).status

    def cancel_current_stage(self) -> None:
        cancel_current_stage(self.config, self.instance_id, self.logger)

    def get_output_path_for_stage(self, stage: PrivateLiftStage) -> str:
        return self.input_path.replace(
            ".csv", "_" + self.instance_id + STAGE_OUTPUT_SUFFIX[stage]
        )

    def run_stage(self, server_ips: List[str], stage: PrivateLiftStage) -> None:
        output_path = self.get_output_path_for_stage(stage)
        if self.should_invoke_operation(stage):
            try:
                if stage is PrivateLiftStage.ID_MATCH:
                    id_match(
                        config=self.config,
                        instance_id=self.instance_id,
                        num_containers=len(server_ips),
                        input_path=self.input_path,
                        output_path=output_path,
                        server_ips=server_ips,
                        logger=self.logger,
                        dry_run=None,
                    )
                elif stage is PrivateLiftStage.COMPUTE:
                    compute(
                        config=self.config,
                        instance_id=self.instance_id,
                        output_path=output_path,
                        logger=self.logger,
                        num_containers=None,
                        spine_path=None,
                        data_path=None,
                        concurrency=None,
                        server_ips=server_ips,
                        dry_run=None,
                    )
                else:
                    aggregate(
                        config=self.config,
                        instance_id=self.instance_id,
                        output_path=output_path,
                        logger=self.logger,
                        input_path=None,
                        num_shards=None,
                        server_ips=server_ips,
                        dry_run=None,
                    )
            except Exception as error:
                self.logger.error(f"Error running partner {stage.value} {error}")


class PLInstanceRunner:
    """
    Private Lift Partner-Publisher computation for an instance.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        instance_id: str,
        input_path: str,
        logger: logging.Logger,
        client: PLGraphAPIClient,
        num_tries: int,
        dry_run: Optional[bool],
    ) -> None:
        self.logger = logger
        self.instance_id = instance_id
        self.publisher = PrivateLiftPublisherInstance(instance_id, logger, client)
        self.partner = PrivateLiftPartnerInstance(
            instance_id, config, input_path, logger
        )
        self.num_tries = num_tries
        self.dry_run = dry_run

    def ready_for_stage(self, stage: PrivateLiftStage):
        return self.publisher.ready_for_stage(stage) and self.partner.ready_for_stage(
            stage
        )

    def get_valid_stage(self) -> Optional[PrivateLiftStage]:
        for stage in PRIVATE_LIFT_STAGES:
            if self.ready_for_stage(stage):
                return stage
        return None

    def wait_valid_stage(self, timeout: int) -> PrivateLiftStage:
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

    def is_finished(self):
        finished_status = COMPLETED_STATUS[PRIVATE_LIFT_STAGES[-1]]
        return (
            self.publisher.status is finished_status
            and self.partner.status is finished_status
        )

    def run(self) -> None:
        tries = 0
        while tries < self.num_tries:
            tries += 1
            try:
                if self.is_finished():
                    self.logger.info(
                        f"Private Lift run completed for instance {self.instance_id}. View results at {self.partner.get_output_path_for_stage(PRIVATE_LIFT_STAGES[-1])}"
                    )
                    return
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

    def run_stage(self, stage: PrivateLiftStage) -> None:
        self.logger.info(f"Running publisher-partner {stage.value}")
        # call publisher <STAGE>
        self.logger.info(f"Invoking publisher {stage.value}.")
        self.publisher.run_stage(stage)
        # keep polling graphapi until publisher status is <STAGE>_STARTED and server_ips are available
        self.publisher.wait_stage_start(stage)
        server_ips = self.publisher.server_ips
        if server_ips is None:
            raise ValueError("in run_stage, server_ips is None")
        # call partner <STAGE>
        self.logger.info(f"Starting partner {stage.value}:")
        self.partner.run_stage(server_ips, stage)
        # wait for stage to complete
        self.wait_stage_complete(stage)

    def wait_stage_complete(
        self, stage: PrivateLiftStage
    ) -> None:
        start_status = STARTED_STATUS[stage]
        complete_status = COMPLETED_STATUS[stage]
        fail_status = FAILED_STATUS[stage]
        timeout = STAGE_TIMEOUT[stage]

        start_time = time()
        cancel_time = 0
        while time() < start_time + timeout:
            self.publisher.update_instance()
            self.partner.update_instance()
            self.logger.info(f"Publisher status: {self.publisher.status}. Partner status: {self.partner.status}.")
            if self.publisher.status is complete_status and self.partner.status is complete_status:
                self.logger.info(f"Stage {stage.value} is complete.")
                return
            if self.publisher.status is fail_status or self.partner.status is fail_status:
                if self.publisher.status is fail_status and self.partner.status is start_status and cancel_time <= CANCEL_STAGE_TIMEOUT:
                    # wait 5 minutes for partner to become fail status on its own
                    # if not, only perform 'cancel_stage' one time
                    if cancel_time == CANCEL_STAGE_TIMEOUT:
                        self.logger.error(f"Canceling partner stage {stage.value}.")
                        self.partner.cancel_current_stage()
                    else:
                        self.logger.info(f"Waiting to cancel partner stage {stage.value}.")
                    # only cancel once
                    cancel_time += POLL_INTERVAL
                else:
                    raise PLInstanceCalculationException(
                        f"Stage {stage.value} failed. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
                    )
            sleep(POLL_INTERVAL)
        raise PLInstanceCalculationException(
            f"Stage {stage.value} timed out after {timeout}s. Publisher status: {self.publisher.status}. Partner status: {self.partner.status}."
        )
