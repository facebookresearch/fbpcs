#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
from time import sleep, time
from typing import Optional, Type

from fbpcs.pl_coordinator.constants import (
    INVALID_STATUS_LIST,
    POLL_INTERVAL,
    OPERATION_REQUEST_TIMEOUT,
)
from fbpcs.pl_coordinator.exceptions import PLInstanceCalculationException
from fbpcs.pl_coordinator.pl_graphapi_utils import GRAPHAPI_INSTANCE_STATUSES
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


# TODO(T107103692): [BE] rename PrivateLiftCalcInstance
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

    def is_finished(self) -> bool:
        finished_status = GRAPHAPI_INSTANCE_STATUSES["RESULT_READY"]
        return self.status is finished_status
