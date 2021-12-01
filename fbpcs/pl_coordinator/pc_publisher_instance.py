#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
from typing import List, Optional

from fbpcs.pl_coordinator.constants import WAIT_VALID_STATUS_TIMEOUT
from fbpcs.pl_coordinator.pc_calc_instance import PrivateLiftCalcInstance
from fbpcs.pl_coordinator.pl_graphapi_utils import (
    GRAPHAPI_INSTANCE_STATUSES,
    GraphAPIGenericException,
    PLGraphAPIClient,
)
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)


# TODO(T107103749): [BE] rename PrivateLiftPublisherInstance
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
            self.client.invoke_operation(self.instance_id, "NEXT")
