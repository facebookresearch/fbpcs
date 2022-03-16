#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import List, Optional

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class InputDataValidationStageService(PrivateComputationStageService):
    """
    This InputDataValidation stage service validates input data files.
    Validation fails if the issues detected in the data file
    do not pass the input_data_validation configuration minimum
    valid thresholds. A failing validation stage will prevent the next
    stage from running.

    It is implemented in a Cloud agnostic way.
    """

    def __init__(self) -> None:
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._failed_status: PrivateComputationInstanceStatus = (
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_FAILED
        )

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """
        Updates the status to COMPLETED and returns the pc_instance
        """
        self._logger.info("[InputDataValidation] - Starting stage")
        # TODO: call the data_input_validation library
        pc_instance.status = (
            PrivateComputationInstanceStatus.INPUT_DATA_VALIDATION_COMPLETED
        )
        self._logger.info("[InputDataValidation] - Finished stage")
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """
        Returns the pc_instance's current status
        """
        return pc_instance.status
