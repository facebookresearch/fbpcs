#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Type

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltState:
    pc_instance_status: PrivateComputationInstanceStatus
    server_ips: Optional[List[str]] = None


class BoltClient(ABC):
    """
    Exposes async methods for creating instances, running stages, updating instances,
    and validating the correctness of a computation
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )

    @abstractmethod
    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        pass

    @abstractmethod
    async def run_stage(
        self,
        instance_id: str,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        pass

    @abstractmethod
    async def update_instance(self, instance_id: str) -> BoltState:
        pass

    @abstractmethod
    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        pass

    async def cancel_current_stage(self, instance_id: str) -> None:
        pass

    def ready_for_stage(
        self,
        status: PrivateComputationInstanceStatus,
        stage: PrivateComputationBaseStageFlow,
    ) -> bool:
        previous_stage = stage.previous_stage
        return status in [
            previous_stage.completed_status if previous_stage else None,
            stage.started_status,
            stage.failed_status,
        ]

    async def get_valid_stage(
        self, instance_id: str, stage_flow: Type[PrivateComputationBaseStageFlow]
    ) -> Optional[PrivateComputationBaseStageFlow]:
        status = (
            await self.update_instance(instance_id=instance_id)
        ).pc_instance_status
        for stage in list(stage_flow):
            if self.ready_for_stage(status, stage):
                return stage
        return None

    async def is_existing_instance(self, instance_args: BoltCreateInstanceArgs) -> bool:
        """Returns whether the instance with instance_args exists

        Args:
            - instance_args: The arguments for creating the instance

        Returns:
            True if there is an instance with these instance_args, False otherwise
        """

        instance_id = instance_args.instance_id
        try:
            self.logger.info(f"Checking if {instance_id} exists...")
            await self.update_instance(instance_id=instance_id)
            self.logger.info(f"{instance_id} found.")
            return True
        except Exception:
            self.logger.info(f"{instance_id} not found.")
            return False
