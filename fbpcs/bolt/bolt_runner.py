#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs, BoltJob
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
    Exposes async methods for creating instances, running stages, updating instances, and validating the correctness of a computation
    """

    @abstractmethod
    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        pass

    @abstractmethod
    async def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
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


class BoltRunner:
    async def run_async(
        self,
        jobs: List[BoltJob],
        publisher_client: BoltClient,
        partner_client: BoltClient,
    ) -> None:
        pass
