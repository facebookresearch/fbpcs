#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from fbpcs.bolt.bolt_client import BoltClient, BoltState
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltPLGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    study_id: str
    breakdown_key: Dict[str, str]


@dataclass
class BoltPAGraphAPICreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str  # used for temporary resuming solution
    dataset_id: str
    timestamp: str
    attribution_rule: str
    num_containers: str


class BoltGraphAPIClient(BoltClient):
    def __init__(
        self, access_token: str, logger: Optional[logging.Logger] = None
    ) -> None:
        pass

    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        pass

    async def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        pass

    async def update_instance(self, instance_id: str) -> BoltState:
        pass

    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        pass
