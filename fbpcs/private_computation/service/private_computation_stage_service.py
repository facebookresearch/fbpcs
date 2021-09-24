#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc

from typing import List, Optional
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_stage_type import PrivateComputationStageType


class PrivateComputationStageService(abc.ABC):
    """
    Handles the business logic for each private computation stage. Each stage should subclass this service and implement the run_async method.
    Any parameters necessary to run the stage that aren't provided by run_async should be passed to the subclass' constructor.
    """

    @abc.abstractclassmethod
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        ...

    @property
    @abc.abstractmethod
    def stage_type(self) -> PrivateComputationStageType:
        ...
