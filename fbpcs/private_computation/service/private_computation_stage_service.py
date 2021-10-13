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


class PrivateComputationStageService(abc.ABC):
    """
    Handles the business logic for each private computation stage. Each stage should subclass this service and implement the run_async method.
    Any parameters necessary to run the stage that aren't provided by run_async should be passed to the subclass' constructor.
    """

    @abc.abstractclassmethod
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        # TODO(T102471612): remove server_ips from run_async, move to subclass constructor instead
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        ...
