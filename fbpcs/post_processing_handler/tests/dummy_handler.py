#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
import random
from typing import TYPE_CHECKING

from fbpcs.post_processing_handler.exception import PostProcessingHandlerRuntimeError
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler

if TYPE_CHECKING:
    from fbpcs.private_computation.entity.private_computation_instance import (
        PrivateComputationInstance,
    )
from fbpcp.service.storage import StorageService


class PostProcessingDummyHandler(PostProcessingHandler):
    """A dummy post processing handler used for testing handler management logic."""

    def __init__(self, probability_of_failure: float = 0) -> None:
        super().__init__()
        self.probability_of_failure = probability_of_failure
        self.logger: logging.Logger = logging.getLogger(__name__)

    async def run(
        self,
        storage_svc: StorageService,
        private_computation_instance: "PrivateComputationInstance",
    ) -> None:
        if random.random() >= self.probability_of_failure:
            self.logger.info(
                f"{private_computation_instance.instance_id=},{private_computation_instance.shard_aggregate_stage_output_path}"
            )
        else:
            raise PostProcessingHandlerRuntimeError(
                "You got bad RNG and the dummy handler failed. Try again."
            )
