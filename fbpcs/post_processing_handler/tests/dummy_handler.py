#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
import random
from typing import Optional, TYPE_CHECKING

from fbpcs.post_processing_handler.exception import PostProcessingHandlerRuntimeError
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler

if TYPE_CHECKING:
    # pyre-fixme[21]: Could not find module
    #  `fbpcs.private_computation.entity.private_computation_instance`.
    from fbpcs.private_computation.entity.private_computation_instance import (
        PrivateComputationInstance,
    )
from fbpcp.service.storage import StorageService
from fbpcs.common.service.trace_logging_service import TraceLoggingService


class PostProcessingDummyHandler(PostProcessingHandler):
    """A dummy post processing handler used for testing handler management logic."""

    def __init__(
        self,
        trace_logging_svc: Optional[TraceLoggingService] = None,
        probability_of_failure: float = 0,
    ) -> None:
        super().__init__()
        self.probability_of_failure = probability_of_failure
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.trace_logging_svc = trace_logging_svc

    async def run(
        self,
        storage_svc: StorageService,
        # pyre-fixme[11]: Annotation `PrivateComputationInstance` is not defined as
        #  a type.
        private_computation_instance: "PrivateComputationInstance",
    ) -> None:
        if random.random() >= self.probability_of_failure:
            self.logger.info(
                f"{private_computation_instance.infra_config.instance_id=},{private_computation_instance.shard_aggregate_stage_output_path}"
            )
        else:
            raise PostProcessingHandlerRuntimeError(
                "You got bad RNG and the dummy handler failed. Try again."
            )
