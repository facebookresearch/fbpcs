# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import threading
from typing import List

from fbpcs.infra.logging_service.server.common.data_model import MetadataEntity
from fbpcs.infra.logging_service.server.common.queue_manager import QueueManager


# Metadata queue in memory
class MemoryQueueManager(QueueManager):
    def __init__(
        self,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger()
        self.queue: List[MetadataEntity] = []
        self.lock = threading.Lock()

    def add_metadata(
        self,
        entity: MetadataEntity,
    ) -> None:
        self.logger.info(f"queue.add_metadata: entity={entity}.")
        with self.lock:
            self.queue.append(entity)

    def peek_metadata(
        self,
        result_limit: int,
    ) -> List[MetadataEntity]:
        with self.lock:
            ret = self.queue[:result_limit]
        if ret:
            self.logger.info(
                f"queue.peek_metadata: result_limit={result_limit}, result_len={len(ret)}, remaining={len(self.queue)}."
            )
        return ret

    def remove_metadata(
        self,
        count: int,
    ) -> None:
        self.logger.info(f"queue.remove_metadata: count={count}.")
        with self.lock:
            del self.queue[:count]
