# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import List

from common.data_model import MetadataEntity
from common.queue_manager import QueueManager


# Metadata queue in memory
class MemoryQueueManager(QueueManager):
    def __init__(
        self,
    ) -> None:
        self.logger = logging.getLogger()
        self.queue = []

    def add_metadata(
        self,
        entity: MetadataEntity,
    ) -> None:
        self.logger.info(f"queue.add_metadata: entity={entity}.")
        self.queue.append(entity)

    def peek_metadata(
        self,
        result_limit: int,
    ) -> List[MetadataEntity]:
        ret = []
        i = 0
        while i < result_limit and i < len(self.queue):
            ret.append(self.queue[i])
            i += 1

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
        i = count
        while i > 0 and len(self.queue) > 0:
            del self.queue[0]
            i -= 1
