# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from typing import List

from common.data_model import MetadataEntity


# Queue interface for Log metadata to be uploaded
class QueueManager(abc.ABC):
    @abc.abstractmethod
    def add_metadata(
        self,
        entity: MetadataEntity,
    ) -> None:
        pass

    @abc.abstractmethod
    def peek_metadata(
        self,
        result_limit: int,
    ) -> List[MetadataEntity]:
        pass

    @abc.abstractmethod
    def remove_metadata(
        self,
        count: int,
    ) -> None:
        pass
