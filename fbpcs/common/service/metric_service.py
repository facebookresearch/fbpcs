#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import logging


class MetricService(abc.ABC):
    def __init__(self, category: str = "default") -> None:
        self.category = category
        self.logger: logging.Logger = logging.getLogger(__name__)

    @abc.abstractmethod
    def bump_entity_key(self, entity: str, key: str, value: int = 1) -> None:
        pass
