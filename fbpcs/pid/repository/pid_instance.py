#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import threading

from fbpcs.pid.entity.pid_instance import PIDInstance


class PIDInstanceRepository(abc.ABC):
    def __init__(self) -> None:
        """
        IMPORTANT: after acquiring this lock, and before releasing it, there cannot
        be async calls, otherwise it will end up with deadlock.
        """
        self.lock = threading.Lock()

    @abc.abstractmethod
    def create(self, instance: PIDInstance) -> None:
        pass

    @abc.abstractmethod
    def read(self, instance_id: str) -> PIDInstance:
        pass

    @abc.abstractmethod
    def update(self, instance: PIDInstance) -> None:
        pass

    @abc.abstractmethod
    def delete(self, instance_id: str) -> None:
        pass
