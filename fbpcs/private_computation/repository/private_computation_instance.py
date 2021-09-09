#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)


class PrivateComputationInstanceRepository(abc.ABC):
    @abc.abstractmethod
    def create(self, instance: PrivateComputationInstance) -> None:
        pass

    @abc.abstractmethod
    def read(self, instance_id: str) -> PrivateComputationInstance:
        pass

    @abc.abstractmethod
    def update(self, instance: PrivateComputationInstance) -> None:
        pass

    @abc.abstractmethod
    def delete(self, instance_id: str) -> None:
        pass
