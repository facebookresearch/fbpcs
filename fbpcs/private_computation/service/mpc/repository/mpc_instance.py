#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc

from fbpcs.private_computation.service.mpc.entity.mpc_instance import MPCInstance


class MPCInstanceRepository(abc.ABC):
    @abc.abstractmethod
    def create(self, instance: MPCInstance) -> None:
        pass

    @abc.abstractmethod
    def read(self, instance_id: str) -> MPCInstance:
        pass

    @abc.abstractmethod
    def update(self, instance: MPCInstance) -> None:
        pass

    @abc.abstractmethod
    def delete(self, instance_id: str) -> None:
        pass
