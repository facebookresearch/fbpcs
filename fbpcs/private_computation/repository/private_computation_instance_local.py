#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.common.repository.instance_local import LocalInstanceRepository
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)


class LocalPrivateComputationInstanceRepository(PrivateComputationInstanceRepository):
    def __init__(self, base_dir: str) -> None:
        self.repo = LocalInstanceRepository(base_dir)

    def create(self, instance: PrivateComputationInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PrivateComputationInstance:
        return PrivateComputationInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PrivateComputationInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
