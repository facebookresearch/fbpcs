#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.repository.instance_local import LocalInstanceRepository
from fbpmp.pid.entity.pid_instance import PIDInstance
from fbpmp.pid.repository.pid_instance import PIDInstanceRepository


class LocalPIDInstanceRepository(PIDInstanceRepository):
    def __init__(self, base_dir: str) -> None:
        super().__init__()
        self.repo = LocalInstanceRepository(base_dir)

    def create(self, instance: PIDInstance) -> None:
        self.repo.create(instance)

    def read(self, instance_id: str) -> PIDInstance:
        return PIDInstance.loads_schema(self.repo.read(instance_id))

    def update(self, instance: PIDInstance) -> None:
        self.repo.update(instance)

    def delete(self, instance_id: str) -> None:
        self.repo.delete(instance_id)
